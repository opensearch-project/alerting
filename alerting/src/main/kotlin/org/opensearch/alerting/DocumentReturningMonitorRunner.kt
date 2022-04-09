package org.opensearch.alerting

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.alerts.AlertIndices.Companion.FINDING_HISTORY_WRITE_INDEX
import org.opensearch.alerting.core.model.DocLevelMonitorInput
import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.alerting.elasticapi.string
import org.opensearch.alerting.model.ActionRunResult
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.AlertingConfigAccessor
import org.opensearch.alerting.model.DocumentExecutionContext
import org.opensearch.alerting.model.DocumentLevelTrigger
import org.opensearch.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.alerting.model.Finding
import org.opensearch.alerting.model.InputRunResults
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.action.Action
import org.opensearch.alerting.script.DocumentLevelTriggerExecutionContext
import org.opensearch.alerting.script.TriggerExecutionContext
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.isAllowed
import org.opensearch.alerting.util.updateMonitor
import org.opensearch.client.Client
import org.opensearch.cluster.routing.ShardRouting
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.Strings
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.search.SearchHits
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder
import java.io.IOException
import java.time.Instant
import java.util.UUID
import kotlin.collections.HashMap
import kotlin.math.max

object DocumentReturningMonitorRunner : MonitorRunner {
    private val logger = LogManager.getLogger(javaClass)

    override suspend fun runMonitor(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryrun: Boolean
    ): MonitorRunResult<DocumentLevelTriggerRunResult> {
        logger.info("Document-level-monitor is running ...")
        var monitorResult = MonitorRunResult<DocumentLevelTriggerRunResult>(monitor.name, periodStart, periodEnd)

        // TODO: is this needed from Charlie?
        try {
            monitorCtx.alertIndices!!.createOrUpdateAlertIndex()
            monitorCtx.alertIndices!!.createOrUpdateInitialAlertHistoryIndex()
            monitorCtx.alertIndices!!.createOrUpdateInitialFindingHistoryIndex()
        } catch (e: Exception) {
            val id = if (monitor.id.trim().isEmpty()) "_na_" else monitor.id
            logger.error("Error loading alerts for monitor: $id", e)
            return monitorResult.copy(error = e)
        }

        try {
            validate(monitor)
        } catch (e: Exception) {
            logger.info("Failed to start Document-level-monitor. Error: ${e.message}")
            return monitorResult.copy(error = e)
        }

        val docLevelMonitorInput = monitor.inputs[0] as DocLevelMonitorInput
        val index = docLevelMonitorInput.indices[0]
        val queries: List<DocLevelQuery> = docLevelMonitorInput.queries

        val isTempMonitor = dryrun || monitor.id == Monitor.NO_ID
        var lastRunContext = monitor.lastRunContext.toMutableMap()
        try {
            if (lastRunContext.isNullOrEmpty()) {
                lastRunContext = createRunContext(monitorCtx.clusterService!!, monitorCtx.client!!, index).toMutableMap()
            }
        } catch (e: Exception) {
            logger.info("Failed to start Document-level-monitor $index. Error: ${e.message}")
            return monitorResult.copy(error = e)
        }

        val count: Int = lastRunContext["shards_count"] as Int
        val updatedLastRunContext = lastRunContext.toMutableMap()
        for (i: Int in 0 until count) {
            val shard = i.toString()
            val maxSeqNo: Long = getMaxSeqNo(monitorCtx.client!!, index, shard)
            updatedLastRunContext[shard] = maxSeqNo

            // update lastRunContext if its a temp monitor as we only want to view the last bit of data then
            // TODO: If dryrun, we should make it so we limit the search as this could still potentially give us lots of data
            if (isTempMonitor) {
                lastRunContext[shard] = max(-1, maxSeqNo - 1)
            }
        }

        val queryToDocIds = mutableMapOf<DocLevelQuery, Set<String>>()
        val docsToQueries = mutableMapOf<String, MutableList<String>>()
        val docExecutionContext = DocumentExecutionContext(queries, lastRunContext, updatedLastRunContext)
        val idQueryMap = mutableMapOf<String, DocLevelQuery>()
        queries.forEach { query ->
            val matchingDocIds = runForEachQuery(monitorCtx, docExecutionContext, query, index)
            queryToDocIds[query] = matchingDocIds
            matchingDocIds.forEach {
                docsToQueries.putIfAbsent(it, mutableListOf())
                docsToQueries[it]?.add(query.id)
            }
            idQueryMap[query.id] = query
        }
        val queryInputResults = queryToDocIds.mapKeys { it.key.id }
        monitorResult = monitorResult.copy(inputResults = InputRunResults(listOf(queryInputResults)))
        val queryIds = queries.map { it.id }

        val triggerResults = mutableMapOf<String, DocumentLevelTriggerRunResult>()
        monitor.triggers.forEach {
            triggerResults[it.id] = runForEachDocTrigger(
                monitorCtx,
                monitorResult,
                it as DocumentLevelTrigger,
                monitor,
                idQueryMap,
                docsToQueries,
                queryIds,
                dryrun
            )
        }

        // Don't update monitor if this is a test monitor
        if (!isTempMonitor) {

            // TODO: Check for race condition against the update monitor api
            // This does the update at the end in case of errors and makes sure all the queries are executed
            val updatedMonitor = monitor.copy(lastRunContext = updatedLastRunContext)
            // note: update has to called in serial for shards of a given index.
            // make sure this is just updated for the specific query or at the end of all the queries
            updateMonitor(monitorCtx.client!!, monitorCtx.xContentRegistry!!, monitorCtx.settings!!, updatedMonitor)
        }

        // TODO: Update the Document as part of the Trigger and return back the trigger action result
        return monitorResult.copy(triggerResults = triggerResults)
    }

    private suspend fun runForEachDocTrigger(
        monitorCtx: MonitorRunnerExecutionContext,
        monitorResult: MonitorRunResult<DocumentLevelTriggerRunResult>,
        trigger: DocumentLevelTrigger,
        monitor: Monitor,
        idQueryMap: Map<String, DocLevelQuery>,
        docsToQueries: Map<String, List<String>>,
        queryIds: List<String>,
        dryrun: Boolean
    ): DocumentLevelTriggerRunResult {
        val triggerCtx = DocumentLevelTriggerExecutionContext(monitor, trigger)
        val triggerResult = monitorCtx.triggerService!!.runDocLevelTrigger(monitor, trigger, triggerCtx, docsToQueries, queryIds)

        logger.info("trigger results")
        logger.info(triggerResult.triggeredDocs.toString())

        val index = (monitor.inputs[0] as DocLevelMonitorInput).indices[0]

        // TODO: modify findings such that there is a finding per document
        val findings = mutableListOf<String>()
        val findingDocPairs = mutableListOf<Pair<String, String>>()

        // TODO: Implement throttling for findings
        if (!dryrun && monitor.id != Monitor.NO_ID) {
            docsToQueries.forEach {
                val triggeredQueries = it.value.map { queryId -> idQueryMap[queryId]!! }
                val findingId = createFindings(monitor, monitorCtx, index, triggeredQueries, listOf(it.key))
                findings.add(findingId)

                if (triggerResult.triggeredDocs.contains(it.key)) {
                    findingDocPairs.add(Pair(findingId, it.key))
                }
            }
        }

        val actionCtx = triggerCtx.copy(
            triggeredDocs = triggerResult.triggeredDocs,
            relatedFindings = findings,
            error = monitorResult.error ?: triggerResult.error
        )

        for (action in trigger.actions) {
            triggerResult.actionResults[action.id] = this.runAction(action, actionCtx, monitorCtx, dryrun)
        }

        // TODO: Implement throttling for alerts
        // Alerts are saved after the actions since if there are failures in the actions, they can be stated in the alert
        if (!dryrun && monitor.id != Monitor.NO_ID) {
            val alerts = mutableListOf<Alert>()
            findingDocPairs.forEach {
                val alert = monitorCtx.alertService!!.composeDocLevelAlert(
                    listOf(it.first),
                    listOf(it.second),
                    triggerCtx,
                    triggerResult,
                    monitorResult.alertError() ?: triggerResult.alertError()
                )
                alerts.add(alert)
            }
            monitorCtx.retryPolicy?.let { monitorCtx.alertService!!.saveAlerts(alerts, it) }
        }
        return triggerResult
    }

    private fun createFindings(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        index: String,
        docLevelQueries: List<DocLevelQuery>,
        matchingDocIds: List<String>
    ): String {
        val finding = Finding(
            id = UUID.randomUUID().toString(),
            relatedDocIds = matchingDocIds,
            monitorId = monitor.id,
            monitorName = monitor.name,
            index = index,
            docLevelQueries = docLevelQueries,
            timestamp = Instant.now()
        )

        val findingStr = finding.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS).string()
        // change this to debug.
        logger.info("Findings: $findingStr")

        // todo: below is all hardcoded, temp code and added only to test. replace this with proper Findings index lifecycle management.
        val indexRequest = IndexRequest(FINDING_HISTORY_WRITE_INDEX)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source(findingStr, XContentType.JSON)
            .id(finding.id)
            .routing(finding.id)

        monitorCtx.client!!.index(indexRequest).actionGet()
        return finding.id
    }

    // TODO: Implement action for triggers
    override suspend fun runAction(
        action: Action,
        ctx: TriggerExecutionContext,
        monitorCtx: MonitorRunnerExecutionContext,
        dryrun: Boolean
    ): ActionRunResult {
        return try {
            if (!MonitorRunnerService.isActionActionable(action, (ctx as DocumentLevelTriggerExecutionContext).alert)) {
                return ActionRunResult(action.id, action.name, mapOf(), true, null, null)
            }
            val actionOutput = mutableMapOf<String, String>()
            actionOutput[Action.SUBJECT] = if (action.subjectTemplate != null)
                MonitorRunnerService.compileTemplate(action.subjectTemplate, ctx)
            else ""
            actionOutput[Action.MESSAGE] = MonitorRunnerService.compileTemplate(action.messageTemplate, ctx)
            if (Strings.isNullOrEmpty(actionOutput[Action.MESSAGE])) {
                throw IllegalStateException("Message content missing in the Destination with id: ${action.destinationId}")
            }
            if (!dryrun) {
                withContext(Dispatchers.IO) {
                    val destination = AlertingConfigAccessor.getDestinationInfo(
                        monitorCtx.client!!,
                        monitorCtx.xContentRegistry!!,
                        action.destinationId
                    )
                    if (!destination.isAllowed(monitorCtx.allowList)) {
                        throw IllegalStateException("Monitor contains a Destination type that is not allowed: ${destination.type}")
                    }

                    val destinationCtx = monitorCtx.destinationContextFactory!!.getDestinationContext(destination)
                    actionOutput[Action.MESSAGE_ID] = destination.publish(
                        actionOutput[Action.SUBJECT],
                        actionOutput[Action.MESSAGE]!!,
                        destinationCtx,
                        monitorCtx.hostDenyList
                    )
                }
            }
            ActionRunResult(action.id, action.name, actionOutput, false, MonitorRunnerService.currentTime(), null)
        } catch (e: Exception) {
            logger.debug("Failed to run action", AlertingException.wrap(e))
            ActionRunResult(action.id, action.name, mapOf(), false, MonitorRunnerService.currentTime(), e)
        }
    }

    private fun validate(monitor: Monitor) {
        if (monitor.inputs.size > 1) {
            throw IOException("Only one input is supported with document-level-monitor.")
        }

        if (monitor.inputs[0].name() != DocLevelMonitorInput.DOC_LEVEL_INPUT_FIELD) {
            throw IOException("Invalid input with document-level-monitor.")
        }

        val docLevelMonitorInput = monitor.inputs[0] as DocLevelMonitorInput
        if (docLevelMonitorInput.indices.size > 1) {
            throw IOException("Only one index is supported with document-level-monitor.")
        }
    }

    fun createRunContext(clusterService: ClusterService, client: Client, index: String): HashMap<String, Any> {
        val lastRunContext = HashMap<String, Any>()
        lastRunContext["index"] = index
        val count = getShardsCount(clusterService, index)
        lastRunContext["shards_count"] = count

        for (i: Int in 0 until count) {
            val shard = i.toString()
            val maxSeqNo: Long = getMaxSeqNo(client, index, shard)
            lastRunContext[shard] = maxSeqNo
        }
        return lastRunContext
    }

    /**
     * Get the current max seq number of the shard. We find it by searching the last document
     *  in the primary shard.
     */
    private fun getMaxSeqNo(client: Client, index: String, shard: String): Long {
        val request: SearchRequest = SearchRequest()
            .indices(index)
            .preference("_shards:$shard")
            .source(
                SearchSourceBuilder()
                    .version(true)
                    .sort("_seq_no", SortOrder.DESC)
                    .seqNoAndPrimaryTerm(true)
                    .query(QueryBuilders.matchAllQuery())
                    .size(1)
            )
        val response: SearchResponse = client.search(request).actionGet()
        if (response.status() !== RestStatus.OK) {
            throw IOException("Failed to get max seq no for shard: $shard")
        }
        if (response.hits.hits.isEmpty())
            return -1L

        return response.hits.hits[0].seqNo
    }

    private fun getShardsCount(clusterService: ClusterService, index: String): Int {
        val allShards: List<ShardRouting> = clusterService!!.state().routingTable().allShards(index)
        return allShards.filter { it.primary() }.size
    }

    private fun runForEachQuery(
        monitorCtx: MonitorRunnerExecutionContext,
        docExecutionCtx: DocumentExecutionContext,
        query: DocLevelQuery,
        index: String
    ): Set<String> {
        val count: Int = docExecutionCtx.lastRunContext["shards_count"] as Int
        val matchingDocs = mutableSetOf<String>()
        for (i: Int in 0 until count) {
            val shard = i.toString()
            try {
                logger.info("Monitor execution for shard: $shard")

                val prevSeqNo = docExecutionCtx.lastRunContext[shard].toString().toLongOrNull()
                val maxSeqNo = docExecutionCtx.updatedLastRunContext[shard].toString().toLong()
                logger.info("Shard_$shard has MaxSeqNo: $maxSeqNo and PrevSeqNo: $prevSeqNo")

                val hits: SearchHits = searchShard(
                    monitorCtx,
                    index,
                    shard,
                    prevSeqNo,
                    maxSeqNo,
                    query.query
                )
                logger.info("Search hits for shard_$shard is: ${hits.hits.size}")

                if (hits.hits.isNotEmpty()) {
                    logger.info("found matches")
                    matchingDocs.addAll(getAllDocIds(hits))
                }
            } catch (e: Exception) {
                logger.info("Failed to run for shard $shard. Error: ${e.message}")
                logger.debug("Failed to run for shard $shard", e)
            }
        }
        return matchingDocs
    }

    private fun searchShard(
        monitorCtx: MonitorRunnerExecutionContext,
        index: String,
        shard: String,
        prevSeqNo: Long?,
        maxSeqNo: Long,
        query: String
    ): SearchHits {
        if (prevSeqNo?.equals(maxSeqNo) == true && maxSeqNo != 0L) {
            return SearchHits.empty()
        }
        val boolQueryBuilder = BoolQueryBuilder()
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("_seq_no").gt(prevSeqNo).lte(maxSeqNo))
        boolQueryBuilder.must(QueryBuilders.queryStringQuery(query))

        val request: SearchRequest = SearchRequest()
            .indices(index)
            .preference("_shards:$shard")
            .source(
                SearchSourceBuilder()
                    .version(true)
                    .query(boolQueryBuilder)
                    .size(10000) // fixme: make this configurable.
            )
        logger.info("Request: $request")
        val response: SearchResponse = monitorCtx.client!!.search(request).actionGet()
        if (response.status() !== RestStatus.OK) {
            throw IOException("Failed to search shard: $shard")
        }
        return response.hits
    }

    private fun getAllDocIds(hits: SearchHits): List<String> {
        return hits.map { hit -> hit.id }
    }
}
