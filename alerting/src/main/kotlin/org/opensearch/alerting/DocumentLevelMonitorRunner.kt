/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchAction
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.alerts.AlertIndices.Companion.FINDING_HISTORY_WRITE_INDEX
import org.opensearch.alerting.core.model.DocLevelMonitorInput
import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.DocumentExecutionContext
import org.opensearch.alerting.model.DocumentLevelTrigger
import org.opensearch.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.alerting.model.Finding
import org.opensearch.alerting.model.InputRunResults
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.opensearchapi.string
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.script.DocumentLevelTriggerExecutionContext
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.updateMonitor
import org.opensearch.client.Client
import org.opensearch.cluster.routing.ShardRouting
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.percolator.PercolateQueryBuilderExt
import org.opensearch.rest.RestStatus
import org.opensearch.search.SearchHits
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder
import java.io.IOException
import java.time.Instant
import java.util.UUID
import kotlin.collections.HashMap
import kotlin.math.max

object DocumentLevelMonitorRunner : MonitorRunner() {
    private val logger = LogManager.getLogger(javaClass)

    override suspend fun runMonitor(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryrun: Boolean
    ): MonitorRunResult<DocumentLevelTriggerRunResult> {
        logger.debug("Document-level-monitor is running ...")
        var monitorResult = MonitorRunResult<DocumentLevelTriggerRunResult>(monitor.name, periodStart, periodEnd)

        try {
            monitorCtx.alertIndices!!.createOrUpdateAlertIndex()
            monitorCtx.alertIndices!!.createOrUpdateInitialAlertHistoryIndex()
            monitorCtx.alertIndices!!.createOrUpdateInitialFindingHistoryIndex()
        } catch (e: Exception) {
            val id = if (monitor.id.trim().isEmpty()) "_na_" else monitor.id
            logger.error("Error setting up alerts and findings indices for monitor: $id", e)
            return monitorResult.copy(error = AlertingException.wrap(e))
        }

        try {
            validate(monitor)
        } catch (e: Exception) {
            logger.info("Failed to start Document-level-monitor. Error: ${e.message}")
            return monitorResult.copy(error = AlertingException.wrap(e))
        }

        val docLevelMonitorInput = monitor.inputs[0] as DocLevelMonitorInput
        val index = docLevelMonitorInput.indices[0]
        val queries: List<DocLevelQuery> = docLevelMonitorInput.queries

        val isTempMonitor = dryrun || monitor.id == Monitor.NO_ID
        val lastRunContext = if (monitor.lastRunContext.isNullOrEmpty()) mutableMapOf()
        else monitor.lastRunContext.toMutableMap() as MutableMap<String, MutableMap<String, Any>>
        val updatedLastRunContext = lastRunContext.toMutableMap()

        val queryToDocIds = mutableMapOf<DocLevelQuery, MutableSet<String>>()
        val docsToQueries = mutableMapOf<String, MutableList<String>>()
        val idQueryMap = mutableMapOf<String, DocLevelQuery>()

        try {
            val getAliasesRequest = GetAliasesRequest(index)
            val getAliasesResponse: GetAliasesResponse = monitorCtx.client!!.suspendUntil {
                monitorCtx.client!!.admin().indices().getAliases(getAliasesRequest, it)
            }
            val aliasIndices = getAliasesResponse.aliases.keys().map { it.value }

            val isAlias = aliasIndices.isNotEmpty()
            logger.debug("index, $index, is an alias index: $isAlias")

            // If the input index is an alias, creating a list of all indices associated with that alias;
            // else creating a list containing the single index input
            val indices = if (isAlias) getAliasesResponse.aliases.keys().map { it.value } else listOf(index)

            indices.forEach { indexName ->
                // Prepare lastRunContext for each index
                val indexLastRunContext = lastRunContext.getOrPut(indexName) {
                    createRunContext(monitorCtx.clusterService!!, monitorCtx.client!!, indexName)
                }

                // Prepare updatedLastRunContext for each index
                val indexUpdatedRunContext = updateLastRunContext(
                    indexLastRunContext.toMutableMap(),
                    monitorCtx,
                    indexName
                ) as MutableMap<String, Any>
                updatedLastRunContext[indexName] = indexUpdatedRunContext

                val count: Int = indexLastRunContext["shards_count"] as Int
                for (i: Int in 0 until count) {
                    val shard = i.toString()

                    // update lastRunContext if its a temp monitor as we only want to view the last bit of data then
                    // TODO: If dryrun, we should make it so we limit the search as this could still potentially give us lots of data
                    if (isTempMonitor) {
                        indexLastRunContext[shard] = max(-1, (indexUpdatedRunContext[shard] as String).toInt() - 1)
                    }
                }

                // Prepare DocumentExecutionContext for each index
                val docExecutionContext = DocumentExecutionContext(queries, indexLastRunContext, indexUpdatedRunContext)

                val matchingDocs = getMatchingDocs(monitor, monitorCtx, docExecutionContext, indexName)

                if (matchingDocs.isNotEmpty()) {
                    val matchedQueriesForDocs = getMatchedQueries(monitorCtx, matchingDocs.map { it.second }, monitor, indexName)

                    matchedQueriesForDocs.forEach { hit ->
                        val (id, query) = Pair(
                            hit.id.replace("_${indexName}_${monitor.id}", ""),
                            ((hit.sourceAsMap["query"] as HashMap<*, *>)["query_string"] as HashMap<*, *>)["query"]
                        )
                        val docLevelQuery = DocLevelQuery(id, id, query.toString())

                        val docIndices = hit.field("_percolator_document_slot").values.map { it.toString().toInt() }
                        docIndices.forEach { idx ->
                            val docIndex = "${matchingDocs[idx].first}|$indexName"
                            if (queryToDocIds.containsKey(docLevelQuery)) {
                                queryToDocIds[docLevelQuery]?.add(docIndex)
                            } else {
                                queryToDocIds[docLevelQuery] = mutableSetOf(docIndex)
                            }

                            if (docsToQueries.containsKey(docIndex)) {
                                docsToQueries[docIndex]?.add(id)
                            } else {
                                docsToQueries[docIndex] = mutableListOf(id)
                            }
                        }
                    }
                }
            }
        } catch (e: Exception) {
            logger.error("Failed to start Document-level-monitor $index. Error: ${e.message}", e)
            val alertingException = AlertingException.wrap(e)
            return monitorResult.copy(error = alertingException, inputResults = InputRunResults(emptyList(), alertingException))
        }

        val queryInputResults = queryToDocIds.mapKeys { it.key.id }
        monitorResult = monitorResult.copy(inputResults = InputRunResults(listOf(queryInputResults)))
        val queryIds = queries.map {
            idQueryMap[it.id] = it
            it.id
        }

        val triggerResults = mutableMapOf<String, DocumentLevelTriggerRunResult>()
        monitor.triggers.forEach {
            triggerResults[it.id] = runForEachDocTrigger(
                monitorCtx,
                monitorResult,
                it as DocumentLevelTrigger,
                monitor,
                idQueryMap,
                docsToQueries,
                queryToDocIds,
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
        queryToDocIds: Map<DocLevelQuery, Set<String>>,
        dryrun: Boolean
    ): DocumentLevelTriggerRunResult {
        val triggerCtx = DocumentLevelTriggerExecutionContext(monitor, trigger)
        val triggerResult = monitorCtx.triggerService!!.runDocLevelTrigger(monitor, trigger, queryToDocIds)

        logger.info("trigger results")
        logger.info(triggerResult.triggeredDocs.toString())

        val findings = mutableListOf<String>()
        val findingDocPairs = mutableListOf<Pair<String, String>>()

        // TODO: Implement throttling for findings
        if (!dryrun && monitor.id != Monitor.NO_ID) {
            docsToQueries.forEach {
                val triggeredQueries = it.value.map { queryId -> idQueryMap[queryId]!! }
                val findingId = createFindings(monitor, monitorCtx, triggeredQueries, it.key)
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

    private suspend fun createFindings(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        docLevelQueries: List<DocLevelQuery>,
        matchingDocId: String
    ): String {
        // Before the "|" is the doc id and after the "|" is the index
        val docIndex = matchingDocId.split("|")

        val finding = Finding(
            id = UUID.randomUUID().toString(),
            relatedDocIds = listOf(docIndex[0]),
            monitorId = monitor.id,
            monitorName = monitor.name,
            index = docIndex[1],
            docLevelQueries = docLevelQueries,
            timestamp = Instant.now()
        )

        val findingStr = finding.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS).string()
        logger.debug("Findings: $findingStr")

        val indexRequest = IndexRequest(FINDING_HISTORY_WRITE_INDEX)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source(findingStr, XContentType.JSON)
            .id(finding.id)
            .routing(finding.id)

        val indexResponse: IndexResponse = monitorCtx.client!!.suspendUntil {
            monitorCtx.client!!.index(indexRequest, it)
        }
        return finding.id
    }

    private suspend fun updateLastRunContext(
        lastRunContext: Map<String, Any>,
        monitorCtx: MonitorRunnerExecutionContext,
        index: String
    ): Map<String, Any> {
        val count: Int = getShardsCount(monitorCtx.clusterService!!, index)
        val updatedLastRunContext = lastRunContext.toMutableMap()
        for (i: Int in 0 until count) {
            val shard = i.toString()
            val maxSeqNo: Long = getMaxSeqNo(monitorCtx.client!!, index, shard)
            updatedLastRunContext[shard] = maxSeqNo.toString()
        }
        return updatedLastRunContext
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

    suspend fun createRunContext(clusterService: ClusterService, client: Client, index: String): HashMap<String, Any> {
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
    private suspend fun getMaxSeqNo(client: Client, index: String, shard: String): Long {
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
        val response: SearchResponse = client.suspendUntil { client.search(request, it) }
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

    private suspend fun getMatchingDocs(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        docExecutionCtx: DocumentExecutionContext,
        index: String
    ): List<Pair<String, BytesReference>> {
        val count: Int = docExecutionCtx.updatedLastRunContext["shards_count"] as Int
        val matchingDocs = mutableListOf<Pair<String, BytesReference>>()
        for (i: Int in 0 until count) {
            val shard = i.toString()
            try {
                logger.info("Monitor execution for shard: $shard")
                val maxSeqNo: Long = docExecutionCtx.updatedLastRunContext[shard].toString().toLong()
                val prevSeqNo = docExecutionCtx.lastRunContext[shard].toString().toLongOrNull()
                logger.info("prevSeq: $prevSeqNo, maxSeq: $maxSeqNo")

                val hits: SearchHits = searchShard(
                    monitorCtx,
                    index,
                    shard,
                    prevSeqNo,
                    maxSeqNo,
                    null
                )
                logger.info("Search hits for shard_$shard is: ${hits.hits.size}")

                if (hits.hits.isNotEmpty()) {
                    matchingDocs.addAll(getAllDocs(hits, index, monitor.id))
                }
            } catch (e: Exception) {
                logger.info("Failed to run for shard $shard. Error: ${e.message}")
            }
        }
        return matchingDocs
    }

    private suspend fun searchShard(
        monitorCtx: MonitorRunnerExecutionContext,
        index: String,
        shard: String,
        prevSeqNo: Long?,
        maxSeqNo: Long,
        query: String?
    ): SearchHits {
        if (prevSeqNo?.equals(maxSeqNo) == true && maxSeqNo != 0L) {
            return SearchHits.empty()
        }
        val boolQueryBuilder = BoolQueryBuilder()
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("_seq_no").gt(prevSeqNo).lte(maxSeqNo))

        if (query != null) {
            boolQueryBuilder.must(QueryBuilders.queryStringQuery(query))
        }

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
        val response: SearchResponse = monitorCtx.client!!.suspendUntil { monitorCtx.client!!.search(request, it) }
        if (response.status() !== RestStatus.OK) {
            throw IOException("Failed to search shard: $shard")
        }
        return response.hits
    }

    private suspend fun getMatchedQueries(
        monitorCtx: MonitorRunnerExecutionContext,
        docs: List<BytesReference>,
        monitor: Monitor,
        index: String
    ): SearchHits {
        val boolQueryBuilder = BoolQueryBuilder().filter(QueryBuilders.matchQuery("index", index))

        val percolateQueryBuilder = PercolateQueryBuilderExt("query", docs, XContentType.JSON)
        if (monitor.id.isNotEmpty()) {
            boolQueryBuilder.filter(QueryBuilders.matchQuery("monitor_id", monitor.id))
        }
        boolQueryBuilder.filter(percolateQueryBuilder)

        val searchRequest = SearchRequest(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)
        val searchSourceBuilder = SearchSourceBuilder()
        searchSourceBuilder.query(boolQueryBuilder)
        searchRequest.source(searchSourceBuilder)

        val response: SearchResponse = monitorCtx.client!!.suspendUntil {
            monitorCtx.client!!.execute(SearchAction.INSTANCE, searchRequest, it)
        }

        if (response.status() !== RestStatus.OK) {
            throw IOException("Failed to search percolate index: ${ScheduledJob.DOC_LEVEL_QUERIES_INDEX}")
        }
        return response.hits
    }

    private fun getAllDocs(hits: SearchHits, index: String, monitorId: String): List<Pair<String, BytesReference>> {
        return hits.map { hit ->
            val sourceMap = hit.sourceAsMap

            var xContentBuilder = XContentFactory.jsonBuilder().startObject()
            sourceMap.forEach { (k, v) ->
                xContentBuilder = xContentBuilder.field("${k}_${index}_$monitorId", v)
            }
            xContentBuilder = xContentBuilder.endObject()

            val sourceRef = BytesReference.bytes(xContentBuilder)

            Pair(hit.id, sourceRef)
        }
    }
}
