/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchAction
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.model.DocumentExecutionContext
import org.opensearch.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.alerting.model.InputRunResults
import org.opensearch.alerting.model.MonitorMetadata
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.userErrorMessage
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.script.DocumentLevelTriggerExecutionContext
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.defaultToPerExecutionAction
import org.opensearch.alerting.util.getActionExecutionPolicy
import org.opensearch.alerting.workflow.WorkflowRunContext
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.routing.Preference
import org.opensearch.cluster.routing.ShardRouting
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.AlertingPluginInterface
import org.opensearch.commons.alerting.action.PublishFindingsRequest
import org.opensearch.commons.alerting.action.SubscribeFindingsResponse
import org.opensearch.commons.alerting.model.ActionExecutionResult
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.DocumentLevelTrigger
import org.opensearch.commons.alerting.model.Finding
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.action.PerAlertActionScope
import org.opensearch.commons.alerting.util.string
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.Operator
import org.opensearch.index.query.QueryBuilders
import org.opensearch.percolator.PercolateQueryBuilderExt
import org.opensearch.search.SearchHits
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder
import java.io.IOException
import java.time.Instant
import java.util.UUID
import kotlin.math.max

object DocumentLevelMonitorRunner : MonitorRunner() {
    private val logger = LogManager.getLogger(javaClass)

    override suspend fun runMonitor(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryrun: Boolean,
        workflowRunContext: WorkflowRunContext?,
        executionId: String
    ): MonitorRunResult<DocumentLevelTriggerRunResult> {
        logger.debug("Document-level-monitor is running ...")
        val isTempMonitor = dryrun || monitor.id == Monitor.NO_ID
        var monitorResult = MonitorRunResult<DocumentLevelTriggerRunResult>(monitor.name, periodStart, periodEnd)

        try {
            monitorCtx.alertIndices!!.createOrUpdateAlertIndex(monitor.dataSources)
            monitorCtx.alertIndices!!.createOrUpdateInitialAlertHistoryIndex(monitor.dataSources)
            monitorCtx.alertIndices!!.createOrUpdateInitialFindingHistoryIndex(monitor.dataSources)
        } catch (e: Exception) {
            val id = if (monitor.id.trim().isEmpty()) "_na_" else monitor.id
            logger.error("Error setting up alerts and findings indices for monitor: $id", e)
            monitorResult = monitorResult.copy(error = AlertingException.wrap(e))
        }

        try {
            validate(monitor)
        } catch (e: Exception) {
            logger.error("Failed to start Document-level-monitor. Error: ${e.message}")
            monitorResult = monitorResult.copy(error = AlertingException.wrap(e))
        }

        var (monitorMetadata, _) = MonitorMetadataService.getOrCreateMetadata(
            monitor = monitor,
            createWithRunContext = false,
            skipIndex = isTempMonitor,
            workflowRunContext?.workflowMetadataId
        )

        val docLevelMonitorInput = monitor.inputs[0] as DocLevelMonitorInput

        val queries: List<DocLevelQuery> = docLevelMonitorInput.queries

        val lastRunContext = if (monitorMetadata.lastRunContext.isNullOrEmpty()) mutableMapOf()
        else monitorMetadata.lastRunContext.toMutableMap() as MutableMap<String, MutableMap<String, Any>>

        val updatedLastRunContext = lastRunContext.toMutableMap()

        val queryToDocIds = mutableMapOf<DocLevelQuery, MutableSet<String>>()
        val inputRunResults = mutableMapOf<String, MutableSet<String>>()
        val docsToQueries = mutableMapOf<String, MutableList<String>>()

        try {
            // Resolve all passed indices to concrete indices
            val concreteIndices = IndexUtils.resolveAllIndices(
                docLevelMonitorInput.indices,
                monitorCtx.clusterService!!,
                monitorCtx.indexNameExpressionResolver!!
            )
            if (concreteIndices.isEmpty()) {
                logger.error("indices not found-${docLevelMonitorInput.indices.joinToString(",")}")
                throw IndexNotFoundException(docLevelMonitorInput.indices.joinToString(","))
            }

            monitorCtx.docLevelMonitorQueries!!.initDocLevelQueryIndex(monitor.dataSources)
            monitorCtx.docLevelMonitorQueries!!.indexDocLevelQueries(
                monitor = monitor,
                monitorId = monitor.id,
                monitorMetadata,
                indexTimeout = monitorCtx.indexTimeout!!
            )

            // cleanup old indices that are not monitored anymore from the same monitor
            for (ind in updatedLastRunContext.keys) {
                if (!concreteIndices.contains(ind)) {
                    updatedLastRunContext.remove(ind)
                }
            }

            // Map of document ids per index when monitor is workflow delegate and has chained findings
            val matchingDocIdsPerIndex = workflowRunContext?.matchingDocIdsPerIndex

            docLevelMonitorInput.indices.forEach { indexName ->
                val concreteIndices = IndexUtils.resolveAllIndices(
                    listOf(indexName),
                    monitorCtx.clusterService!!,
                    monitorCtx.indexNameExpressionResolver!!
                )
                val updatedIndexName = indexName.replace("*", "_")
                val conflictingFields = monitorCtx.docLevelMonitorQueries!!.getAllConflictingFields(
                    monitorCtx.clusterService!!.state(),
                    concreteIndices
                )

                concreteIndices.forEach { concreteIndexName ->
                    // Prepare lastRunContext for each index
                    val indexLastRunContext = lastRunContext.getOrPut(concreteIndexName) {
                        val isIndexCreatedRecently = createdRecently(
                            monitor,
                            periodStart,
                            periodEnd,
                            monitorCtx.clusterService!!.state().metadata.index(concreteIndexName)
                        )
                        MonitorMetadataService.createRunContextForIndex(concreteIndexName, isIndexCreatedRecently)
                    }

                    // Prepare updatedLastRunContext for each index
                    val indexUpdatedRunContext = updateLastRunContext(
                        indexLastRunContext.toMutableMap(),
                        monitorCtx,
                        concreteIndexName
                    ) as MutableMap<String, Any>
                    updatedLastRunContext[concreteIndexName] = indexUpdatedRunContext

                    val count: Int = indexLastRunContext["shards_count"] as Int
                    for (i: Int in 0 until count) {
                        val shard = i.toString()

                        // update lastRunContext if its a temp monitor as we only want to view the last bit of data then
                        // TODO: If dryrun, we should make it so we limit the search as this could still potentially give us lots of data
                        if (isTempMonitor) {
                            indexLastRunContext[shard] = max(-1, (indexUpdatedRunContext[shard] as String).toInt() - 10)
                        }
                    }

                    // Prepare DocumentExecutionContext for each index
                    val docExecutionContext = DocumentExecutionContext(queries, indexLastRunContext, indexUpdatedRunContext)

                    val matchingDocs = getMatchingDocs(
                        monitor,
                        monitorCtx,
                        docExecutionContext,
                        updatedIndexName,
                        concreteIndexName,
                        conflictingFields.toList(),
                        matchingDocIdsPerIndex?.get(concreteIndexName)
                    )

                    if (matchingDocs.isNotEmpty()) {
                        val matchedQueriesForDocs = getMatchedQueries(
                            monitorCtx,
                            matchingDocs.map { it.second },
                            monitor,
                            monitorMetadata,
                            updatedIndexName,
                            concreteIndexName
                        )

                        matchedQueriesForDocs.forEach { hit ->
                            val id = hit.id
                                .replace("_${updatedIndexName}_${monitor.id}", "")
                                .replace("_${concreteIndexName}_${monitor.id}", "")

                            val docIndices = hit.field("_percolator_document_slot").values.map { it.toString().toInt() }
                            docIndices.forEach { idx ->
                                val docIndex = "${matchingDocs[idx].first}|$concreteIndexName"
                                inputRunResults.getOrPut(id) { mutableSetOf() }.add(docIndex)
                                docsToQueries.getOrPut(docIndex) { mutableListOf() }.add(id)
                            }
                        }
                    }
                }
            }
            monitorResult = monitorResult.copy(inputResults = InputRunResults(listOf(inputRunResults)))

            /*
             populate the map queryToDocIds with pairs of <DocLevelQuery object from queries in monitor metadata &
             list of matched docId from inputRunResults>
             this fixes the issue of passing id, name, tags fields of DocLevelQuery object correctly to TriggerExpressionParser
             */
            queries.forEach {
                if (inputRunResults.containsKey(it.id)) {
                    queryToDocIds[it] = inputRunResults[it.id]!!
                }
            }

            val idQueryMap: Map<String, DocLevelQuery> = queries.associateBy { it.id }

            val triggerResults = mutableMapOf<String, DocumentLevelTriggerRunResult>()
            // If there are no triggers defined, we still want to generate findings
            if (monitor.triggers.isEmpty()) {
                if (dryrun == false && monitor.id != Monitor.NO_ID) {
                    docsToQueries.forEach {
                        val triggeredQueries = it.value.map { queryId -> idQueryMap[queryId]!! }
                        createFindings(monitor, monitorCtx, triggeredQueries, it.key, true)
                    }
                }
            } else {
                monitor.triggers.forEach {
                    triggerResults[it.id] = runForEachDocTrigger(
                        monitorCtx,
                        monitorResult,
                        it as DocumentLevelTrigger,
                        monitor,
                        idQueryMap,
                        docsToQueries,
                        queryToDocIds,
                        dryrun,
                        executionId = executionId,
                        workflowRunContext = workflowRunContext
                    )
                }
            }
            // Don't update monitor if this is a test monitor
            if (!isTempMonitor) {
                // If any error happened during trigger execution, upsert monitor error alert
                val errorMessage = constructErrorMessageFromTriggerResults(triggerResults = triggerResults)
                if (errorMessage.isNotEmpty()) {
                    monitorCtx.alertService!!.upsertMonitorErrorAlert(
                        monitor = monitor,
                        errorMessage = errorMessage,
                        executionId = executionId,
                        workflowRunContext
                    )
                } else {
                    onSuccessfulMonitorRun(monitorCtx, monitor)
                }

                MonitorMetadataService.upsertMetadata(
                    monitorMetadata.copy(lastRunContext = updatedLastRunContext),
                    true
                )
            }

            // TODO: Update the Document as part of the Trigger and return back the trigger action result
            return monitorResult.copy(triggerResults = triggerResults)
        } catch (e: Exception) {
            val errorMessage = ExceptionsHelper.detailedMessage(e)
            monitorCtx.alertService!!.upsertMonitorErrorAlert(monitor, errorMessage, executionId, workflowRunContext)
            logger.error("Failed running Document-level-monitor ${monitor.name}", e)
            val alertingException = AlertingException(
                errorMessage,
                RestStatus.INTERNAL_SERVER_ERROR,
                e
            )
            return monitorResult.copy(error = alertingException, inputResults = InputRunResults(emptyList(), alertingException))
        }
    }

    private suspend fun onSuccessfulMonitorRun(monitorCtx: MonitorRunnerExecutionContext, monitor: Monitor) {
        monitorCtx.alertService!!.clearMonitorErrorAlert(monitor)
        if (monitor.dataSources.alertsHistoryIndex != null) {
            monitorCtx.alertService!!.moveClearedErrorAlertsToHistory(
                monitor.id,
                monitor.dataSources.alertsIndex,
                monitor.dataSources.alertsHistoryIndex!!
            )
        }
    }

    private fun constructErrorMessageFromTriggerResults(
        triggerResults: MutableMap<String, DocumentLevelTriggerRunResult>? = null
    ): String {
        var errorMessage = ""
        if (triggerResults != null) {
            val triggersErrorBuilder = StringBuilder()
            triggerResults.forEach {
                if (it.value.error != null) {
                    triggersErrorBuilder.append("[${it.key}]: [${it.value.error!!.userErrorMessage()}]").append(" | ")
                }
            }
            if (triggersErrorBuilder.isNotEmpty()) {
                errorMessage = "Trigger errors: $triggersErrorBuilder"
            }
        }
        return errorMessage
    }

    private suspend fun runForEachDocTrigger(
        monitorCtx: MonitorRunnerExecutionContext,
        monitorResult: MonitorRunResult<DocumentLevelTriggerRunResult>,
        trigger: DocumentLevelTrigger,
        monitor: Monitor,
        idQueryMap: Map<String, DocLevelQuery>,
        docsToQueries: Map<String, List<String>>,
        queryToDocIds: Map<DocLevelQuery, Set<String>>,
        dryrun: Boolean,
        workflowRunContext: WorkflowRunContext?,
        executionId: String
    ): DocumentLevelTriggerRunResult {
        val triggerCtx = DocumentLevelTriggerExecutionContext(monitor, trigger)
        val triggerResult = monitorCtx.triggerService!!.runDocLevelTrigger(monitor, trigger, queryToDocIds)

        val findings = mutableListOf<String>()
        val findingDocPairs = mutableListOf<Pair<String, String>>()

        // TODO: Implement throttling for findings
        docsToQueries.forEach {
            val triggeredQueries = it.value.map { queryId -> idQueryMap[queryId]!! }
            val findingId = createFindings(
                monitor,
                monitorCtx,
                triggeredQueries,
                it.key,
                !dryrun && monitor.id != Monitor.NO_ID,
                executionId
            )
            findings.add(findingId)

            if (triggerResult.triggeredDocs.contains(it.key)) {
                findingDocPairs.add(Pair(findingId, it.key))
            }
        }

        val actionCtx = triggerCtx.copy(
            triggeredDocs = triggerResult.triggeredDocs,
            relatedFindings = findings,
            error = monitorResult.error ?: triggerResult.error
        )

        val alerts = mutableListOf<Alert>()
        findingDocPairs.forEach {
            val alert = monitorCtx.alertService!!.composeDocLevelAlert(
                listOf(it.first),
                listOf(it.second),
                triggerCtx,
                monitorResult.alertError() ?: triggerResult.alertError(),
                executionId = executionId,
                workflorwRunContext = workflowRunContext
            )
            alerts.add(alert)
        }

        val shouldDefaultToPerExecution = defaultToPerExecutionAction(
            monitorCtx.maxActionableAlertCount,
            monitorId = monitor.id,
            triggerId = trigger.id,
            totalActionableAlertCount = alerts.size,
            monitorOrTriggerError = actionCtx.error
        )

        for (action in trigger.actions) {
            val actionExecutionScope = action.getActionExecutionPolicy(monitor)!!.actionExecutionScope
            if (actionExecutionScope is PerAlertActionScope && !shouldDefaultToPerExecution) {
                for (alert in alerts) {
                    val actionResults = this.runAction(action, actionCtx.copy(alerts = listOf(alert)), monitorCtx, monitor, dryrun)
                    triggerResult.actionResultsMap.getOrPut(alert.id) { mutableMapOf() }
                    triggerResult.actionResultsMap[alert.id]?.set(action.id, actionResults)
                }
            } else if (alerts.isNotEmpty()) {
                val actionResults = this.runAction(action, actionCtx.copy(alerts = alerts), monitorCtx, monitor, dryrun)
                for (alert in alerts) {
                    triggerResult.actionResultsMap.getOrPut(alert.id) { mutableMapOf() }
                    triggerResult.actionResultsMap[alert.id]?.set(action.id, actionResults)
                }
            }
        }

        // Alerts are saved after the actions since if there are failures in the actions, they can be stated in the alert
        if (!dryrun && monitor.id != Monitor.NO_ID) {
            val updatedAlerts = alerts.map { alert ->
                val actionResults = triggerResult.actionResultsMap.getOrDefault(alert.id, emptyMap())
                val actionExecutionResults = actionResults.values.map { actionRunResult ->
                    ActionExecutionResult(actionRunResult.actionId, actionRunResult.executionTime, if (actionRunResult.throttled) 1 else 0)
                }
                alert.copy(actionExecutionResults = actionExecutionResults)
            }

            monitorCtx.retryPolicy?.let {
                monitorCtx.alertService!!.saveAlerts(
                    monitor.dataSources,
                    updatedAlerts,
                    it,
                    routingId = monitor.id
                )
            }
        }
        return triggerResult
    }

    private suspend fun createFindings(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        docLevelQueries: List<DocLevelQuery>,
        matchingDocId: String,
        shouldCreateFinding: Boolean,
        workflowExecutionId: String? = null,
    ): String {
        // Before the "|" is the doc id and after the "|" is the index
        val docIndex = matchingDocId.split("|")

        val finding = Finding(
            id = UUID.randomUUID().toString(),
            relatedDocIds = listOf(docIndex[0]),
            correlatedDocIds = listOf(docIndex[0]),
            monitorId = monitor.id,
            monitorName = monitor.name,
            index = docIndex[1],
            docLevelQueries = docLevelQueries,
            timestamp = Instant.now(),
            executionId = workflowExecutionId
        )

        val findingStr = finding.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS).string()
        logger.debug("Findings: $findingStr")

        if (shouldCreateFinding) {
            val indexRequest = IndexRequest(monitor.dataSources.findingsIndex)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(findingStr, XContentType.JSON)
                .id(finding.id)
                .routing(finding.id)

            monitorCtx.client!!.suspendUntil<Client, IndexResponse> {
                monitorCtx.client!!.index(indexRequest, it)
            }
        }

        try {
            publishFinding(monitor, monitorCtx, finding)
        } catch (e: Exception) {
            // suppress exception
            logger.error("Optional finding callback failed", e)
        }
        return finding.id
    }

    private fun publishFinding(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        finding: Finding
    ) {
        val publishFindingsRequest = PublishFindingsRequest(monitor.id, finding)
        AlertingPluginInterface.publishFinding(
            monitorCtx.client!! as NodeClient,
            publishFindingsRequest,
            object : ActionListener<SubscribeFindingsResponse> {
                override fun onResponse(response: SubscribeFindingsResponse) {}

                override fun onFailure(e: Exception) {}
            }
        )
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

        if ((monitor.inputs[0] as DocLevelMonitorInput).indices.isEmpty()) {
            throw IllegalArgumentException("DocLevelMonitorInput has no indices")
        }
    }

    // Checks if the index was created from the last execution run or when the monitor was last updated to ensure that
    // new index is monitored from the beginning of that index
    private fun createdRecently(
        monitor: Monitor,
        periodStart: Instant,
        periodEnd: Instant,
        indexMetadata: IndexMetadata
    ): Boolean {
        val lastExecutionTime = if (periodStart == periodEnd) monitor.lastUpdateTime else periodStart
        val indexCreationDate = indexMetadata.settings.get("index.creation_date")?.toLong() ?: 0L
        return indexCreationDate > lastExecutionTime.toEpochMilli()
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
        index: String,
        concreteIndex: String,
        conflictingFields: List<String>,
        docIds: List<String>? = null
    ): List<Pair<String, BytesReference>> {
        val count: Int = docExecutionCtx.updatedLastRunContext["shards_count"] as Int
        val matchingDocs = mutableListOf<Pair<String, BytesReference>>()
        for (i: Int in 0 until count) {
            val shard = i.toString()
            try {
                val maxSeqNo: Long = docExecutionCtx.updatedLastRunContext[shard].toString().toLong()
                val prevSeqNo = docExecutionCtx.lastRunContext[shard].toString().toLongOrNull()

                val hits: SearchHits = searchShard(
                    monitorCtx,
                    concreteIndex,
                    shard,
                    prevSeqNo,
                    maxSeqNo,
                    null,
                    docIds
                )

                if (hits.hits.isNotEmpty()) {
                    matchingDocs.addAll(getAllDocs(hits, index, concreteIndex, monitor.id, conflictingFields))
                }
            } catch (e: Exception) {
                logger.warn("Failed to run for shard $shard. Error: ${e.message}")
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
        query: String?,
        docIds: List<String>? = null
    ): SearchHits {
        if (prevSeqNo?.equals(maxSeqNo) == true && maxSeqNo != 0L) {
            return SearchHits.empty()
        }
        val boolQueryBuilder = BoolQueryBuilder()
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("_seq_no").gt(prevSeqNo).lte(maxSeqNo))

        if (query != null) {
            boolQueryBuilder.must(QueryBuilders.queryStringQuery(query))
        }

        if (!docIds.isNullOrEmpty()) {
            boolQueryBuilder.filter(QueryBuilders.termsQuery("_id", docIds))
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
            .preference(Preference.PRIMARY_FIRST.type())
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
        monitorMetadata: MonitorMetadata,
        index: String,
        concreteIndex: String
    ): SearchHits {
        val boolQueryBuilder = BoolQueryBuilder().must(QueryBuilders.matchQuery("index", index).operator(Operator.AND))

        val percolateQueryBuilder = PercolateQueryBuilderExt("query", docs, XContentType.JSON)
        if (monitor.id.isNotEmpty()) {
            boolQueryBuilder.must(QueryBuilders.matchQuery("monitor_id", monitor.id).operator(Operator.AND))
        }
        boolQueryBuilder.filter(percolateQueryBuilder)

        val queryIndex = monitorMetadata.sourceToQueryIndexMapping[index + monitor.id]
        if (queryIndex == null) {
            val message = "Failed to resolve concrete queryIndex from sourceIndex during monitor execution!" +
                " sourceIndex:$concreteIndex queryIndex:${monitor.dataSources.queryIndex}"
            logger.error(message)
            throw AlertingException.wrap(
                OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR)
            )
        }
        val searchRequest = SearchRequest(queryIndex).preference(Preference.PRIMARY_FIRST.type())
        val searchSourceBuilder = SearchSourceBuilder()
        searchSourceBuilder.query(boolQueryBuilder)
        searchRequest.source(searchSourceBuilder)

        var response: SearchResponse
        try {
            response = monitorCtx.client!!.suspendUntil {
                monitorCtx.client!!.execute(SearchAction.INSTANCE, searchRequest, it)
            }
        } catch (e: Exception) {
            throw IllegalStateException(
                "Failed to run percolate search for sourceIndex [$index] and queryIndex [$queryIndex] for ${docs.size} document(s)", e
            )
        }

        if (response.status() !== RestStatus.OK) {
            throw IOException("Failed to search percolate index: $queryIndex")
        }
        return response.hits
    }

    private fun getAllDocs(
        hits: SearchHits,
        index: String,
        concreteIndex: String,
        monitorId: String,
        conflictingFields: List<String>
    ): List<Pair<String, BytesReference>> {
        return hits.map { hit ->
            val sourceMap = hit.sourceAsMap

            transformDocumentFieldNames(
                sourceMap,
                conflictingFields,
                "_${index}_$monitorId",
                "_${concreteIndex}_$monitorId",
                ""
            )

            var xContentBuilder = XContentFactory.jsonBuilder().map(sourceMap)

            val sourceRef = BytesReference.bytes(xContentBuilder)

            logger.debug("Document [${hit.id}] payload after transform: ", sourceRef.utf8ToString())

            Pair(hit.id, sourceRef)
        }
    }

    /**
     * Traverses document fields in leaves recursively and appends [fieldNameSuffixIndex] to field names with same names
     * but different mappings & [fieldNameSuffixPattern] to field names which have unique names.
     *
     * Example for index name is my_log_index and Monitor ID is TReewWdsf2gdJFV:
     * {                         {
     *   "a": {                     "a": {
     *     "b": 1234      ---->       "b_my_log_index_TReewWdsf2gdJFV": 1234
     *   }                          }
     * }
     *
     * @param jsonAsMap               Input JSON (as Map)
     * @param fieldNameSuffix         Field suffix which is appended to existing field name
     */
    private fun transformDocumentFieldNames(
        jsonAsMap: MutableMap<String, Any>,
        conflictingFields: List<String>,
        fieldNameSuffixPattern: String,
        fieldNameSuffixIndex: String,
        fieldNamePrefix: String
    ) {
        val tempMap = mutableMapOf<String, Any>()
        val it: MutableIterator<Map.Entry<String, Any>> = jsonAsMap.entries.iterator()
        while (it.hasNext()) {
            val entry = it.next()
            if (entry.value is Map<*, *>) {
                transformDocumentFieldNames(
                    entry.value as MutableMap<String, Any>,
                    conflictingFields,
                    fieldNameSuffixPattern,
                    fieldNameSuffixIndex,
                    if (fieldNamePrefix == "") entry.key else "$fieldNamePrefix.${entry.key}"
                )
            } else if (!entry.key.endsWith(fieldNameSuffixPattern) && !entry.key.endsWith(fieldNameSuffixIndex)) {
                var alreadyReplaced = false
                conflictingFields.forEach { conflictingField ->
                    if (conflictingField == "$fieldNamePrefix.${entry.key}" || (fieldNamePrefix == "" && conflictingField == entry.key)) {
                        tempMap["${entry.key}$fieldNameSuffixIndex"] = entry.value
                        it.remove()
                        alreadyReplaced = true
                    }
                }
                if (!alreadyReplaced) {
                    tempMap["${entry.key}$fieldNameSuffixPattern"] = entry.value
                    it.remove()
                }
            }
        }
        jsonAsMap.putAll(tempMap)
    }
}
