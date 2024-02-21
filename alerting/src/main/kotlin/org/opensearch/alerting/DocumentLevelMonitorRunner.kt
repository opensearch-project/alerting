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
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.script.DocumentLevelTriggerExecutionContext
import org.opensearch.alerting.settings.AlertingSettings.Companion.PERCOLATE_QUERY_DOCS_SIZE_MEMORY_PERCENTAGE_LIMIT
import org.opensearch.alerting.settings.AlertingSettings.Companion.PERCOLATE_QUERY_MAX_NUM_DOCS_IN_MEMORY
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.defaultToPerExecutionAction
import org.opensearch.alerting.util.getActionExecutionPolicy
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.routing.ShardRouting
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.ActionExecutionResult
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.DocumentLevelTrigger
import org.opensearch.commons.alerting.model.Finding
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.action.PerAlertActionScope
import org.opensearch.commons.alerting.util.string
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.Operator
import org.opensearch.index.query.QueryBuilders
import org.opensearch.percolator.PercolateQueryBuilderExt
import org.opensearch.rest.RestStatus
import org.opensearch.search.SearchHit
import org.opensearch.search.SearchHits
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder
import java.io.IOException
import java.time.Instant
import java.util.UUID
import java.util.stream.Collectors
import kotlin.math.max

class DocumentLevelMonitorRunner : MonitorRunner() {
    private val logger = LogManager.getLogger(javaClass)
    var nonPercolateSearchesTimeTakenStat = 0L
    var percolateQueriesTimeTakenStat = 0L
    var totalDocsQueriedStat = 0L
    var docTransformTimeTakenStat = 0L
    var totalDocsSizeInBytesStat = 0L
    var docsSizeOfBatchInBytes = 0L
    /* Contains list of docs source that are held in memory to submit to percolate query against query index.
            * Docs are fetched from the source index per shard and transformed.*/
    val transformedDocs = mutableListOf<Pair<String, TransformedDocDto>>()

    override suspend fun runMonitor(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryrun: Boolean
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
            monitor,
            createWithRunContext = false,
            skipIndex = isTempMonitor
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
            val allConcreteIndices = IndexUtils.resolveAllIndices(
                docLevelMonitorInput.indices,
                monitorCtx.clusterService!!,
                monitorCtx.indexNameExpressionResolver!!
            )
            if (allConcreteIndices.isEmpty()) {
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
            val runContextKeys = updatedLastRunContext.keys.toMutableSet()
            for (ind in runContextKeys) {
                if (!allConcreteIndices.contains(ind)) {
                    updatedLastRunContext.remove(ind)
                }
            }

            val concreteIndicesSeenSoFar = mutableListOf<String>()
            val updatedIndexNames = mutableListOf<String>()
            docLevelMonitorInput.indices.forEach { indexName ->
                var concreteIndices = IndexUtils.resolveAllIndices(
                    listOf(indexName),
                    monitorCtx.clusterService!!,
                    monitorCtx.indexNameExpressionResolver!!
                )
                var lastWriteIndex: String? = null
                if (IndexUtils.isAlias(indexName, monitorCtx.clusterService!!.state()) ||
                    IndexUtils.isDataStream(indexName, monitorCtx.clusterService!!.state())
                ) {
                    lastWriteIndex = concreteIndices.find { lastRunContext.containsKey(it) }
                    if (lastWriteIndex != null) {
                        val lastWriteIndexCreationDate =
                            IndexUtils.getCreationDateForIndex(lastWriteIndex, monitorCtx.clusterService!!.state())
                        concreteIndices = IndexUtils.getNewestIndicesByCreationDate(
                            concreteIndices,
                            monitorCtx.clusterService!!.state(),
                            lastWriteIndexCreationDate
                        )
                    }
                }
                concreteIndicesSeenSoFar.addAll(concreteIndices)
                val updatedIndexName = indexName.replace("*", "_")
                updatedIndexNames.add(updatedIndexName)
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
                        concreteIndexName,
                    ) as MutableMap<String, Any>
                    if (IndexUtils.isAlias(indexName, monitorCtx.clusterService!!.state()) ||
                        IndexUtils.isDataStream(indexName, monitorCtx.clusterService!!.state())
                    ) {
                        if (concreteIndexName == IndexUtils.getWriteIndex(indexName, monitorCtx.clusterService!!.state())) {
                            updatedLastRunContext.remove(lastWriteIndex)
                            updatedLastRunContext[concreteIndexName] = indexUpdatedRunContext
                        }
                    } else {
                        updatedLastRunContext[concreteIndexName] = indexUpdatedRunContext
                    }

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

                    fetchShardDataAndMaybeExecutePercolateQueries(
                        monitor,
                        monitorCtx,
                        docExecutionContext,
                        updatedIndexName,
                        concreteIndexName,
                        conflictingFields.toList(),
                        monitorMetadata,
                        inputRunResults,
                        docsToQueries,
                        updatedIndexNames,
                        concreteIndicesSeenSoFar,
                    )
                }
            }
            /* if all indices are covered still in-memory docs size limit is not breached we would need to submit
               the percolate query at the end */
            if (transformedDocs.isNotEmpty()) {
                performPercolateQueryAndResetCounters(
                    monitorCtx,
                    monitor,
                    monitorMetadata,
                    updatedIndexNames,
                    concreteIndicesSeenSoFar,
                    inputRunResults,
                    docsToQueries,
                )
            }
            monitorResult = monitorResult.copy(inputResults = InputRunResults(listOf(inputRunResults)))
        } catch (e: Exception) {
            logger.error("Failed to start Document-level-monitor ${monitor.name}", e)
            val alertingException = AlertingException(
                ExceptionsHelper.unwrapCause(e).cause?.message.toString(),
                RestStatus.INTERNAL_SERVER_ERROR,
                e
            )
            monitorResult = monitorResult.copy(error = alertingException, inputResults = InputRunResults(emptyList(), alertingException))
        } finally {
            logger.debug(
                "PERF_DEBUG_STATS: Monitor ${monitor.id} " +
                    "Time spent on fetching data from shards in millis: $nonPercolateSearchesTimeTakenStat"
            )
            logger.debug(
                "PERF_DEBUG_STATS: Monitor {} Time spent on percolate queries in millis: {}",
                monitor.id,
                percolateQueriesTimeTakenStat
            )
            logger.debug(
                "PERF_DEBUG_STATS: Monitor {} Time spent on transforming doc fields in millis: {}",
                monitor.id,
                docTransformTimeTakenStat
            )
            logger.debug("PERF_DEBUG_STATS: Monitor {} Num docs queried: {}", monitor.id, totalDocsQueriedStat)
        }

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
            MonitorMetadataService.upsertMetadata(
                monitorMetadata.copy(lastRunContext = updatedLastRunContext),
                true
            )
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

        val findings = mutableListOf<String>()
        val findingDocPairs = mutableListOf<Pair<String, String>>()

        // TODO: Implement throttling for findings
        docsToQueries.forEach {
            val triggeredQueries = it.value.map { queryId -> idQueryMap[queryId]!! }
            val findingId = createFindings(monitor, monitorCtx, triggeredQueries, it.key, !dryrun && monitor.id != Monitor.NO_ID)
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
                monitorResult.alertError() ?: triggerResult.alertError()
            )
            alerts.add(alert)
        }

        if (findingDocPairs.isEmpty() && monitorResult.error != null) {
            val alert = monitorCtx.alertService!!.composeDocLevelAlert(
                listOf(),
                listOf(),
                triggerCtx,
                monitorResult.alertError() ?: triggerResult.alertError()
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

            monitorCtx.retryPolicy?.let { monitorCtx.alertService!!.saveAlerts(monitor.dataSources, updatedAlerts, it) }
        }
        return triggerResult
    }

    private suspend fun createFindings(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        docLevelQueries: List<DocLevelQuery>,
        matchingDocId: String,
        shouldCreateFinding: Boolean
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
        return finding.id
    }

    private suspend fun updateLastRunContext(
        lastRunContext: Map<String, Any>,
        monitorCtx: MonitorRunnerExecutionContext,
        index: String,
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

    suspend fun createRunContext(
        clusterService: ClusterService,
        client: Client,
        index: String,
        createdRecently: Boolean = false
    ): HashMap<String, Any> {
        val lastRunContext = HashMap<String, Any>()
        lastRunContext["index"] = index
        val count = getShardsCount(clusterService, index)
        lastRunContext["shards_count"] = count

        for (i: Int in 0 until count) {
            val shard = i.toString()
            val maxSeqNo: Long = if (createdRecently) -1L else getMaxSeqNo(client, index, shard)
            lastRunContext[shard] = maxSeqNo
        }
        return lastRunContext
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
        nonPercolateSearchesTimeTakenStat += response.took.millis
        if (response.hits.hits.isEmpty())
            return -1L

        return response.hits.hits[0].seqNo
    }

    private fun getShardsCount(clusterService: ClusterService, index: String): Int {
        val allShards: List<ShardRouting> = clusterService!!.state().routingTable().allShards(index)
        return allShards.filter { it.primary() }.size
    }

    /** 1. Fetch data per shard for given index. (only 10000 docs are fetched.
     * needs to be converted to scroll if not performant enough)
     *  2. Transform documents to conform to format required for percolate query
     *  3a. Check if docs in memory are crossing threshold defined by setting.
     *  3b. If yes, perform percolate query and update docToQueries Map with all hits from percolate queries */
    private suspend fun fetchShardDataAndMaybeExecutePercolateQueries(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        docExecutionCtx: DocumentExecutionContext,
        indexName: String,
        concreteIndexName: String,
        conflictingFields: List<String>,
        monitorMetadata: MonitorMetadata,
        inputRunResults: MutableMap<String, MutableSet<String>>,
        docsToQueries: MutableMap<String, MutableList<String>>,
        monitorInputIndices: List<String>,
        concreteIndices: List<String>,
    ) {
        val count: Int = docExecutionCtx.updatedLastRunContext["shards_count"] as Int
        for (i: Int in 0 until count) {
            val shard = i.toString()
            try {
                val maxSeqNo: Long = docExecutionCtx.updatedLastRunContext[shard].toString().toLong()
                val prevSeqNo = docExecutionCtx.lastRunContext[shard].toString().toLongOrNull()

                val hits: SearchHits = searchShard(
                    monitorCtx,
                    concreteIndexName,
                    shard,
                    prevSeqNo,
                    maxSeqNo,
                    null
                )
                val startTime = System.currentTimeMillis()
                transformedDocs.addAll(
                    transformSearchHitsAndReconstructDocs(
                        hits,
                        indexName,
                        concreteIndexName,
                        monitor.id,
                        conflictingFields,
                    )
                )
                docTransformTimeTakenStat += System.currentTimeMillis() - startTime
            } catch (e: Exception) {
                logger.error(
                    "Monitor ${monitor.id} :" +
                        " Failed to run fetch data from shard [$shard] of index [$concreteIndexName]. Error: ${e.message}",
                    e
                )
            }
            if (
                transformedDocs.isNotEmpty() &&
                shouldPerformPercolateQueryAndFlushInMemoryDocs(transformedDocs.size, monitorCtx)
            ) {
                performPercolateQueryAndResetCounters(
                    monitorCtx,
                    monitor,
                    monitorMetadata,
                    monitorInputIndices,
                    concreteIndices,
                    inputRunResults,
                    docsToQueries,
                )
            }
        }
    }

    private fun shouldPerformPercolateQueryAndFlushInMemoryDocs(
        numDocs: Int,
        monitorCtx: MonitorRunnerExecutionContext,
    ): Boolean {
        return isInMemoryDocsSizeExceedingMemoryLimit(docsSizeOfBatchInBytes, monitorCtx) ||
            isInMemoryNumDocsExceedingMaxDocsPerPercolateQueryLimit(numDocs, monitorCtx)
    }

    private suspend fun performPercolateQueryAndResetCounters(
        monitorCtx: MonitorRunnerExecutionContext,
        monitor: Monitor,
        monitorMetadata: MonitorMetadata,
        monitorInputIndices: List<String>,
        concreteIndices: List<String>,
        inputRunResults: MutableMap<String, MutableSet<String>>,
        docsToQueries: MutableMap<String, MutableList<String>>,
    ) {
        try {
            val percolateQueryResponseHits = runPercolateQueryOnTransformedDocs(
                monitorCtx,
                transformedDocs,
                monitor,
                monitorMetadata,
                concreteIndices,
                monitorInputIndices,
            )

            percolateQueryResponseHits.forEach { hit ->
                var id = hit.id
                concreteIndices.forEach { id = id.replace("_${it}_${monitor.id}", "") }
                monitorInputIndices.forEach { id = id.replace("_${it}_${monitor.id}", "") }
                val docIndices = hit.field("_percolator_document_slot").values.map { it.toString().toInt() }
                docIndices.forEach { idx ->
                    val docIndex = "${transformedDocs[idx].first}|${transformedDocs[idx].second.concreteIndexName}"
                    inputRunResults.getOrPut(id) { mutableSetOf() }.add(docIndex)
                    docsToQueries.getOrPut(docIndex) { mutableListOf() }.add(id)
                }
            }
            totalDocsQueriedStat += transformedDocs.size.toLong()
        } finally {
            transformedDocs.clear()
            docsSizeOfBatchInBytes = 0
        }
    }

    /** Executes search query on given shard of given index to fetch docs with sequene number greater than prevSeqNo.
     * This method hence fetches only docs from shard which haven't been queried before
     */
    private suspend fun searchShard(
        monitorCtx: MonitorRunnerExecutionContext,
        index: String,
        shard: String,
        prevSeqNo: Long?,
        maxSeqNo: Long,
        query: String?,
        docIds: List<String>? = null,
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
                    .size(10000)
            )
        val response: SearchResponse = monitorCtx.client!!.suspendUntil { monitorCtx.client!!.search(request, it) }
        if (response.status() !== RestStatus.OK) {
            logger.error("Failed search shard. Response: $response")
            throw IOException("Failed to search shard: [$shard] in index [$index]. Response status is ${response.status()}")
        }
        nonPercolateSearchesTimeTakenStat += response.took.millis
        return response.hits
    }

    /** Executes percolate query on the docs against the monitor's query index and return the hits from the search response*/
    private suspend fun runPercolateQueryOnTransformedDocs(
        monitorCtx: MonitorRunnerExecutionContext,
        docs: MutableList<Pair<String, TransformedDocDto>>,
        monitor: Monitor,
        monitorMetadata: MonitorMetadata,
        concreteIndices: List<String>,
        monitorInputIndices: List<String>,
    ): SearchHits {
        val indices = docs.stream().map { it.second.indexName }.distinct().collect(Collectors.toList())
        val boolQueryBuilder = BoolQueryBuilder().must(buildShouldClausesOverPerIndexMatchQueries(indices))
        val percolateQueryBuilder =
            PercolateQueryBuilderExt("query", docs.map { it.second.docSource }, XContentType.JSON)
        if (monitor.id.isNotEmpty()) {
            boolQueryBuilder.must(QueryBuilders.matchQuery("monitor_id", monitor.id).operator(Operator.AND))
        }
        boolQueryBuilder.filter(percolateQueryBuilder)
        val queryIndices =
            docs.map { monitorMetadata.sourceToQueryIndexMapping[it.second.indexName + monitor.id] }.distinct()
        if (queryIndices.isEmpty()) {
            val message =
                "Monitor ${monitor.id}: Failed to resolve query Indices from source indices during monitor execution!" +
                    " sourceIndices: $monitorInputIndices"
            logger.error(message)
            throw AlertingException.wrap(
                OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR)
            )
        }

        val searchRequest =
            SearchRequest().indices(*queryIndices.toTypedArray())
        val searchSourceBuilder = SearchSourceBuilder()
        searchSourceBuilder.query(boolQueryBuilder)
        searchRequest.source(searchSourceBuilder)
        logger.debug(
            "Monitor ${monitor.id}: " +
                "Executing percolate query for docs from source indices " +
                "$monitorInputIndices against query index $queryIndices"
        )
        var response: SearchResponse
        try {
            response = monitorCtx.client!!.suspendUntil {
                monitorCtx.client!!.execute(SearchAction.INSTANCE, searchRequest, it)
            }
        } catch (e: Exception) {
            throw IllegalStateException(
                "Monitor ${monitor.id}:" +
                    " Failed to run percolate search for sourceIndex [${concreteIndices.joinToString()}] " +
                    "and queryIndex [${queryIndices.joinToString()}] for ${docs.size} document(s)",
                e
            )
        }

        if (response.status() !== RestStatus.OK) {
            throw IOException(
                "Monitor ${monitor.id}: Failed to search percolate index: ${queryIndices.joinToString()}. " +
                    "Response status is ${response.status()}"
            )
        }
        logger.debug("Monitor ${monitor.id} PERF_DEBUG: Percolate query time taken millis = ${response.took}")
        percolateQueriesTimeTakenStat += response.took.millis
        return response.hits
    }
    /** we cannot use terms query because `index` field's mapping is of type TEXT and not keyword. Refer doc-level-queries.json*/
    private fun buildShouldClausesOverPerIndexMatchQueries(indices: List<String>): BoolQueryBuilder {
        val boolQueryBuilder = QueryBuilders.boolQuery()
        indices.forEach { boolQueryBuilder.should(QueryBuilders.matchQuery("index", it)) }
        return boolQueryBuilder
    }

    /** Transform field names and index names in all the search hits to format required to run percolate search against them.
     * Hits are transformed using method transformDocumentFieldNames() */
    private fun transformSearchHitsAndReconstructDocs(
        hits: SearchHits,
        index: String,
        concreteIndex: String,
        monitorId: String,
        conflictingFields: List<String>,
    ): List<Pair<String, TransformedDocDto>> {
        return hits.mapNotNull(fun(hit: SearchHit): Pair<String, TransformedDocDto>? {
            try {
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
                docsSizeOfBatchInBytes += sourceRef.ramBytesUsed()
                totalDocsSizeInBytesStat += sourceRef.ramBytesUsed()
                return Pair(hit.id, TransformedDocDto(index, concreteIndex, hit.id, sourceRef))
            } catch (e: Exception) {
                logger.error("Monitor $monitorId: Failed to transform payload $hit for percolate query", e)
                // skip any document which we fail to transform because we anyway won't be able to run percolate queries on them.
                return null
            }
        })
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

    /**
     * Returns true, if the docs fetched from shards thus far amount to less than threshold
     * amount of percentage (default:10. setting is dynamic and configurable) of the total heap size or not.
     *
     */
    private fun isInMemoryDocsSizeExceedingMemoryLimit(docsBytesSize: Long, monitorCtx: MonitorRunnerExecutionContext): Boolean {
        var thresholdPercentage = PERCOLATE_QUERY_DOCS_SIZE_MEMORY_PERCENTAGE_LIMIT.get(monitorCtx.settings)
        val heapMaxBytes = monitorCtx.jvmStats!!.mem.heapMax.bytes
        val thresholdBytes = (thresholdPercentage.toDouble() / 100.0) * heapMaxBytes

        return docsBytesSize > thresholdBytes
    }

    private fun isInMemoryNumDocsExceedingMaxDocsPerPercolateQueryLimit(numDocs: Int, monitorCtx: MonitorRunnerExecutionContext): Boolean {
        var maxNumDocsThreshold = PERCOLATE_QUERY_MAX_NUM_DOCS_IN_MEMORY.get(monitorCtx.settings)
        return numDocs >= maxNumDocsThreshold
    }

    /**
     * POJO holding information about each doc's concrete index, id, input index pattern/alias/datastream name
     * and doc source. A list of these POJOs would be passed to percolate query execution logic.
     */
    data class TransformedDocDto(
        var indexName: String,
        var concreteIndexName: String,
        var docId: String,
        var docSource: BytesReference
    )
}
