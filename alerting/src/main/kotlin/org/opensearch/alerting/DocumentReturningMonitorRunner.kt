package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.elasticapi.string
import org.opensearch.alerting.model.ActionRunResult
import org.opensearch.alerting.model.DocumentExecutionContext
import org.opensearch.alerting.model.DocumentLevelTrigger
import org.opensearch.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.alerting.model.Finding
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.action.Action
import org.opensearch.alerting.model.docLevelInput.DocLevelMonitorInput
import org.opensearch.alerting.model.docLevelInput.DocLevelQuery
import org.opensearch.alerting.script.DocumentLevelTriggerExecutionContext
import org.opensearch.alerting.script.TriggerExecutionContext
import org.opensearch.alerting.util.updateMonitor
import org.opensearch.cluster.routing.ShardRouting
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

object DocumentReturningMonitorRunner : MonitorRunner {
    private val logger = LogManager.getLogger(javaClass)

    override suspend fun runMonitor(monitor: Monitor, monitorCtx: MonitorRunnerExecutionContext, periodStart: Instant, periodEnd: Instant, dryrun: Boolean):
        MonitorRunResult<DocumentLevelTriggerRunResult> {
        logger.info("Document-level-monitor is running ...")
        val monitorResult = MonitorRunResult<DocumentLevelTriggerRunResult>(monitor.name, periodStart, periodEnd)
        try {
            validate(monitor)
        } catch (e: Exception) {
            logger.info("Failed to start Document-level-monitor. Error: ${e.message}")
            return monitorResult.copy(error = e)
        }

        val docLevelMonitorInput = monitor.inputs[0] as DocLevelMonitorInput
        val queries: List<DocLevelQuery> = docLevelMonitorInput.queries

        // Retrieving the first index here as doc level monitors do not currently support multiple indices aside from aliases
        val index = docLevelMonitorInput.indices[0]

        val lastRunContext = if (monitor.lastRunContext.isNullOrEmpty()) mutableMapOf()
        else monitor.lastRunContext.toMutableMap() as MutableMap<String, MutableMap<String, Any>>

        val updatedLastRunContext = lastRunContext.toMutableMap()

        val queryToDocIds = mutableMapOf<DocLevelQuery, Set<String>>()
        val docsToQueries = mutableMapOf<String, MutableList<String>>()

        try {
            val getAliasesRequest = GetAliasesRequest(index)
            val getAliasesResponse = monitorCtx.client!!.admin().indices().getAliases(getAliasesRequest).actionGet()
            val aliasIndices = getAliasesResponse.aliases.keys().map { it.value }

            // TODO: Add log message if index is/is not alias?
            val isAlias = aliasIndices.isNotEmpty()

            // If the input index is an alias, creating a list of all indices associated with that alias;
            // else creating a list containing the single index input
            val indices = if (isAlias) getAliasesResponse.aliases.keys().map { it.value } else listOf(index)

            indices.forEach { indexName ->
                // Prepare lastRunContext for each index
                val indexLastRunContext = lastRunContext.getOrPut(indexName) { createRunContext(monitorCtx, indexName) }

                // Prepare updatedLastRunContext for each index
                val indexUpdatedRunContext = updateLastRunContext(
                    indexLastRunContext.toMutableMap(),
                    monitorCtx,
                    indexName
                ) as MutableMap<String, Any>
                updatedLastRunContext[indexName] = indexUpdatedRunContext

                // Prepare DocumentExecutionContext for each index
                val docExecutionContext = DocumentExecutionContext(queries, indexLastRunContext, indexUpdatedRunContext)

                val queryResults = getDocLevelQueryResults(
                    monitorCtx,
                    docExecutionContext,
                    indexName,
                    queries
                )

                // Only need to compile the query results together when executing the monitor for more than 1 index
                if (indices.size > 1) {
                    val newQueryToDocIds = mutableMapOf<DocLevelQuery, Set<String>>()
                    queryResults["queryDocIds"]?.forEach {
                        val docLevelQuery = it.key
                        val matchingDocIds = it.value
                        val existingQueryToDocIds = queryToDocIds.getOrDefault(docLevelQuery, setOf()).toMutableSet()
                        existingQueryToDocIds.addAll(matchingDocIds)
                        newQueryToDocIds[it.key as DocLevelQuery] = existingQueryToDocIds
                    }
                    queryToDocIds.putAll(newQueryToDocIds)

                    val newDocsToQueries = mutableMapOf<String, MutableList<String>>()
                    queryResults["docsToQueries"]?.forEach {
                        val docId = it.key
                        val queryIds = it.value
                        val existingQueryIds = docsToQueries.getOrDefault(docId, mutableListOf())
                        existingQueryIds.addAll(queryIds)
                        newDocsToQueries[docId as String] = existingQueryIds
                    }
                    docsToQueries.putAll(newDocsToQueries)
                } else {
                    queryToDocIds.putAll(queryResults["queryDocIds"] as MutableMap<DocLevelQuery, Set<String>>)
                    docsToQueries.putAll(queryResults["docsToQueries"] as MutableMap<String, MutableList<String>>)
                }
            }
        } catch (e: Exception) {
            logger.info("Failed to start Document-level-monitor $index. Error: ${e.message}")
        }

        val queryIds = queries.map { it.id }

        monitor.triggers.forEach {
            runForEachDocTrigger(monitorCtx, it as DocumentLevelTrigger, monitor, docsToQueries, queryIds, queryToDocIds, dryrun)
        }

        // Don't save alerts if this is a test monitor
        if (!dryrun && monitor.id != Monitor.NO_ID) {
            // TODO: Check for race condition against the update monitor api
            // This does the update at the end in case of errors and makes sure all the queries are executed
            val updatedMonitor = monitor.copy(lastRunContext = updatedLastRunContext)
            // note: update has to called in serial for shards of a given index.
            // make sure this is just updated for the specific query or at the end of all the queries
            updateMonitor(monitorCtx.client!!, monitorCtx.xContentRegistry!!, monitorCtx.settings!!, updatedMonitor)
        }

        // TODO: Update the Document as part of the Trigger and return back the trigger action result
        val triggerResults = mutableMapOf<String, DocumentLevelTriggerRunResult>()
        return monitorResult.copy(triggerResults = triggerResults)
    }

    private fun runForEachDocTrigger(
        monitorCtx: MonitorRunnerExecutionContext,
        trigger: DocumentLevelTrigger,
        monitor: Monitor,
        docsToQueries: Map<String, List<String>>,
        queryIds: List<String>,
        queryToDocIds: Map<DocLevelQuery, Set<String>>,
        dryrun: Boolean
    ) {
        val triggerCtx = DocumentLevelTriggerExecutionContext(monitor, trigger)
        val triggerResult = monitorCtx.triggerService!!.runDocLevelTrigger(monitor, trigger, triggerCtx, docsToQueries, queryIds)

        logger.info("trigger results")
        logger.info(triggerResult.triggeredDocs.toString())

        val index = (monitor.inputs[0] as DocLevelMonitorInput).indices[0]

        queryToDocIds.forEach {
            val queryTriggeredDocs = it.value.intersect(triggerResult.triggeredDocs)
            // TODO: Update finding only if it is not dry run, else return the findings
            if (queryTriggeredDocs.isNotEmpty() && !dryrun && monitor.id != Monitor.NO_ID) {
                val findingId = createFindings(monitor, monitorCtx, index, it.key, queryTriggeredDocs, trigger)
                // TODO: check if need to create alert, if so create it and point it to FindingId
                // TODO: run action as well, but this mat need to be throttled based on Mo's comment for bucket level alerting
            }
        }
    }

    private fun updateLastRunContext(
        lastRunContext: Map<String, Any>,
        monitorCtx: MonitorRunnerExecutionContext,
        index: String
    ): Map<String, Any> {
        // TODO DRAFT: Is it better to get shards_count by calling getShardsCount
        //  as opposed to retrieving the count from lastRunContext? The number of shards
        //  could changing between monitor executions.
//        val count: Int = lastRunContext["shards_count"] as Int
        val count: Int = getShardsCount(monitorCtx, index)
        val updatedLastRunContext = lastRunContext.toMutableMap()
        for (i: Int in 0 until count) {
            val shard = i.toString()
            val maxSeqNo: Long = getMaxSeqNo(monitorCtx, index, shard)
            updatedLastRunContext[shard] = maxSeqNo.toString()
        }
        return updatedLastRunContext
    }

    private fun getDocLevelQueryResults(
        monitorCtx: MonitorRunnerExecutionContext,
        documentExecutionContext: DocumentExecutionContext,
        index: String,
        queries: List<DocLevelQuery>
    ): MutableMap<String, MutableMap<out Any, out Collection<String>>> {
        val queryDocIds = mutableMapOf<DocLevelQuery, Set<String>>()
        val docsToQueries = mutableMapOf<String, MutableList<String>>()
        for (query in queries) {
            val matchingDocIds = runForEachQuery(monitorCtx, documentExecutionContext, query, index)
            queryDocIds[query] = matchingDocIds
            matchingDocIds.forEach {
                docsToQueries.putIfAbsent(it, mutableListOf())
                docsToQueries[it]?.add(query.id)
            }
        }
        return mutableMapOf(
            ("queryDocIds" to queryDocIds),
            ("docsToQueries" to docsToQueries)
        )
    }

    fun createFindings(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        index: String,
        docLevelQuery: DocLevelQuery,
        matchingDocIds: Set<String>,
        trigger: DocumentLevelTrigger
    ): String {
        val finding = Finding(
            id = UUID.randomUUID().toString(),
            relatedDocId = matchingDocIds.joinToString(","),
            monitorId = monitor.id,
            monitorName = monitor.name,
            index = index,
            queryId = docLevelQuery.id,
            queryTags = docLevelQuery.tags,
            severity = docLevelQuery.severity,
            timestamp = Instant.now(),
            triggerId = trigger.id,
            triggerName = trigger.name
        )

        val findingStr = finding.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS).string()
        // change this to debug.
        logger.info("Findings: $findingStr")

        // todo: below is all hardcoded, temp code and added only to test. replace this with proper Findings index lifecycle management.
        val indexRequest = IndexRequest(".opensearch-alerting-findings")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source(findingStr, XContentType.JSON)

        monitorCtx.client!!.index(indexRequest).actionGet()
        return finding.id
    }

    // TODO: Implement action for triggers
    override suspend fun runAction(action: Action, ctx: TriggerExecutionContext, monitorCtx: MonitorRunnerExecutionContext, dryrun: Boolean): ActionRunResult {
        return ActionRunResult(action.id, action.name, mapOf(), false, MonitorRunnerService.currentTime(), null)
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

    private fun createRunContext(monitorCtx: MonitorRunnerExecutionContext, index: String): HashMap<String, Any> {
        val lastRunContext = HashMap<String, Any>()
        val count = getShardsCount(monitorCtx, index)
        lastRunContext["shards_count"] = count

        for (i: Int in 0 until count) {
            val shard = i.toString()
            val maxSeqNo: Long = getMaxSeqNo(monitorCtx, index, shard)
            lastRunContext[shard] = maxSeqNo
        }
        return lastRunContext
    }

    /**
     * Get the current max seq number of the shard. We find it by searching the last document
     *  in the primary shard.
     */
    private fun getMaxSeqNo(monitorCtx: MonitorRunnerExecutionContext, index: String, shard: String): Long {
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
        val response: SearchResponse = monitorCtx.client!!.search(request).actionGet()
        if (response.status() !== RestStatus.OK) {
            throw IOException("Failed to get max seq no for shard: $shard")
        }
        if (response.hits.hits.isEmpty())
            return -1L

        return response.hits.hits[0].seqNo
    }

    private fun getShardsCount(monitorCtx: MonitorRunnerExecutionContext, index: String): Int {
        val allShards: List<ShardRouting> = monitorCtx.clusterService!!.state().routingTable().allShards(index)
        return allShards.filter { it.primary() }.size
    }

    private fun runForEachQuery(
        monitorCtx: MonitorRunnerExecutionContext,
        docExecutionCtx: DocumentExecutionContext,
        query: DocLevelQuery,
        index: String
    ): Set<String> {
        // TODO DRAFT: Is it better to get shards_count from updatedLastRunContext as opposed to lastRunContext?
        //  The number of shards could changing between monitor executions
//        val count: Int = docExecutionCtx.lastRunContext["shards_count"] as Int
        val count: Int = docExecutionCtx.updatedLastRunContext["shards_count"] as Int
        val matchingDocs = mutableSetOf<String>()
        for (i: Int in 0 until count) {
            val shard = i.toString()
            try {
                logger.info("Monitor execution for shard: $shard")

                val maxSeqNo: Long = docExecutionCtx.updatedLastRunContext[shard].toString().toLong()
                logger.info("MaxSeqNo of shard_$shard is $maxSeqNo")

                val hits: SearchHits = searchShard(
                    monitorCtx,
                    index,
                    shard,
                    docExecutionCtx.lastRunContext[shard].toString().toLongOrNull(),
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

    private fun searchShard(monitorCtx: MonitorRunnerExecutionContext, index: String, shard: String, prevSeqNo: Long?, maxSeqNo: Long, query: String): SearchHits {
        if (prevSeqNo?.equals(maxSeqNo) == true) {
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
