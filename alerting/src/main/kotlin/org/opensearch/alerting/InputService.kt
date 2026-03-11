/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.apache.logging.log4j.LogManager
import org.json.JSONObject
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.PPLUtils.appendDataRowsLimit
import org.opensearch.alerting.PPLUtils.capAndReformatPPLQueryResults
import org.opensearch.alerting.PPLUtils.executePplQuery
import org.opensearch.alerting.opensearchapi.convertToMap
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AggregationQueryRewriter
import org.opensearch.alerting.util.BucketSelectorQueryBuilder
import org.opensearch.alerting.util.CrossClusterMonitorUtils
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.addUserBackendRolesFilter
import org.opensearch.alerting.util.clusterMetricsMonitorHelpers.executeTransportAction
import org.opensearch.alerting.util.clusterMetricsMonitorHelpers.toMap
import org.opensearch.alerting.util.getCancelAfterTimeInterval
import org.opensearch.alerting.util.getRoleFilterEnabled
import org.opensearch.alerting.util.use
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.routing.Preference
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.BucketLevelTrigger
import org.opensearch.commons.alerting.model.ClusterMetricsInput
import org.opensearch.commons.alerting.model.InputRunResults
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.PPLInput
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.alerting.model.TriggerAfterKey
import org.opensearch.commons.alerting.model.WorkflowRunContext
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.MatchQueryBuilder
import org.opensearch.index.query.QueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.query.RangeQueryBuilder
import org.opensearch.index.query.TermsQueryBuilder
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.ScriptType
import org.opensearch.script.TemplateScript
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.node.NodeClient
import java.time.Duration
import java.time.Instant
import kotlin.time.measureTimedValue

/** Service that handles the collection of input results for Monitor executions */
class InputService(
    val client: Client,
    val scriptService: ScriptService,
    val namedWriteableRegistry: NamedWriteableRegistry,
    val xContentRegistry: NamedXContentRegistry,
    val clusterService: ClusterService,
    val settings: Settings,
    val indexNameExpressionResolver: IndexNameExpressionResolver
) {

    private val logger = LogManager.getLogger(InputService::class.java)

    suspend fun collectInputResults(
        monitor: Monitor,
        periodStart: Instant,
        periodEnd: Instant,
        prevResult: InputRunResults? = null,
        workflowRunContext: WorkflowRunContext? = null,
        useStandardBucketSelector: Boolean = false
    ): InputRunResults {
        return try {
            val results = mutableListOf<Map<String, Any>>()
            val aggTriggerAfterKey: MutableMap<String, TriggerAfterKey> = mutableMapOf()

            // If monitor execution is triggered from a workflow
            val matchingDocIdsPerIndex = workflowRunContext?.matchingDocIdsPerIndex

            // TODO: If/when multiple input queries are supported for Bucket-Level Monitor execution, aggTriggerAfterKeys will
            //  need to be updated to account for it
            monitor.inputs.forEach { input ->
                when (input) {
                    is SearchInput -> {
                        val searchRequest = getSearchRequest(
                            monitor = monitor,
                            searchInput = input,
                            periodStart = periodStart,
                            periodEnd = periodEnd,
                            prevResult = prevResult,
                            matchingDocIdsPerIndex = matchingDocIdsPerIndex,
                            returnSampleDocs = false,
                            useStandardBucketSelector = useStandardBucketSelector
                        )
                        val searchResponse: SearchResponse = client.suspendUntil { client.search(searchRequest, it) }
                        aggTriggerAfterKey += AggregationQueryRewriter.getAfterKeysFromSearchResponse(
                            searchResponse,
                            monitor.triggers,
                            prevResult?.aggTriggersAfterKey
                        )
                        results += searchResponse.convertToMap()
                    }
                    is ClusterMetricsInput -> {
                        results += handleClusterMetricsInput(input)
                    }
                    else -> {
                        throw IllegalArgumentException("Unsupported input type: ${input.name()}.")
                    }
                }
            }
            InputRunResults(results.toList(), aggTriggersAfterKey = aggTriggerAfterKey)
        } catch (e: Exception) {
            logger.info("Error collecting inputs for monitor: ${monitor.id}", e)
            InputRunResults(emptyList(), e)
        }
    }

    /**
     * Extends the given query builder with query that filters the given indices with the given doc ids per index
     * Used whenever we want to select the documents that were found in chained delegate execution of the current workflow run
     *
     * @param query Original bucket monitor query
     * @param matchingDocIdsPerIndex Map of finding doc ids grouped by index
     */
    private fun updateInputQueryWithFindingDocIds(
        query: QueryBuilder,
        matchingDocIdsPerIndex: Map<String, List<String>>,
    ): QueryBuilder {
        val queryBuilder = QueryBuilders.boolQuery().must(query)
        val shouldQuery = QueryBuilders.boolQuery()

        matchingDocIdsPerIndex.forEach { entry ->
            shouldQuery
                .should()
                .add(
                    BoolQueryBuilder()
                        .must(MatchQueryBuilder("_index", entry.key))
                        .must(TermsQueryBuilder("_id", entry.value))
                )
        }
        return queryBuilder.must(shouldQuery)
    }

    private fun chainedFindingExist(indexToDocIds: Map<String, List<String>>?) =
        !indexToDocIds.isNullOrEmpty()

    private fun deepCopyQuery(query: SearchSourceBuilder): SearchSourceBuilder {
        val out = BytesStreamOutput()
        query.writeTo(out)
        val sin = NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        return SearchSourceBuilder(sin)
    }

    /**
     * We moved anomaly result index to system index list. So common user could not directly query
     * this index any more. This method will stash current thread context to pass security check.
     * So monitor job can access anomaly result index. We will add monitor user roles filter in
     * search query to only return documents the monitor user can access.
     *
     * On alerting Kibana, monitor users can only see detectors that they have read access. So they
     * can't create monitor on other user's detector which they have no read access. Even they know
     * other user's detector id and use it to create monitor, this method will only return anomaly
     * results they can read.
     */
    suspend fun collectInputResultsForADMonitor(monitor: Monitor, periodStart: Instant, periodEnd: Instant): InputRunResults {
        return try {
            val results = mutableListOf<Map<String, Any>>()
            val input = monitor.inputs[0] as SearchInput

            val searchParams = mapOf("period_start" to periodStart.toEpochMilli(), "period_end" to periodEnd.toEpochMilli())
            val searchSource = scriptService.compile(
                Script(
                    ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG,
                    input.query.toString(), searchParams
                ),
                TemplateScript.CONTEXT
            )
                .newInstance(searchParams)
                .execute()

            val searchRequest = SearchRequest()
                .indices(*input.indices.toTypedArray())
                .preference(Preference.PRIMARY_FIRST.type())
            XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, searchSource).use {
                searchRequest.source(SearchSourceBuilder.fromXContent(it))
            }

            val cancelTimeout = getCancelAfterTimeInterval()
            if (cancelTimeout != -1L) {
                searchRequest.cancelAfterTimeInterval = TimeValue.timeValueMinutes(cancelTimeout)
            }

            // Add user role filter for AD result
            client.threadPool().threadContext.stashContext().use {
                // Possible long term solution:
                // 1.Use secure rest client to send request to AD search result API. If no permission exception,
                // that mean user has read access on AD result. Then don't need to add user role filter when query
                // AD result if AD backend role filter is disabled.
                // 2.Security provide some transport action to verify if user has permission to search AD result.
                // Monitor runner will send transport request to check permission first. If security plugin response
                // is yes, user has permission to query AD result. If AD role filter enabled, we will add user role
                // filter to protect data at user role level; otherwise, user can query any AD result.
                if (getRoleFilterEnabled(clusterService, settings, "plugins.anomaly_detection.filter_by_backend_roles")) {
                    addUserBackendRolesFilter(monitor.user, searchRequest.source())
                }
                val searchResponse: SearchResponse = client.suspendUntil { client.search(searchRequest, it) }
                results += searchResponse.convertToMap()
            }
            InputRunResults(results.toList())
        } catch (e: Exception) {
            logger.info("Error collecting anomaly result inputs for monitor: ${monitor.id}", e)
            InputRunResults(emptyList(), e)
        }
    }

    suspend fun collectInputResultsForPPLMonitor(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        transportService: TransportService
    ): InputRunResults {
        return try {
            // PPL Alerting:
            // these query results are for number_of_results PPL triggers,
            // only the number of results returned by base query matters
            // for those triggers, not the contents themselves
            val basePplQueryResults = runPPLBaseQuery(
                monitor,
                (monitor.inputs[0] as PPLInput).query,
                monitorCtx,
                transportService
            )
            val numPplResults = basePplQueryResults.getLong("total")

            // PPL Alerting:
            // PPL Trigger evaluations won't read this input result in.
            // for num results triggers, this is because the contents of the query results
            // are unimportant, only the number of results matters.
            // for custom trigger, this is because it'll be running its own query
            // (base query + custom condition) and evaluating on those query results.
            // thus, the size capped and reformatted base query results are included
            // here to populate the final customer facing response of monitor execution
            val cappedPPLBaseQueryResults = capAndReformatPPLQueryResults(
                basePplQueryResults,
                monitorCtx.clusterService!!.clusterSettings.get(AlertingSettings.PPL_QUERY_RESULTS_MAX_SIZE)
            )

            InputRunResults(emptyList(), null, null, cappedPPLBaseQueryResults, numPplResults)
        } catch (e: Exception) {
            logger.error(
                "failed to run PPL Monitor base query " +
                    "from PPL Monitor ${monitor.name} (id: ${monitor.id}",
                e
            )
            InputRunResults(emptyList(), e, null, listOf(), null)
        }
    }

    // for PPL Monitor execution, the base PPL query is run once
    // for number_of_results PPL triggers
    private suspend fun runPPLBaseQuery(
        pplMonitor: Monitor,
        baseQuery: String,
        monitorCtx: MonitorRunnerExecutionContext,
        transportService: TransportService
    ): JSONObject {

        // TODO: change name to trigger max duration
        val monitorExecutionDuration = monitorCtx
            .clusterService!!
            .clusterSettings
            .get(AlertingSettings.PPL_MONITOR_EXECUTION_MAX_DURATION)

        var queryResponseJson: JSONObject? = null

        withTimeout(monitorExecutionDuration.millis) {
            // limit the number of PPL query result data rows returned
            val dataRowsLimit = monitorCtx.clusterService!!.clusterSettings.get(AlertingSettings.PPL_QUERY_RESULTS_MAX_DATAROWS)
            val limitedQueryToExecute = appendDataRowsLimit(baseQuery, dataRowsLimit)

            logger.debug("executing the base PPL query of monitor: ${pplMonitor.id}")
            val (queryResponseJsonReceived, timeTaken) = measureTimedValue {
                executePplQuery(
                    limitedQueryToExecute,
                    false,
                    monitorCtx.client!! as NodeClient
                )
            }
            logger.debug("base query results: $queryResponseJsonReceived")
            logger.debug("time taken to execute base query against sql/ppl plugin: $timeTaken")

            queryResponseJson = queryResponseJsonReceived
        }

        return queryResponseJson!!
    }

    fun getSearchRequest(
        monitor: Monitor,
        searchInput: SearchInput,
        periodStart: Instant,
        periodEnd: Instant,
        prevResult: InputRunResults?,
        matchingDocIdsPerIndex: Map<String, List<String>>?,
        returnSampleDocs: Boolean = false,
        useStandardBucketSelector: Boolean = false
    ): SearchRequest {
        // TODO: Figure out a way to use SearchTemplateRequest without bringing in the entire TransportClient
        val searchParams = mapOf(
            "period_start" to periodStart.toEpochMilli(),
            "period_end" to periodEnd.toEpochMilli()
        )

        // Deep copying query before passing it to rewriteQuery since otherwise, the monitor.input is modified directly
        // which causes a strange bug where the rewritten query persists on the Monitor across executions
        val copiedQuery = deepCopyQuery(searchInput.query)

        // When using standard bucket_selector, inject it as a sub-agg instead of BucketSelectorExt.
        if (useStandardBucketSelector) {
            val bucketTriggers = monitor.triggers.filterIsInstance<BucketLevelTrigger>()
            if (bucketTriggers.isNotEmpty()) {
                BucketSelectorQueryBuilder.injectBucketSelector(copiedQuery, bucketTriggers)
            }
        }

        val rewrittenQuery = AggregationQueryRewriter.rewriteQuery(
            copiedQuery,
            prevResult,
            monitor.triggers,
            returnSampleDocs,
            skipBucketSelectorInjection = useStandardBucketSelector
        )

        // Rewrite query to consider the doc ids per given index
        if (chainedFindingExist(matchingDocIdsPerIndex) && rewrittenQuery.query() != null) {
            val updatedSourceQuery = updateInputQueryWithFindingDocIds(rewrittenQuery.query(), matchingDocIdsPerIndex!!)
            rewrittenQuery.query(updatedSourceQuery)
        }

        val searchSource = scriptService.compile(
            Script(
                ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG,
                rewrittenQuery.toString(), searchParams
            ),
            TemplateScript.CONTEXT
        )
            .newInstance(searchParams)
            .execute()

        val indexes = CrossClusterMonitorUtils.parseIndexesForRemoteSearch(searchInput.indices, clusterService)

        val resolvedIndexes = if (searchInput.query.query() == null) indexes else {
            val query = searchInput.query.query()
            resolveOnlyQueryableIndicesFromLocalClusterAliases(
                monitor,
                periodEnd,
                query,
                indexes
            )
        }

        val searchRequest = SearchRequest()
            .indices(*resolvedIndexes.toTypedArray())
            .preference(Preference.PRIMARY_FIRST.type())

        XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, searchSource).use {
            searchRequest.source(SearchSourceBuilder.fromXContent(it))
        }

        val cancelTimeout = getCancelAfterTimeInterval()
        if (cancelTimeout != -1L) {
            searchRequest.cancelAfterTimeInterval = TimeValue.timeValueMinutes(cancelTimeout)
        }

        return searchRequest
    }

    /**
     * Resolves concrete indices from aliases based on a time range query and availability in the local cluster.
     *
     * <p>If an index passed to OpenSearch is an alias, this method will only select those indices
     * resolved from the alias that meet the following criteria:
     *
     * <ol>
     *     <li>The index's creation date falls within the time range specified in the query's timestamp field.</li>
     *     <li>The index immediately preceding the time range in terms of creation date is also included.</li>
     * </ol>
     *
     * <p>This ensures that queries targeting aliases consider relevant indices based on their creation time,
     * including the one immediately before the specified range to account for potential data at the boundary.
     */
    private fun resolveOnlyQueryableIndicesFromLocalClusterAliases(
        monitor: Monitor,
        periodEnd: Instant,
        query: QueryBuilder,
        indexes: List<String>,
    ): List<String> {
        val resolvedIndexes = ArrayList<String>()
        indexes.forEach {
            // we don't optimize for remote cluster aliases. we directly pass them to search request
            if (CrossClusterMonitorUtils.isRemoteClusterIndex(it, clusterService))
                resolvedIndexes.add(it)
            else {
                val state = clusterService.state()
                if (IndexUtils.isAlias(it, state)) {
                    val resolveStartTimeOfQueryTimeRange = resolveStartTimeofQueryTimeRange(monitor, query, periodEnd)
                    if (resolveStartTimeOfQueryTimeRange != null) {
                        val indices = IndexUtils.resolveAllIndices(listOf(it), clusterService, indexNameExpressionResolver)
                        val sortedIndices = indices
                            .mapNotNull { state.metadata().index(it) } // Get IndexMetadata for each index
                            .sortedBy { it.creationDate } // Sort by creation date

                        var includePrevious = true
                        for (i in sortedIndices.indices) {
                            val indexMetadata = sortedIndices[i]
                            val creationDate = indexMetadata.creationDate

                            if (creationDate >= resolveStartTimeOfQueryTimeRange.toEpochMilli()) {
                                resolvedIndexes.add(indexMetadata.index.name)
                                includePrevious = false // No need to include previous anymore
                            } else if (
                                includePrevious && (
                                    i == sortedIndices.lastIndex ||
                                        sortedIndices[i + 1].creationDate >= resolveStartTimeOfQueryTimeRange.toEpochMilli()
                                    )
                            ) {
                                // Include the index immediately before the timestamp
                                resolvedIndexes.add(indexMetadata.index.name)
                                includePrevious = false
                            }
                        }
                    } else {
                        // add alias without optimizing for resolve indices
                        resolvedIndexes.add(it)
                    }
                } else {
                    resolvedIndexes.add(it)
                }
            }
        }
        return resolvedIndexes
    }

    private suspend fun handleClusterMetricsInput(input: ClusterMetricsInput): MutableList<Map<String, Any>> {
        logger.debug("ClusterMetricsInput clusterMetricType: {}", input.clusterMetricType)

        val remoteMonitoringEnabled = clusterService.clusterSettings.get(AlertingSettings.CROSS_CLUSTER_MONITORING_ENABLED)
        logger.debug("Remote monitoring enabled: {}", remoteMonitoringEnabled)

        val results = mutableListOf<Map<String, Any>>()
        val responseMap = mutableMapOf<String, Map<String, Any>>()
        if (remoteMonitoringEnabled && input.clusters.isNotEmpty()) {
            // If remote monitoring is enabled, and the monitor is configured to execute against remote clusters,
            // execute the API against each cluster, and compile the results.
            client.threadPool().threadContext.stashContext().use {
                val singleThreadContext = newSingleThreadContext("ClusterMetricsMonitorThread")
                withContext(singleThreadContext) {
                    it.restore()
                    input.clusters.forEach { cluster ->
                        val targetClient = CrossClusterMonitorUtils.getClientForCluster(cluster, client, clusterService)
                        val response = executeTransportAction(input, targetClient)
                        // Not all supported API reference the cluster name in their response.
                        // Mapping each response to the cluster name before adding to results.
                        // Not adding this same logic for local-only monitors to avoid breaking existing monitors.
                        responseMap[cluster] = response.toMap()
                    }
                    results += responseMap
                }
            }
        } else {
            // Else only execute the API against the local cluster.
            val response = executeTransportAction(input, client)
            results += response.toMap()
        }
        return results
    }

    fun resolveStartTimeofQueryTimeRange(monitor: Monitor, query: QueryBuilder, periodEnd: Instant): Instant? {
        try {
            val rangeQuery = findRangeQuery(query) ?: return null
            val searchParameter = rangeQuery.from().toString() // we are looking for 'timeframe' variable {{period_end}}||-<timeframe>

            val timeframeString = searchParameter.substringAfter("||-")
            val timeframeRegex = Regex("(\\d+)([a-zA-Z]+)")
            val matchResult = timeframeRegex.find(timeframeString)
            val (amount, unit) = matchResult?.destructured?.let { (a, u) -> a to u }
                ?: throw IllegalArgumentException("Invalid timeframe format: $timeframeString")
            val duration = when (unit) {
                "s" -> Duration.ofSeconds(amount.toLong())
                "m" -> Duration.ofMinutes(amount.toLong())
                "h" -> Duration.ofHours(amount.toLong())
                "d" -> Duration.ofDays(amount.toLong())
                else -> throw IllegalArgumentException("Invalid time unit: $unit")
            }

            return periodEnd.minus(duration)
        } catch (e: Exception) {
            logger.error(
                "Monitor ${monitor.id}:" +
                    " Failed to resolve time frame of search query while optimizing to query only on few of alias' concrete indices",
                e
            )
            return null // won't do optimization as we failed to resolve the timeframe due to unexpected error
        }
    }

    private fun findRangeQuery(queryBuilder: QueryBuilder?): RangeQueryBuilder? {
        if (queryBuilder == null) return null
        if (queryBuilder is RangeQueryBuilder) return queryBuilder

        if (queryBuilder is BoolQueryBuilder) {
            for (clause in queryBuilder.must()) {
                val rangeQuery = findRangeQuery(clause)
                if (rangeQuery != null) return rangeQuery
            }
            for (clause in queryBuilder.should()) {
                val rangeQuery = findRangeQuery(clause)
                if (rangeQuery != null) return rangeQuery
            }
            // You can also check queryBuilder.filter() and queryBuilder.mustNot() if needed
        }

        // Add handling for other query types if necessary (e.g., NestedQueryBuilder, etc.)
        return null
    }
}
