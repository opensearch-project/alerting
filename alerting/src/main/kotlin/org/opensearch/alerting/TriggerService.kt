/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import kotlinx.coroutines.withTimeout
import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.PPLUtils.appendCustomCondition
import org.opensearch.alerting.PPLUtils.appendDataRowsLimit
import org.opensearch.alerting.PPLUtils.capAndReformatPPLQueryResults
import org.opensearch.alerting.PPLUtils.executePplQuery
import org.opensearch.alerting.chainedAlertCondition.parsers.ChainedAlertExpressionParser
import org.opensearch.alerting.opensearchapi.InjectorContextElement
import org.opensearch.alerting.opensearchapi.withClosableContext
import org.opensearch.alerting.script.BucketLevelTriggerExecutionContext
import org.opensearch.alerting.script.ChainedAlertTriggerExecutionContext
import org.opensearch.alerting.script.QueryLevelTriggerExecutionContext
import org.opensearch.alerting.script.TriggerScript
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.triggercondition.parsers.TriggerExpressionParser
import org.opensearch.alerting.util.BucketKeyFilter
import org.opensearch.alerting.util.CrossClusterMonitorUtils
import org.opensearch.alerting.util.getBucketKeysHash
import org.opensearch.cluster.service.ClusterService
import org.opensearch.commons.alerting.aggregation.bucketselectorext.BucketSelectorIndices.Fields.BUCKET_INDICES
import org.opensearch.commons.alerting.aggregation.bucketselectorext.BucketSelectorIndices.Fields.PARENT_BUCKET_PATH
import org.opensearch.commons.alerting.model.AggregationResultBucket
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.BucketLevelTrigger
import org.opensearch.commons.alerting.model.BucketLevelTriggerRunResult
import org.opensearch.commons.alerting.model.ChainedAlertTrigger
import org.opensearch.commons.alerting.model.ChainedAlertTriggerRunResult
import org.opensearch.commons.alerting.model.ClusterMetricsTriggerRunResult
import org.opensearch.commons.alerting.model.ClusterMetricsTriggerRunResult.ClusterTriggerResult
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.DocumentLevelTrigger
import org.opensearch.commons.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.PPLTrigger
import org.opensearch.commons.alerting.model.PPLTrigger.NumResultsCondition
import org.opensearch.commons.alerting.model.QueryLevelTrigger
import org.opensearch.commons.alerting.model.QueryLevelTriggerRunResult
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.model.WorkflowRunContext
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.search.aggregations.Aggregation
import org.opensearch.search.aggregations.Aggregations
import org.opensearch.search.aggregations.support.AggregationPath
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.node.NodeClient
import kotlin.time.measureTimedValue

/** Service that handles executing Triggers */
class TriggerService(val scriptService: ScriptService) {

    private val logger = LogManager.getLogger(TriggerService::class.java)
    private val ALWAYS_RUN = Script("return true")
    private val NEVER_RUN = Script("return false")

    fun isQueryLevelTriggerActionable(
        ctx: QueryLevelTriggerExecutionContext,
        result: QueryLevelTriggerRunResult,
        workflowRunContext: WorkflowRunContext?,
    ): Boolean {
        if (workflowRunContext?.auditDelegateMonitorAlerts == true) return false
        // Suppress actions if the current alert is acknowledged and there are no errors.
        val suppress = ctx.alert?.alert?.state == Alert.State.ACKNOWLEDGED && result.error == null && ctx.error == null
        return result.triggered && !suppress
    }

    fun isChainedAlertTriggerActionable(
        ctx: ChainedAlertTriggerExecutionContext,
        result: ChainedAlertTriggerRunResult,
    ): Boolean {
        // Suppress actions if the current alert is acknowledged and there are no errors.
        val suppress = ctx.alert?.state == Alert.State.ACKNOWLEDGED && result.error == null && ctx.error == null
        return result.triggered && !suppress
    }

    fun runQueryLevelTrigger(
        monitor: Monitor,
        trigger: QueryLevelTrigger,
        ctx: QueryLevelTriggerExecutionContext
    ): QueryLevelTriggerRunResult {
        return try {
            val triggered = scriptService.compile(trigger.condition, TriggerScript.CONTEXT)
                .newInstance(trigger.condition.params)
                .execute(ctx)
            QueryLevelTriggerRunResult(trigger.name, triggered, null)
        } catch (e: Exception) {
            logger.info("Error running script for monitor ${monitor.id}, trigger: ${trigger.id}", e)
            // if the script fails we need to send an alert so set triggered = true
            QueryLevelTriggerRunResult(trigger.name, true, e)
        }
    }

    fun runClusterMetricsTrigger(
        monitor: Monitor,
        trigger: QueryLevelTrigger,
        ctx: QueryLevelTriggerExecutionContext,
        clusterService: ClusterService
    ): ClusterMetricsTriggerRunResult {
        var runResult: ClusterMetricsTriggerRunResult?
        try {
            val inputResults = ctx.results.getOrElse(0) { mapOf() }
            var triggered = false
            val clusterTriggerResults = mutableListOf<ClusterTriggerResult>()
            if (CrossClusterMonitorUtils.isRemoteMonitor(monitor, clusterService)) {
                inputResults.forEach { clusterResult ->
                    // Reducing the inputResults to only include results from 1 cluster at a time
                    val clusterTriggerCtx = ctx.copy(results = listOf(mapOf(clusterResult.toPair())))

                    val clusterTriggered = scriptService.compile(trigger.condition, TriggerScript.CONTEXT)
                        .newInstance(trigger.condition.params)
                        .execute(clusterTriggerCtx)

                    if (clusterTriggered) {
                        triggered = clusterTriggered
                        clusterTriggerResults.add(ClusterTriggerResult(cluster = clusterResult.key, triggered = clusterTriggered))
                    }
                }
            } else {
                triggered = scriptService.compile(trigger.condition, TriggerScript.CONTEXT)
                    .newInstance(trigger.condition.params)
                    .execute(ctx)
                if (triggered) clusterTriggerResults
                    .add(ClusterTriggerResult(cluster = clusterService.clusterName.value(), triggered = triggered))
            }
            runResult = ClusterMetricsTriggerRunResult(
                triggerName = trigger.name,
                triggered = triggered,
                error = null,
                clusterTriggerResults = clusterTriggerResults
            )
        } catch (e: Exception) {
            logger.info("Error running script for monitor ${monitor.id}, trigger: ${trigger.id}", e)
            // if the script fails we need to send an alert so set triggered = true
            runResult = ClusterMetricsTriggerRunResult(trigger.name, true, e)
        }
        return runResult!!
    }

    // TODO: improve performance and support match all and match any
    fun runDocLevelTrigger(
        monitor: Monitor,
        trigger: DocumentLevelTrigger,
        queryToDocIds: Map<DocLevelQuery, Set<String>>
    ): DocumentLevelTriggerRunResult {
        return try {
            var triggeredDocs = mutableListOf<String>()

            if (trigger.condition.idOrCode.equals(ALWAYS_RUN.idOrCode)) {
                for (value in queryToDocIds.values) {
                    triggeredDocs.addAll(value)
                }
            } else if (!trigger.condition.idOrCode.equals(NEVER_RUN.idOrCode)) {
                triggeredDocs = TriggerExpressionParser(trigger.condition.idOrCode).parse()
                    .evaluate(queryToDocIds).toMutableList()
            }

            DocumentLevelTriggerRunResult(trigger.name, triggeredDocs, null)
        } catch (e: Exception) {
            logger.info("Error running script for monitor ${monitor.id}, trigger: ${trigger.id}", e)
            // if the script fails we need to send an alert so set triggered = true
            DocumentLevelTriggerRunResult(trigger.name, emptyList(), e)
        }
    }

    fun runChainedAlertTrigger(
        workflow: Workflow,
        trigger: ChainedAlertTrigger,
        alertGeneratingMonitors: Set<String>,
        monitorIdToAlertIdsMap: Map<String, Set<String>>,
    ): ChainedAlertTriggerRunResult {
        val associatedAlertIds = mutableSetOf<String>()
        return try {
            val parsedTriggerCondition = ChainedAlertExpressionParser(trigger.condition.idOrCode).parse()
            val evaluate = parsedTriggerCondition.evaluate(alertGeneratingMonitors)
            if (evaluate) {
                val monitorIdsInTriggerCondition = parsedTriggerCondition.getMonitorIds(parsedTriggerCondition)
                monitorIdsInTriggerCondition.forEach { associatedAlertIds.addAll(monitorIdToAlertIdsMap.getOrDefault(it, emptySet())) }
            }
            ChainedAlertTriggerRunResult(trigger.name, triggered = evaluate, null, associatedAlertIds = associatedAlertIds)
        } catch (e: Exception) {
            logger.error("Error running chained alert trigger script for workflow ${workflow.id}, trigger: ${trigger.id}", e)
            ChainedAlertTriggerRunResult(
                triggerName = trigger.name,
                triggered = false,
                error = e,
                associatedAlertIds = emptySet()
            )
        }
    }

    fun runBucketLevelTriggerFromFilteredResponse(
        monitor: Monitor,
        trigger: BucketLevelTrigger,
        ctx: BucketLevelTriggerExecutionContext
    ): BucketLevelTriggerRunResult {
        return try {
            val parentBucketPath = trigger.bucketSelector.parentBucketPath
            val aggregationPath = AggregationPath.parse(parentBucketPath)
            val aggs = ctx.results[0][Aggregations.AGGREGATIONS_FIELD]
            require(aggs is Map<*, *>) { "Unexpected aggregations type: ${aggs?.javaClass}" }
            var parentAgg: Map<*, *> = aggs
            aggregationPath.pathElementsAsStringList.forEach { subAgg ->
                val child = parentAgg[subAgg]
                require(child is Map<*, *>) { "Unexpected type for agg '$subAgg': ${child?.javaClass}" }
                parentAgg = child
            }
            val buckets = parentAgg[Aggregation.CommonFields.BUCKETS.preferredName]
            require(buckets is List<*>) { "Unexpected buckets type: ${buckets?.javaClass}" }
            val selectedBuckets = mutableMapOf<String, AggregationResultBucket>()
            for (bucket in buckets) {
                require(bucket is Map<*, *>) { "Unexpected bucket type: ${bucket?.javaClass}" }
                @Suppress("UNCHECKED_CAST")
                val bucketDict = bucket as Map<String, Any>
                val bucketKeyValuesList = getBucketKeyValuesList(bucketDict)
                val aggResultBucket = AggregationResultBucket(parentBucketPath, bucketKeyValuesList, bucketDict)
                selectedBuckets[aggResultBucket.getBucketKeysHash()] = aggResultBucket
            }
            val filteredBuckets = BucketKeyFilter.filterBuckets(selectedBuckets, trigger.bucketSelector.filter)
            BucketLevelTriggerRunResult(trigger.name, null, filteredBuckets)
        } catch (e: Exception) {
            logger.info("Error running trigger [${trigger.id}] for monitor [${monitor.id}]", e)
            BucketLevelTriggerRunResult(trigger.name, e, emptyMap())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun runBucketLevelTrigger(
        monitor: Monitor,
        trigger: BucketLevelTrigger,
        ctx: BucketLevelTriggerExecutionContext
    ): BucketLevelTriggerRunResult {
        return try {
            val bucketIndices = (
                (ctx.results[0][Aggregations.AGGREGATIONS_FIELD] as HashMap<*, *>)
                    .get(trigger.id) as HashMap<*, *>
                )[BUCKET_INDICES] as List<*>
            val parentBucketPath = (
                (ctx.results[0][Aggregations.AGGREGATIONS_FIELD] as HashMap<*, *>)
                    .get(trigger.id) as HashMap<*, *>
                )[PARENT_BUCKET_PATH] as String
            val aggregationPath = AggregationPath.parse(parentBucketPath)
            // TODO test this part by passing sub-aggregation path
            var parentAgg = (ctx.results[0][Aggregations.AGGREGATIONS_FIELD] as HashMap<*, *>)
            aggregationPath.pathElementsAsStringList.forEach { sub_agg ->
                parentAgg = (parentAgg[sub_agg] as HashMap<*, *>)
            }
            val buckets = parentAgg[Aggregation.CommonFields.BUCKETS.preferredName] as List<*>
            val selectedBuckets = mutableMapOf<String, AggregationResultBucket>()
            for (bucketIndex in bucketIndices) {
                val bucketDict = buckets[bucketIndex as Int] as Map<String, Any>
                val bucketKeyValuesList = getBucketKeyValuesList(bucketDict)
                val aggResultBucket = AggregationResultBucket(parentBucketPath, bucketKeyValuesList, bucketDict)
                selectedBuckets[aggResultBucket.getBucketKeysHash()] = aggResultBucket
            }
            BucketLevelTriggerRunResult(trigger.name, null, selectedBuckets)
        } catch (e: Exception) {
            logger.info("Error running trigger [${trigger.id}] for monitor [${monitor.id}]", e)
            BucketLevelTriggerRunResult(trigger.name, e, emptyMap())
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun getBucketKeyValuesList(bucket: Map<String, Any>): List<String> {
        val keyField = Aggregation.CommonFields.KEY.preferredName
        val keyValuesList = mutableListOf<String>()
        when {
            bucket[keyField] is List<*> && bucket.containsKey(Aggregation.CommonFields.KEY_AS_STRING.preferredName) ->
                keyValuesList.add(bucket[Aggregation.CommonFields.KEY_AS_STRING.preferredName] as String)
            bucket[keyField] is String -> keyValuesList.add(bucket[keyField] as String)
            // In the case where the key field is an Int
            bucket[keyField] is Int -> keyValuesList.add(bucket[keyField].toString())
            // In the case where the key field is an object with multiple values (such as a composite aggregation with more than one source)
            // the values will be iterated through and converted into a string
            bucket[keyField] is Map<*, *> -> (bucket[keyField] as Map<String, Any>).values.map { keyValuesList.add(it.toString()) }
            else -> throw IllegalArgumentException("Unexpected format for key in bucket [$bucket]")
        }

        return keyValuesList
    }

    fun runPplNumResultsTrigger(
        pplTrigger: PPLTrigger,
        numResults: Long?
    ): QueryLevelTriggerRunResult {

        if (numResults == null) {
            return QueryLevelTriggerRunResult(
                pplTrigger.name,
                true,
                IllegalStateException("Did not receive a number of results from PPL query execution: ${pplTrigger.id}")
            )
        }

        if (pplTrigger.numResultsCondition == null) {
            return QueryLevelTriggerRunResult(
                pplTrigger.name,
                true,
                IllegalStateException("No number of results condition found for trigger: ${pplTrigger.id}")
            )
        }

        if (pplTrigger.numResultsValue == null) {
            return QueryLevelTriggerRunResult(
                pplTrigger.name,
                true,
                IllegalStateException("No number of results value found for trigger: ${pplTrigger.id}")
            )
        }

        val numResultsCondition = pplTrigger.numResultsCondition!!
        val numResultsValue = pplTrigger.numResultsValue!!

        val triggered = when (numResultsCondition) {
            NumResultsCondition.GREATER_THAN -> numResults > numResultsValue
            NumResultsCondition.GREATER_THAN_EQUAL -> numResults >= numResultsValue
            NumResultsCondition.LESS_THAN -> numResults < numResultsValue
            NumResultsCondition.LESS_THAN_EQUAL -> numResults <= numResultsValue
            NumResultsCondition.EQUAL -> numResults == numResultsValue
            NumResultsCondition.NOT_EQUAL -> numResults != numResultsValue
        }

        logger.debug("Number of Results PPLTrigger ${pplTrigger.name} with ID ${pplTrigger.id} triggered: $triggered")

        // unlike evaluating custom conditions, where we must include the query results because
        // a custom condition executes its own PPL query, number of results trigger evaluation
        // doesn't need to include query results because they're evaluated purely on the size
        // of the results. the results themselves are already held in QueryLevelMonitorRunner.kt
        // (the caller of this function), so passing the results here only to return them again
        // inside the QueryLevelTriggerRunResult would be redundant
        return QueryLevelTriggerRunResult(pplTrigger.name, triggered, null)
    }

    suspend fun runPplCustomTrigger(
        pplMonitor: Monitor,
        pplTrigger: PPLTrigger,
        query: String,
        monitorCtx: MonitorRunnerExecutionContext,
        transportService: TransportService
    ): QueryLevelTriggerRunResult {

        if (pplTrigger.customCondition == null) {
            return QueryLevelTriggerRunResult(
                pplTrigger.name,
                true,
                IllegalStateException("No custom condition found for trigger: ${pplTrigger.id}")
            )
        }

        // TODO: change name to trigger max duration
        val monitorExecutionDuration = monitorCtx
            .clusterService!!
            .clusterSettings
            .get(AlertingSettings.PPL_MONITOR_EXECUTION_MAX_DURATION)
        val queryResultsSizeLimit = monitorCtx
            .clusterService!!
            .clusterSettings
            .get(AlertingSettings.PPL_QUERY_RESULTS_MAX_SIZE)

        var triggered: Boolean? = null
        var customConditionQueryResults: List<Map<String, Any?>>? = null

        try {
            withTimeout(monitorExecutionDuration.millis) {
                logger.debug("checking if custom condition is used and appending to base query")

                val customCondition = pplTrigger.customCondition!!

                // append the custom condition to query
                val queryToExecute = appendCustomCondition(query, customCondition)

                // limit the number of PPL query result data rows returned
                val dataRowsLimit = monitorCtx.clusterService!!.clusterSettings.get(AlertingSettings.PPL_QUERY_RESULTS_MAX_DATAROWS)
                val limitedQueryToExecute = appendDataRowsLimit(queryToExecute, dataRowsLimit)

                // TODO: after getting ppl query results, see if the number of results
                // retrieved equals the max allowed number of query results. this implies
                // query results might have been excluded, in which case a warning message
                // in the alert and notification must be added that results were excluded
                // and an alert that should have been generated might not have been

                logger.debug("executing the PPL query of monitor: ${pplMonitor.id} with custom condition: $customCondition")
                // execute the PPL query
                val (queryResponseJson, timeTaken) = measureTimedValue {
                    withClosableContext(
                        InjectorContextElement(
                            pplMonitor.id,
                            monitorCtx.settings!!,
                            monitorCtx.threadPool!!.threadContext,
                            pplMonitor.user?.roles,
                            pplMonitor.user
                        )
                    ) {
                        executePplQuery(
                            limitedQueryToExecute,
                            false,
                            monitorCtx.client!! as NodeClient
                        )
                    }
                }
                logger.debug("query results for trigger ${pplTrigger.id}: $queryResponseJson")
                logger.debug("time taken to execute query against sql/ppl plugin: $timeTaken")

                // val numPplResults = basePplQueryResults.getLong("total")

                // the custom condition query returns all buckets that met the custom condition,
                // so if there are any results at all, the custom condition was met for at least one bukcet,
                // this trigger has triggered.
                triggered = queryResponseJson.getLong("total") > 0

                // cap and reformat the results to be included in trigger run result
                customConditionQueryResults = capAndReformatPPLQueryResults(queryResponseJson, queryResultsSizeLimit)

                logger.debug("Custom PPLTrigger ${pplTrigger.name} with ID ${pplTrigger.id} triggered: $triggered")
            }

            return QueryLevelTriggerRunResult(
                pplTrigger.name,
                triggered!!,
                null,
                mutableMapOf(),
                customConditionQueryResults!!
            )
        } catch (e: Exception) {
            logger.error(
                "failed to run PPL Custom Trigger ${pplTrigger.name} (id: ${pplTrigger.id} " +
                    "from PPL Monitor ${pplMonitor.name} (id: ${pplMonitor.id}",
                e
            )

            return QueryLevelTriggerRunResult(pplTrigger.name, true, e)
        }
    }
}
