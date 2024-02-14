/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.chainedAlertCondition.parsers.ChainedAlertExpressionParser
import org.opensearch.alerting.model.BucketLevelTriggerRunResult
import org.opensearch.alerting.model.ChainedAlertTriggerRunResult
import org.opensearch.alerting.model.ClusterMetricsTriggerRunResult
import org.opensearch.alerting.model.ClusterMetricsTriggerRunResult.ClusterTriggerResult
import org.opensearch.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.alerting.model.QueryLevelTriggerRunResult
import org.opensearch.alerting.script.BucketLevelTriggerExecutionContext
import org.opensearch.alerting.script.ChainedAlertTriggerExecutionContext
import org.opensearch.alerting.script.QueryLevelTriggerExecutionContext
import org.opensearch.alerting.script.TriggerScript
import org.opensearch.alerting.triggercondition.parsers.TriggerExpressionParser
import org.opensearch.alerting.util.CrossClusterMonitorUtils
import org.opensearch.alerting.util.getBucketKeysHash
import org.opensearch.alerting.workflow.WorkflowRunContext
import org.opensearch.cluster.service.ClusterService
import org.opensearch.commons.alerting.aggregation.bucketselectorext.BucketSelectorIndices.Fields.BUCKET_INDICES
import org.opensearch.commons.alerting.aggregation.bucketselectorext.BucketSelectorIndices.Fields.PARENT_BUCKET_PATH
import org.opensearch.commons.alerting.model.AggregationResultBucket
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.BucketLevelTrigger
import org.opensearch.commons.alerting.model.ChainedAlertTrigger
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.DocumentLevelTrigger
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.QueryLevelTrigger
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.search.aggregations.Aggregation
import org.opensearch.search.aggregations.Aggregations
import org.opensearch.search.aggregations.support.AggregationPath

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
        val suppress = ctx.alert?.state == Alert.State.ACKNOWLEDGED && result.error == null && ctx.error == null
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

    @Suppress("UNCHECKED_CAST")
    fun runBucketLevelTrigger(
        monitor: Monitor,
        trigger: BucketLevelTrigger,
        ctx: BucketLevelTriggerExecutionContext
    ): BucketLevelTriggerRunResult {
        return try {
            val bucketIndices =
                ((ctx.results[0][Aggregations.AGGREGATIONS_FIELD] as HashMap<*, *>)[trigger.id] as HashMap<*, *>)[BUCKET_INDICES] as List<*>
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
}
