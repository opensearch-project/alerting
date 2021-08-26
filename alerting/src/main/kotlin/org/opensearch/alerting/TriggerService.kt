/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.aggregation.bucketselectorext.BucketSelectorIndices.Fields.BUCKET_INDICES
import org.opensearch.alerting.aggregation.bucketselectorext.BucketSelectorIndices.Fields.PARENT_BUCKET_PATH
import org.opensearch.alerting.model.AggregationResultBucket
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.BucketLevelTrigger
import org.opensearch.alerting.model.BucketLevelTriggerRunResult
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.QueryLevelTrigger
import org.opensearch.alerting.model.QueryLevelTriggerRunResult
import org.opensearch.alerting.script.BucketLevelTriggerExecutionContext
import org.opensearch.alerting.script.QueryLevelTriggerExecutionContext
import org.opensearch.alerting.script.TriggerScript
import org.opensearch.alerting.util.getBucketKeysHash
import org.opensearch.script.ScriptService
import org.opensearch.search.aggregations.Aggregation
import org.opensearch.search.aggregations.Aggregations
import org.opensearch.search.aggregations.support.AggregationPath
import java.lang.IllegalArgumentException

/** Service that handles executing Triggers */
class TriggerService(val scriptService: ScriptService) {

    private val logger = LogManager.getLogger(TriggerService::class.java)

    fun isQueryLevelTriggerActionable(ctx: QueryLevelTriggerExecutionContext, result: QueryLevelTriggerRunResult): Boolean {
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

    @Suppress("UNCHECKED_CAST")
    fun runBucketLevelTrigger(
        monitor: Monitor,
        trigger: BucketLevelTrigger,
        ctx: BucketLevelTriggerExecutionContext
    ): BucketLevelTriggerRunResult {
        return try {
            val bucketIndices =
                ((ctx.results[0][Aggregations.AGGREGATIONS_FIELD] as HashMap<*, *>)[trigger.id] as HashMap<*, *>)[BUCKET_INDICES] as List<*>
            val parentBucketPath = ((ctx.results[0][Aggregations.AGGREGATIONS_FIELD] as HashMap<*, *>)
                .get(trigger.id) as HashMap<*, *>)[PARENT_BUCKET_PATH] as String
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
            logger.info("Error running script for monitor ${monitor.id}, trigger: ${trigger.id}", e)
            // TODO empty map here with error should be treated in the same way as QueryLevelTrigger with error running script
            BucketLevelTriggerRunResult(trigger.name, e, emptyMap())
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun getBucketKeyValuesList(bucket: Map<String, Any>): List<String> {
        val keyField = Aggregation.CommonFields.KEY.preferredName
        val keyValuesList = mutableListOf<String>()
        when {
            bucket[keyField] is String -> keyValuesList.add(bucket[keyField] as String)
            // In the case where the key field is an object with multiple values (such as a composite aggregation with more than one source)
            // the values will be iterated through and converted into a string
            bucket[keyField] is Map<*, *> -> (bucket[keyField] as Map<String, Any>).values.map { keyValuesList.add(it as String) }
            else -> throw IllegalArgumentException("Unexpected format for key in bucket [$bucket]")
        }

        return keyValuesList
    }
}
