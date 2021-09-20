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

package org.opensearch.alerting.util

import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.model.BucketLevelTrigger
import org.opensearch.alerting.model.InputRunResults
import org.opensearch.alerting.model.Trigger
import org.opensearch.alerting.model.TriggerAfterKey
import org.opensearch.search.aggregations.AggregationBuilder
import org.opensearch.search.aggregations.AggregatorFactories
import org.opensearch.search.aggregations.bucket.SingleBucketAggregation
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.opensearch.search.aggregations.support.AggregationPath
import org.opensearch.search.builder.SearchSourceBuilder

class AggregationQueryRewriter {

    companion object {
        /**
         * Add the bucket selector conditions for each trigger in input query. It also adds afterKeys from previous result
         * for each trigger.
         */
        fun rewriteQuery(query: SearchSourceBuilder, prevResult: InputRunResults?, triggers: List<Trigger>): SearchSourceBuilder {
            triggers.forEach { trigger ->
                if (trigger is BucketLevelTrigger) {
                    // add bucket selector pipeline aggregation for each trigger in query
                    query.aggregation(trigger.bucketSelector)
                    // if this request is processing the subsequent pages of input query result, then add after key
                    if (prevResult?.aggTriggersAfterKey?.get(trigger.id) != null) {
                        val parentBucketPath = AggregationPath.parse(trigger.bucketSelector.parentBucketPath)
                        var aggBuilders = (query.aggregations() as AggregatorFactories.Builder).aggregatorFactories
                        var factory: AggregationBuilder? = null
                        for (i in 0 until parentBucketPath.pathElements.size) {
                            factory = null
                            for (aggFactory in aggBuilders) {
                                if (aggFactory.name.equals(parentBucketPath.pathElements[i].name)) {
                                    aggBuilders = aggFactory.subAggregations
                                    factory = aggFactory
                                    break
                                }
                            }
                            if (factory == null) {
                                throw IllegalArgumentException("ParentBucketPath: $parentBucketPath not found in input query results")
                            }
                        }
                        if (factory is CompositeAggregationBuilder) {
                            // if the afterKey from previous result is null, what does it signify?
                            // A) result set exhausted OR  B) first page ?
                            val afterKey = prevResult.aggTriggersAfterKey[trigger.id]!!.afterKey
                            factory.aggregateAfter(afterKey)
                        } else {
                            throw IllegalStateException("AfterKeys are not expected to be present in non CompositeAggregationBuilder")
                        }
                    }
                }
            }

            return query
        }

        /**
         * For each trigger, returns the after keys if present in query result.
         */
        fun getAfterKeysFromSearchResponse(
            searchResponse: SearchResponse,
            triggers: List<Trigger>,
            prevBucketLevelTriggerAfterKeys: Map<String, TriggerAfterKey>?
        ): Map<String, TriggerAfterKey> {
            val bucketLevelTriggerAfterKeys = mutableMapOf<String, TriggerAfterKey>()
            triggers.forEach { trigger ->
                if (trigger is BucketLevelTrigger) {
                    val parentBucketPath = AggregationPath.parse(trigger.bucketSelector.parentBucketPath)
                    var aggs = searchResponse.aggregations
                    // assuming all intermediate aggregations as SingleBucketAggregation
                    for (i in 0 until parentBucketPath.pathElements.size - 1) {
                        aggs = (aggs.asMap()[parentBucketPath.pathElements[i].name] as SingleBucketAggregation).aggregations
                    }
                    val lastAgg = aggs.asMap[parentBucketPath.pathElements.last().name]
                    // if leaf is CompositeAggregation, then fetch afterKey if present
                    if (lastAgg is CompositeAggregation) {
                        /*
                         * Bucket-Level Triggers can have different parent bucket paths that they are tracking for condition evaluation.
                         * These different bucket paths could have different page sizes, meaning one could be exhausted while another
                         * bucket path still has pages to iterate in the query responses.
                         *
                         * To ensure that these can be exhausted and tracked independently, the after key that led to the last page (which
                         * should be an empty result for the bucket path) will be saved when the last page is hit and will be continued
                         * to be passed on for that bucket path if there are still other bucket paths being paginated.
                         */
                        val afterKey = lastAgg.afterKey()
                        val prevTriggerAfterKey = prevBucketLevelTriggerAfterKeys?.get(trigger.id)
                        bucketLevelTriggerAfterKeys[trigger.id] = when {
                            // If the previous TriggerAfterKey was null, this should be the first page
                            prevTriggerAfterKey == null -> TriggerAfterKey(afterKey, afterKey == null)
                            // If the previous TriggerAfterKey already hit the last page, pass along the after key it used to get there
                            prevTriggerAfterKey.lastPage -> prevTriggerAfterKey
                            // If the previous TriggerAfterKey had not reached the last page and the after key for the current result
                            // is null, then the last page has been reached so the after key that was used to get there is stored
                            afterKey == null -> TriggerAfterKey(prevTriggerAfterKey.afterKey, true)
                            // Otherwise, update the after key to the current one
                            else -> TriggerAfterKey(afterKey, false)
                        }
                    }
                }
            }
            return bucketLevelTriggerAfterKeys
        }
    }
}
