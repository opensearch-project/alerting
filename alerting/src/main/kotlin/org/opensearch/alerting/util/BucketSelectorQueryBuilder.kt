/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.commons.alerting.model.BucketLevelTrigger
import org.opensearch.search.aggregations.AggregationBuilder
import org.opensearch.search.aggregations.AggregatorFactories
import org.opensearch.search.aggregations.PipelineAggregatorBuilders
import org.opensearch.search.aggregations.support.AggregationPath
import org.opensearch.search.builder.SearchSourceBuilder

/**
 * Constructs standard `bucket_selector` pipeline aggregations from [BucketLevelTrigger] conditions
 * and injects them as sub-aggregations of the parent aggregation identified by `parentBucketPath`.
 *
 * This replaces the custom `BucketSelectorExt` approach for environments where custom plugin
 * aggregations are not available (e.g., serverless clusters). Standard `bucket_selector` natively
 * removes non-matching buckets, so remaining buckets in the response are the triggered buckets.
 */
object BucketSelectorQueryBuilder {

    const val TRIGGER_FILTER_PREFIX = "_trigger_filter_"

    /**
     * Injects a standard `bucket_selector` pipeline sub-aggregation for each trigger under its
     * parent aggregation identified by [BucketLevelTrigger.bucketSelector.parentBucketPath].
     *
     * @param query The search source to modify
     * @param triggers The bucket-level triggers whose conditions should be injected
     * @return The modified search source
     * @throws IllegalArgumentException if a trigger's parentBucketPath cannot be resolved
     */
    fun injectBucketSelector(query: SearchSourceBuilder, triggers: List<BucketLevelTrigger>): SearchSourceBuilder {
        for (trigger in triggers) {
            val selector = trigger.bucketSelector
            val parentAgg = findParentAgg(
                query.aggregations() as AggregatorFactories.Builder,
                selector.parentBucketPath
            )
            val pipelineAgg = PipelineAggregatorBuilders.bucketSelector(
                "$TRIGGER_FILTER_PREFIX${trigger.id}",
                selector.bucketsPathsMap,
                selector.script
            )
            parentAgg.subAggregation(pipelineAgg)
        }
        return query
    }

    private fun findParentAgg(aggFactories: AggregatorFactories.Builder, parentBucketPath: String): AggregationBuilder {
        val pathElements = AggregationPath.parse(parentBucketPath).pathElementsAsStringList
        var aggBuilders = aggFactories.aggregatorFactories
        var found: AggregationBuilder? = null

        for (element in pathElements) {
            found = null
            for (agg in aggBuilders) {
                if (agg.name == element) {
                    found = agg
                    aggBuilders = agg.subAggregations
                    break
                }
            }
            if (found == null) {
                throw IllegalArgumentException("ParentBucketPath: $parentBucketPath not found in query aggregations")
            }
        }

        return found!!
    }
}
