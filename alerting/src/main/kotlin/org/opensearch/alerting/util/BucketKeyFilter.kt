/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.commons.alerting.aggregation.bucketselectorext.BucketSelectorExtFilter
import org.opensearch.commons.alerting.model.AggregationResultBucket
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude
import java.util.regex.Pattern

/**
 * Applies [BucketSelectorExtFilter] include/exclude patterns to bucket results post-response.
 *
 * In the standard `bucket_selector` approach, the filter cannot be applied server-side since
 * `BucketSelectorExt` is a custom plugin aggregation. Instead, the include/exclude filtering
 * is applied after the response is received.
 */
object BucketKeyFilter {

    /**
     * Filters buckets by applying include/exclude patterns from the [BucketSelectorExtFilter].
     *
     * @param buckets The triggered buckets keyed by bucket keys hash
     * @param filter The optional filter with include/exclude patterns; null means pass-through
     * @return Filtered map of buckets
     */
    fun filterBuckets(
        buckets: Map<String, AggregationResultBucket>,
        filter: BucketSelectorExtFilter?
    ): Map<String, AggregationResultBucket> {
        if (filter == null) return buckets

        return if (filter.isCompositeAggregation) {
            filterCompositeKeys(buckets, filter.filtersMap ?: return buckets)
        } else {
            filterSimpleKeys(buckets, filter.filters ?: return buckets)
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun filterCompositeKeys(
        buckets: Map<String, AggregationResultBucket>,
        filtersMap: HashMap<String, IncludeExclude>
    ): Map<String, AggregationResultBucket> {
        return buckets.filter { (_, bucket) ->
            val keyMap = bucket.bucket?.get("key") as? Map<String, Any> ?: return@filter true
            filtersMap.all { (sourceKey, includeExclude) ->
                val value = keyMap[sourceKey]?.toString() ?: return@all true
                isAccepted(value, includeExclude)
            }
        }
    }

    private fun filterSimpleKeys(
        buckets: Map<String, AggregationResultBucket>,
        includeExclude: IncludeExclude
    ): Map<String, AggregationResultBucket> {
        return buckets.filter { (_, bucket) ->
            val key = bucket.bucketKeys.joinToString("#")
            isAccepted(key, includeExclude)
        }
    }

    internal fun isAccepted(value: String, includeExclude: IncludeExclude): Boolean {
        val (includeRegex, excludeRegex) = extractPatterns(includeExclude)
        if (includeRegex != null && !includeRegex.matcher(value).matches()) return false
        if (excludeRegex != null && excludeRegex.matcher(value).matches()) return false
        return true
    }

    /**
     * Extracts include/exclude regex patterns from [IncludeExclude].
     * Uses reflection to access private fields since no public API exposes the raw patterns.
     */
    private fun extractPatterns(includeExclude: IncludeExclude): Pair<Pattern?, Pattern?> {
        val clazz = IncludeExclude::class.java
        val includeField = clazz.getDeclaredField("include")
        val excludeField = clazz.getDeclaredField("exclude")
        includeField.isAccessible = true
        excludeField.isAccessible = true
        val include = (includeField.get(includeExclude) as? String)?.let { Pattern.compile(it) }
        val exclude = (excludeField.get(includeExclude) as? String)?.let { Pattern.compile(it) }
        return Pair(include, exclude)
    }
}
