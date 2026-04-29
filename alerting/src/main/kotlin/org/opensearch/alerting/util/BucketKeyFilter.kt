/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.aggregation.bucketselectorext.BucketSelectorExtFilter
import org.opensearch.commons.alerting.model.AggregationResultBucket
import org.opensearch.core.xcontent.ToXContent
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
        val patterns = filtersMap.mapValues { (_, ie) -> extractPatterns(ie) }
        return buckets.filter { (_, bucket) ->
            val keyMap = bucket.bucket?.get("key") as? Map<String, Any> ?: return@filter true
            patterns.all { (sourceKey, regexPair) ->
                val value = keyMap[sourceKey]?.toString() ?: return@all true
                isAccepted(value, regexPair)
            }
        }
    }

    private fun filterSimpleKeys(
        buckets: Map<String, AggregationResultBucket>,
        includeExclude: IncludeExclude
    ): Map<String, AggregationResultBucket> {
        val regexPair = extractPatterns(includeExclude)
        return buckets.filter { (_, bucket) ->
            val key = bucket.bucketKeys.joinToString("#")
            isAccepted(key, regexPair)
        }
    }

    private fun isAccepted(value: String, patterns: Pair<Pattern?, Pattern?>): Boolean {
        val (includeRegex, excludeRegex) = patterns
        if (includeRegex != null && !includeRegex.matcher(value).matches()) return false
        if (excludeRegex != null && excludeRegex.matcher(value).matches()) return false
        return true
    }

    /**
     * Extracts include/exclude regex strings from [IncludeExclude] via XContent serialization.
     */
    private fun extractPatterns(includeExclude: IncludeExclude): Pair<Pattern?, Pattern?> {
        val builder = XContentFactory.jsonBuilder().startObject()
        includeExclude.toXContent(builder, ToXContent.EMPTY_PARAMS)
        builder.endObject()
        val json = builder.toString()
        val map = XContentType.JSON.xContent()
            .createParser(null, null, json)
            .use { parser -> parser.map() }
        val include = (map["include"] as? String)?.let { Pattern.compile(it) }
        val exclude = (map["exclude"] as? String)?.let { Pattern.compile(it) }
        return Pair(include, exclude)
    }
}
