/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.commons.alerting.aggregation.bucketselectorext.BucketSelectorExtFilter
import org.opensearch.commons.alerting.model.AggregationResultBucket
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude
import org.opensearch.test.OpenSearchTestCase

class BucketKeyFilterTests : OpenSearchTestCase() {

    private fun bucket(parentPath: String, keys: List<String>, docCount: Int = 1): AggregationResultBucket {
        val keyField = if (keys.size == 1) keys[0] else keys.associateBy { it }
        return AggregationResultBucket(parentPath, keys, mapOf("key" to keyField, "doc_count" to docCount))
    }

    private fun bucketsMap(parentPath: String, vararg keys: String): Map<String, AggregationResultBucket> {
        return keys.associate { key ->
            val b = bucket(parentPath, listOf(key))
            b.getBucketKeysHash() to b
        }
    }

    fun `test filter with null filter passes all buckets through`() {
        val buckets = bucketsMap("agg", "200", "404", "500")
        val result = BucketKeyFilter.filterBuckets(buckets, null)
        assertEquals(3, result.size)
    }

    fun `test filter with include pattern`() {
        val filter = BucketSelectorExtFilter(IncludeExclude("2.*", null))
        val buckets = bucketsMap("agg", "200", "201", "404", "500")
        val result = BucketKeyFilter.filterBuckets(buckets, filter)
        assertEquals(2, result.size)
        assertTrue(result.containsKey("200"))
        assertTrue(result.containsKey("201"))
    }

    fun `test filter with exclude pattern`() {
        val filter = BucketSelectorExtFilter(IncludeExclude(null, "4.*"))
        val buckets = bucketsMap("agg", "200", "404", "500")
        val result = BucketKeyFilter.filterBuckets(buckets, filter)
        assertEquals(2, result.size)
        assertTrue(result.containsKey("200"))
        assertTrue(result.containsKey("500"))
    }

    fun `test filter with include and exclude patterns`() {
        val filter = BucketSelectorExtFilter(IncludeExclude("2.*", "201"))
        val buckets = bucketsMap("agg", "200", "201", "404", "500")
        val result = BucketKeyFilter.filterBuckets(buckets, filter)
        // include "2.*" matches 200, 201; exclude "201" removes 201 -> only 200
        assertEquals(1, result.size)
        assertTrue(result.containsKey("200"))
    }

    fun `test filter with no matching buckets`() {
        val filter = BucketSelectorExtFilter(IncludeExclude("999", null))
        val buckets = bucketsMap("agg", "200", "404", "500")
        val result = BucketKeyFilter.filterBuckets(buckets, filter)
        assertTrue(result.isEmpty())
    }

    fun `test filter with composite agg keys`() {
        val filtersMap = hashMapOf("host" to IncludeExclude("server1", null))
        val filter = BucketSelectorExtFilter(filtersMap)
        val b1 = AggregationResultBucket(
            "comp", listOf("server1", "200"),
            mapOf("key" to mapOf("host" to "server1", "status" to "200"), "doc_count" to 3)
        )
        val b2 = AggregationResultBucket(
            "comp", listOf("server2", "500"),
            mapOf("key" to mapOf("host" to "server2", "status" to "500"), "doc_count" to 1)
        )
        val buckets = mapOf(b1.getBucketKeysHash() to b1, b2.getBucketKeysHash() to b2)
        val result = BucketKeyFilter.filterBuckets(buckets, filter)
        assertEquals(1, result.size)
        assertTrue(result.containsKey(b1.getBucketKeysHash()))
    }
}
