/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.alerting.randomBucketLevelTrigger
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.alerting.aggregation.bucketselectorext.BucketSelectorExtAggregationBuilder
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.script.Script
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase

class BucketSelectorQueryBuilderTests : OpenSearchTestCase() {

    private fun triggerWithSelector(
        parentBucketPath: String,
        bucketsPathsMap: Map<String, String> = mapOf("docCount" to "_count"),
        script: String = "params.docCount > 0"
    ) = randomBucketLevelTrigger().let { trigger ->
        trigger.copy(
            bucketSelector = BucketSelectorExtAggregationBuilder(
                trigger.id, bucketsPathsMap.toMutableMap(), Script(script), parentBucketPath, null
            )
        )
    }

    /** Serialize query to string for assertion — pipeline sub-aggs aren't in getSubAggregations() */
    private fun SearchSourceBuilder.toJsonString(): String {
        val builder = XContentFactory.jsonBuilder()
        toXContent(builder, ToXContent.EMPTY_PARAMS)
        return builder.toString()
    }

    fun `test inject bucket selector under composite agg`() {
        val compositeAgg = CompositeAggregationBuilder(
            "composite_agg", listOf(TermsValuesSourceBuilder("test_field").field("test_field"))
        )
        val query = SearchSourceBuilder().size(0).aggregation(compositeAgg)
        val trigger = triggerWithSelector("composite_agg")

        val result = BucketSelectorQueryBuilder.injectBucketSelector(query, listOf(trigger))

        val json = result.toJsonString()
        assertTrue("bucket_selector should be present", json.contains("bucket_selector"))
        assertTrue(
            "trigger filter name should be present",
            json.contains("${BucketSelectorQueryBuilder.TRIGGER_FILTER_PREFIX}${trigger.id}")
        )
    }

    fun `test inject bucket selector under terms agg`() {
        val termsAgg = TermsAggregationBuilder("status_codes").field("status")
        val query = SearchSourceBuilder().size(0).aggregation(termsAgg)
        val trigger = triggerWithSelector("status_codes")

        val result = BucketSelectorQueryBuilder.injectBucketSelector(query, listOf(trigger))

        val json = result.toJsonString()
        assertTrue("bucket_selector should be present", json.contains("bucket_selector"))
        assertTrue(
            "trigger filter name should be present",
            json.contains("${BucketSelectorQueryBuilder.TRIGGER_FILTER_PREFIX}${trigger.id}")
        )
    }

    fun `test inject bucket selector with nested parent path`() {
        val innerAgg = TermsAggregationBuilder("inner_agg").field("status")
        val outerAgg = CompositeAggregationBuilder(
            "outer_agg", listOf(TermsValuesSourceBuilder("host").field("host"))
        ).subAggregation(innerAgg)
        val query = SearchSourceBuilder().size(0).aggregation(outerAgg)
        val trigger = triggerWithSelector("outer_agg>inner_agg")

        val result = BucketSelectorQueryBuilder.injectBucketSelector(query, listOf(trigger))

        val json = result.toJsonString()
        assertTrue("bucket_selector should be present under inner_agg", json.contains("bucket_selector"))
        assertTrue(json.contains("${BucketSelectorQueryBuilder.TRIGGER_FILTER_PREFIX}${trigger.id}"))
        // Verify it's nested inside inner_agg by checking the JSON structure
        val innerAggIdx = json.indexOf("\"inner_agg\"")
        val bucketSelectorIdx = json.indexOf("bucket_selector")
        assertTrue("bucket_selector should appear after inner_agg", bucketSelectorIdx > innerAggIdx)
    }

    fun `test inject multiple triggers under same parent`() {
        val compositeAgg = CompositeAggregationBuilder(
            "composite_agg", listOf(TermsValuesSourceBuilder("test_field").field("test_field"))
        )
        val query = SearchSourceBuilder().size(0).aggregation(compositeAgg)
        val trigger1 = triggerWithSelector("composite_agg")
        val trigger2 = triggerWithSelector("composite_agg", script = "params.docCount > 5")

        val result = BucketSelectorQueryBuilder.injectBucketSelector(query, listOf(trigger1, trigger2))

        val json = result.toJsonString()
        assertTrue(json.contains("${BucketSelectorQueryBuilder.TRIGGER_FILTER_PREFIX}${trigger1.id}"))
        assertTrue(json.contains("${BucketSelectorQueryBuilder.TRIGGER_FILTER_PREFIX}${trigger2.id}"))
    }

    fun `test parent agg not found throws exception`() {
        val query = SearchSourceBuilder().size(0).aggregation(
            TermsAggregationBuilder("some_agg").field("field")
        )
        val trigger = triggerWithSelector("nonexistent_agg")

        expectThrows(IllegalArgumentException::class.java) {
            BucketSelectorQueryBuilder.injectBucketSelector(query, listOf(trigger))
        }
    }

    fun `test preserves existing sub aggs`() {
        val existingSubAgg = TermsAggregationBuilder("existing_sub").field("sub_field")
        val compositeAgg = CompositeAggregationBuilder(
            "composite_agg", listOf(TermsValuesSourceBuilder("test_field").field("test_field"))
        ).subAggregation(existingSubAgg)
        val query = SearchSourceBuilder().size(0).aggregation(compositeAgg)
        val trigger = triggerWithSelector("composite_agg")

        val result = BucketSelectorQueryBuilder.injectBucketSelector(query, listOf(trigger))

        val json = result.toJsonString()
        assertTrue("existing sub-agg should be preserved", json.contains("existing_sub"))
        assertTrue("bucket_selector should be added", json.contains("bucket_selector"))
        assertTrue(json.contains("${BucketSelectorQueryBuilder.TRIGGER_FILTER_PREFIX}${trigger.id}"))
    }
}
