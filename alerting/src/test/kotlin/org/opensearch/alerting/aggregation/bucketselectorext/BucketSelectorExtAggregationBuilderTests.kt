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

package org.opensearch.alerting.aggregation.bucketselectorext

import org.opensearch.alerting.AlertingPlugin
import org.opensearch.plugins.SearchPlugin
import org.opensearch.script.Script
import org.opensearch.script.ScriptType
import org.opensearch.search.aggregations.BasePipelineAggregationTestCase
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude
import org.opensearch.search.aggregations.pipeline.BucketHelpers.GapPolicy

class BucketSelectorExtAggregationBuilderTests : BasePipelineAggregationTestCase<BucketSelectorExtAggregationBuilder>() {
    override fun plugins(): List<SearchPlugin?> {
        return listOf(AlertingPlugin())
    }

    override fun createTestAggregatorFactory(): BucketSelectorExtAggregationBuilder {
        val name = randomAlphaOfLengthBetween(3, 20)
        val bucketsPaths: MutableMap<String, String> = HashMap()
        val numBucketPaths = randomIntBetween(1, 10)
        for (i in 0 until numBucketPaths) {
            bucketsPaths[randomAlphaOfLengthBetween(1, 20)] = randomAlphaOfLengthBetween(1, 40)
        }
        val script: Script
        if (randomBoolean()) {
            script = mockScript("script")
        } else {
            val params: MutableMap<String, Any> = HashMap()
            if (randomBoolean()) {
                params["foo"] = "bar"
            }
            val type = randomFrom(*ScriptType.values())
            script =
                Script(
                    type, if (type == ScriptType.STORED) null else
                    randomFrom("my_lang", Script.DEFAULT_SCRIPT_LANG), "script", params
                )
        }
        val parentBucketPath = randomAlphaOfLengthBetween(3, 20)
        val filter = BucketSelectorExtFilter(IncludeExclude("foo.*", "bar.*"))
        val factory = BucketSelectorExtAggregationBuilder(
            name, bucketsPaths,
            script, parentBucketPath, filter
        )
        if (randomBoolean()) {
            factory.gapPolicy(randomFrom(*GapPolicy.values()))
        }
        return factory
    }
}
