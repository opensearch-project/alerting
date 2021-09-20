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

import org.junit.Assert
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.model.InputRunResults
import org.opensearch.alerting.model.Trigger
import org.opensearch.alerting.model.TriggerAfterKey
import org.opensearch.alerting.randomBucketLevelTrigger
import org.opensearch.alerting.randomBucketSelectorExtAggregationBuilder
import org.opensearch.alerting.randomQueryLevelTrigger
import org.opensearch.cluster.ClusterModule
import org.opensearch.common.CheckedFunction
import org.opensearch.common.ParseField
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.search.aggregations.Aggregation
import org.opensearch.search.aggregations.AggregationBuilder
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.opensearch.search.aggregations.bucket.composite.ParsedComposite
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import java.io.IOException

class AggregationQueryRewriterTests : OpenSearchTestCase() {

    fun `test RewriteQuery empty previous result`() {
        val triggers: MutableList<Trigger> = mutableListOf()
        for (i in 0 until 10) {
            triggers.add(randomBucketLevelTrigger())
        }
        val queryBuilder = SearchSourceBuilder()
        val termAgg: AggregationBuilder = TermsAggregationBuilder("testPath").field("sports")
        queryBuilder.aggregation(termAgg)
        val prevResult = null
        AggregationQueryRewriter.rewriteQuery(queryBuilder, prevResult, triggers)
        Assert.assertEquals(queryBuilder.aggregations().pipelineAggregatorFactories.size, 10)
    }

    fun `skip test RewriteQuery with non-empty previous result`() {
        val triggers: MutableList<Trigger> = mutableListOf()
        for (i in 0 until 10) {
            triggers.add(randomBucketLevelTrigger())
        }
        val queryBuilder = SearchSourceBuilder()
        val termAgg: AggregationBuilder = CompositeAggregationBuilder(
            "testPath",
            listOf(TermsValuesSourceBuilder("k1"), TermsValuesSourceBuilder("k2"))
        )
        queryBuilder.aggregation(termAgg)
        val aggTriggersAfterKey = mutableMapOf<String, TriggerAfterKey>()
        for (trigger in triggers) {
            aggTriggersAfterKey[trigger.id] = TriggerAfterKey(hashMapOf(Pair("k1", "v1"), Pair("k2", "v2")), false)
        }
        val prevResult = InputRunResults(emptyList(), null, aggTriggersAfterKey)
        AggregationQueryRewriter.rewriteQuery(queryBuilder, prevResult, triggers)
        Assert.assertEquals(queryBuilder.aggregations().pipelineAggregatorFactories.size, 10)
        queryBuilder.aggregations().aggregatorFactories.forEach {
            if (it.name.equals("testPath")) {
//                val compAgg = it as CompositeAggregationBuilder
                // TODO: This is calling forbidden API and causing build failures, need to find an alternative
                //  instead of trying to access private member variables
//                val afterField = CompositeAggregationBuilder::class.java.getDeclaredField("after")
//                afterField.isAccessible = true
//                Assert.assertEquals(afterField.get(compAgg), hashMapOf(Pair("k1", "v1"), Pair("k2", "v2")))
            }
        }
    }

    fun `test RewriteQuery with non aggregation trigger`() {
        val triggers: MutableList<Trigger> = mutableListOf()
        for (i in 0 until 10) {
            triggers.add(randomQueryLevelTrigger())
        }
        val queryBuilder = SearchSourceBuilder()
        val termAgg: AggregationBuilder = TermsAggregationBuilder("testPath").field("sports")
        queryBuilder.aggregation(termAgg)
        val prevResult = null
        AggregationQueryRewriter.rewriteQuery(queryBuilder, prevResult, triggers)
        Assert.assertEquals(queryBuilder.aggregations().pipelineAggregatorFactories.size, 0)
    }

    fun `test after keys from search response`() {
        val responseContent = """
        {
          "took" : 97,
          "timed_out" : false,
          "_shards" : {
            "total" : 3,
            "successful" : 3,
            "skipped" : 0,
            "failed" : 0
          },
          "hits" : {
            "total" : {
              "value" : 20,
              "relation" : "eq"
            },
            "max_score" : null,
            "hits" : [ ]
          },
          "aggregations" : {
            "composite#testPath" : {
              "after_key" : {
                "sport" : "Basketball"
              },
              "buckets" : [
                {
                  "key" : {
                    "sport" : "Basketball"
                  },
                  "doc_count" : 5
                }
              ]
            }
          }
        }
        """.trimIndent()

        val aggTriggers: MutableList<Trigger> = mutableListOf(randomBucketLevelTrigger())
        val tradTriggers: MutableList<Trigger> = mutableListOf(randomQueryLevelTrigger())

        val searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, responseContent))
        val afterKeys = AggregationQueryRewriter.getAfterKeysFromSearchResponse(searchResponse, aggTriggers, null)
        Assert.assertEquals(afterKeys[aggTriggers[0].id]?.afterKey, hashMapOf(Pair("sport", "Basketball")))

        val afterKeys2 = AggregationQueryRewriter.getAfterKeysFromSearchResponse(searchResponse, tradTriggers, null)
        Assert.assertEquals(afterKeys2.size, 0)
    }

    fun `test after keys from search responses for multiple bucket paths and different page counts`() {
        val firstResponseContent = """
        {
          "took" : 0,
          "timed_out" : false,
          "_shards" : {
            "total" : 1,
            "successful" : 1,
            "skipped" : 0,
            "failed" : 0
          },
          "hits" : {
            "total" : {
              "value" : 4675,
              "relation" : "eq"
            },
            "max_score" : null,
            "hits" : [ ]
          },
          "aggregations" : {
            "composite2#smallerResults" : {
              "after_key" : {
                "category" : "Women's Shoes"
              },
              "buckets" : [
                {
                  "key" : {
                    "category" : "Women's Shoes"
                  },
                  "doc_count" : 1136
                }
              ]
            },
            "composite3#largerResults" : {
              "after_key" : {
                "user" : "abigail"
              },
              "buckets" : [
                {
                  "key" : {
                    "user" : "abd"
                  },
                  "doc_count" : 188
                },
                {
                  "key" : {
                    "user" : "abigail"
                  },
                  "doc_count" : 128
                }
              ]
            }
          }
        }
        """.trimIndent()

        val secondResponseContent = """
        {
          "took" : 0,
          "timed_out" : false,
          "_shards" : {
            "total" : 1,
            "successful" : 1,
            "skipped" : 0,
            "failed" : 0
          },
          "hits" : {
            "total" : {
              "value" : 4675,
              "relation" : "eq"
            },
            "max_score" : null,
            "hits" : [ ]
          },
          "aggregations" : {
            "composite2#smallerResults" : {
              "buckets" : [ ]
            },
            "composite3#largerResults" : {
              "after_key" : {
                "user" : "boris"
              },
              "buckets" : [
                {
                  "key" : {
                    "user" : "betty"
                  },
                  "doc_count" : 148
                },
                {
                  "key" : {
                    "user" : "boris"
                  },
                  "doc_count" : 74
                }
              ]
            }
          }
        }
        """.trimIndent()

        val thirdResponseContent = """
        {
          "took" : 0,
          "timed_out" : false,
          "_shards" : {
            "total" : 1,
            "successful" : 1,
            "skipped" : 0,
            "failed" : 0
          },
          "hits" : {
            "total" : {
              "value" : 4675,
              "relation" : "eq"
            },
            "max_score" : null,
            "hits" : [ ]
          },
          "aggregations" : {
            "composite2#smallerResults" : {
              "buckets" : [ ]
            },
            "composite3#largerResults" : {
              "buckets" : [ ]
            }
          }
        }
        """.trimIndent()

        val bucketLevelTriggers: MutableList<Trigger> = mutableListOf(
            randomBucketLevelTrigger(bucketSelector = randomBucketSelectorExtAggregationBuilder(parentBucketPath = "smallerResults")),
            randomBucketLevelTrigger(bucketSelector = randomBucketSelectorExtAggregationBuilder(parentBucketPath = "largerResults"))
        )

        var searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, firstResponseContent))
        val afterKeys = AggregationQueryRewriter.getAfterKeysFromSearchResponse(searchResponse, bucketLevelTriggers, null)
        assertEquals(hashMapOf(Pair("category", "Women's Shoes")), afterKeys[bucketLevelTriggers[0].id]?.afterKey)
        assertEquals(false, afterKeys[bucketLevelTriggers[0].id]?.lastPage)
        assertEquals(hashMapOf(Pair("user", "abigail")), afterKeys[bucketLevelTriggers[1].id]?.afterKey)
        assertEquals(false, afterKeys[bucketLevelTriggers[1].id]?.lastPage)

        searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, secondResponseContent))
        val afterKeys2 = AggregationQueryRewriter.getAfterKeysFromSearchResponse(searchResponse, bucketLevelTriggers, afterKeys)
        assertEquals(hashMapOf(Pair("category", "Women's Shoes")), afterKeys2[bucketLevelTriggers[0].id]?.afterKey)
        assertEquals(true, afterKeys2[bucketLevelTriggers[0].id]?.lastPage)
        assertEquals(hashMapOf(Pair("user", "boris")), afterKeys2[bucketLevelTriggers[1].id]?.afterKey)
        assertEquals(false, afterKeys2[bucketLevelTriggers[1].id]?.lastPage)

        searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, thirdResponseContent))
        val afterKeys3 = AggregationQueryRewriter.getAfterKeysFromSearchResponse(searchResponse, bucketLevelTriggers, afterKeys2)
        assertEquals(hashMapOf(Pair("category", "Women's Shoes")), afterKeys3[bucketLevelTriggers[0].id]?.afterKey)
        assertEquals(true, afterKeys3[bucketLevelTriggers[0].id]?.lastPage)
        assertEquals(hashMapOf(Pair("user", "boris")), afterKeys3[bucketLevelTriggers[1].id]?.afterKey)
        assertEquals(true, afterKeys3[bucketLevelTriggers[1].id]?.lastPage)
    }

    override fun xContentRegistry(): NamedXContentRegistry {
        val entries = ClusterModule.getNamedXWriteables()
        entries.add(
            NamedXContentRegistry.Entry(
                Aggregation::class.java, ParseField(CompositeAggregationBuilder.NAME),
                CheckedFunction<XContentParser, ParsedComposite, IOException> { parser: XContentParser? ->
                    ParsedComposite.fromXContent(
                        parser, "testPath"
                    )
                }
            )
        )
        entries.add(
            NamedXContentRegistry.Entry(
                Aggregation::class.java, ParseField(CompositeAggregationBuilder.NAME + "2"),
                CheckedFunction<XContentParser, ParsedComposite, IOException> { parser: XContentParser? ->
                    ParsedComposite.fromXContent(
                        parser, "smallerResults"
                    )
                }
            )
        )
        entries.add(
            NamedXContentRegistry.Entry(
                Aggregation::class.java, ParseField(CompositeAggregationBuilder.NAME + "3"),
                CheckedFunction<XContentParser, ParsedComposite, IOException> { parser: XContentParser? ->
                    ParsedComposite.fromXContent(
                        parser, "largerResults"
                    )
                }
            )
        )
        return NamedXContentRegistry(entries)
    }
}
