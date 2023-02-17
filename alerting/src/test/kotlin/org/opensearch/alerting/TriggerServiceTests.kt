/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.junit.Before
import org.mockito.Mockito
import org.opensearch.alerting.model.BucketLevelTriggerRunResult
import org.opensearch.alerting.model.InputRunResults
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.script.BucketLevelTriggerExecutionContext
import org.opensearch.common.xcontent.DeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentType
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant

class TriggerServiceTests : OpenSearchTestCase() {
    private lateinit var scriptService: ScriptService
    private lateinit var triggerService: TriggerService

    @Before
    fun setup() {
        scriptService = Mockito.mock(ScriptService::class.java)
        triggerService = TriggerService(scriptService)
    }

    fun `test run bucket level trigger with bucket key as int`() {
        val bucketSelectorExtAggregationBuilder = randomBucketSelectorExtAggregationBuilder(
            bucketsPathsMap = mutableMapOf("_count" to "_count", "_key" to "_key"),
            script = randomScript(source = "params._count > 0"),
            parentBucketPath = "status_code"
        )
        val trigger = randomBucketLevelTrigger(bucketSelector = bucketSelectorExtAggregationBuilder)
        val monitor = randomBucketLevelMonitor(triggers = listOf(trigger))

        val inputResultsStr = "{\n" +
            "  \"_shards\": {\n" +
            "    \"total\": 1,\n" +
            "    \"failed\": 0,\n" +
            "    \"successful\": 1,\n" +
            "    \"skipped\": 0\n" +
            "  },\n" +
            "  \"hits\": {\n" +
            "    \"hits\": [\n" +
            "      {\n" +
            "        \"_index\": \"sample-http-responses\",\n" +
            "        \"_type\": \"http\",\n" +
            "        \"_source\": {\n" +
            "          \"status_code\": 100,\n" +
            "          \"http_4xx\": 0,\n" +
            "          \"http_3xx\": 0,\n" +
            "          \"http_5xx\": 0,\n" +
            "          \"http_2xx\": 0,\n" +
            "          \"timestamp\": 100000,\n" +
            "          \"http_1xx\": 1\n" +
            "        },\n" +
            "        \"_id\": 1,\n" +
            "        \"_score\": 1\n" +
            "      }\n" +
            "    ],\n" +
            "    \"total\": {\n" +
            "      \"value\": 4,\n" +
            "      \"relation\": \"eq\"\n" +
            "    },\n" +
            "    \"max_score\": 1\n" +
            "  },\n" +
            "  \"took\": 37,\n" +
            "  \"timed_out\": false,\n" +
            "  \"aggregations\": {\n" +
            "    \"status_code\": {\n" +
            "      \"doc_count_error_upper_bound\": 0,\n" +
            "      \"sum_other_doc_count\": 0,\n" +
            "      \"buckets\": [\n" +
            "        {\n" +
            "          \"doc_count\": 2,\n" +
            "          \"key\": 100\n" +
            "        },\n" +
            "        {\n" +
            "          \"doc_count\": 1,\n" +
            "          \"key\": 102\n" +
            "        },\n" +
            "        {\n" +
            "          \"doc_count\": 1,\n" +
            "          \"key\": 201\n" +
            "        }\n" +
            "      ]\n" +
            "    },\n" +
            "    \"${trigger.id}\": {\n" +
            "      \"parent_bucket_path\": \"status_code\",\n" +
            "      \"bucket_indices\": [\n" +
            "        0,\n" +
            "        1,\n" +
            "        2\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}"

        val parser = XContentType.JSON.xContent()
            .createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                inputResultsStr
            )

        val inputResults = parser.map()

        var monitorRunResult = MonitorRunResult<BucketLevelTriggerRunResult>(monitor.name, Instant.now(), Instant.now())
        monitorRunResult = monitorRunResult.copy(inputResults = InputRunResults(listOf(inputResults)))
        val triggerCtx = BucketLevelTriggerExecutionContext(monitor, trigger, monitorRunResult)

        val bucketLevelTriggerRunResult = triggerService.runBucketLevelTrigger(monitor, trigger, triggerCtx)
        assertNull(bucketLevelTriggerRunResult.error)
    }

    fun `testBucketLevelTriggerWithMapKeyBucketSelector`() {
        val bucketSelectorExtAggregationBuilder = randomBucketSelectorExtAggregationBuilder(
            bucketsPathsMap = mutableMapOf("_count" to "_count", "_key" to "_key"),
            script = randomScript(source = "params._count > 0"),
            parentBucketPath = "status_code"
        )
        val trigger = randomBucketLevelTrigger(bucketSelector = bucketSelectorExtAggregationBuilder)
        val monitor = randomBucketLevelMonitor(triggers = listOf(trigger))

        val inputResultsStr = "{\n" +
            "  \"_shards\": {\n" +
            "    \"total\": 1,\n" +
            "    \"failed\": 0,\n" +
            "    \"successful\": 1,\n" +
            "    \"skipped\": 0\n" +
            "  },\n" +
            "  \"hits\": {\n" +
            "    \"hits\": [\n" +
            "      {\n" +
            "        \"_index\": \"sample-http-responses\",\n" +
            "        \"_type\": \"http\",\n" +
            "        \"_source\": {\n" +
            "          \"status_code\": 100,\n" +
            "          \"http_4xx\": 0,\n" +
            "          \"http_3xx\": 0,\n" +
            "          \"http_5xx\": 0,\n" +
            "          \"http_2xx\": 0,\n" +
            "          \"timestamp\": 100000,\n" +
            "          \"http_1xx\": 1\n" +
            "        },\n" +
            "        \"_id\": 1,\n" +
            "        \"_score\": 1\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"sample-http-responses\",\n" +
            "        \"_type\": \"http\",\n" +
            "        \"_source\": {\n" +
            "          \"status_code\": 102,\n" +
            "          \"http_4xx\": 0,\n" +
            "          \"http_3xx\": 0,\n" +
            "          \"http_5xx\": 0,\n" +
            "          \"http_2xx\": 0,\n" +
            "          \"timestamp\": 160000,\n" +
            "          \"http_1xx\": 1\n" +
            "        },\n" +
            "        \"_id\": 2,\n" +
            "        \"_score\": 1\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"sample-http-responses\",\n" +
            "        \"_type\": \"http\",\n" +
            "        \"_source\": {\n" +
            "          \"status_code\": 100,\n" +
            "          \"http_4xx\": 0,\n" +
            "          \"http_3xx\": 0,\n" +
            "          \"http_5xx\": 0,\n" +
            "          \"http_2xx\": 0,\n" +
            "          \"timestamp\": 220000,\n" +
            "          \"http_1xx\": 1\n" +
            "        },\n" +
            "        \"_id\": 4,\n" +
            "        \"_score\": 1\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"sample-http-responses\",\n" +
            "        \"_type\": \"http\",\n" +
            "        \"_source\": {\n" +
            "          \"status_code\": 201,\n" +
            "          \"http_4xx\": 0,\n" +
            "          \"http_3xx\": 0,\n" +
            "          \"http_5xx\": 0,\n" +
            "          \"http_2xx\": 1,\n" +
            "          \"timestamp\": 280000,\n" +
            "          \"http_1xx\": 0\n" +
            "        },\n" +
            "        \"_id\": 5,\n" +
            "        \"_score\": 1\n" +
            "      }\n" +
            "    ],\n" +
            "    \"total\": {\n" +
            "      \"value\": 4,\n" +
            "      \"relation\": \"eq\"\n" +
            "    },\n" +
            "    \"max_score\": 1\n" +
            "  },\n" +
            "  \"took\": 15,\n" +
            "  \"timed_out\": false,\n" +
            "  \"aggregations\": {\n" +
            "    \"${trigger.id}\": {\n" +
            "      \"parent_bucket_path\": \"status_code\",\n" +
            "      \"bucket_indices\": [\n" +
            "        0,\n" +
            "        1,\n" +
            "        2\n" +
            "      ]\n" +
            "    },\n" +
            "    \"status_code\": {\n" +
            "      \"buckets\": [\n" +
            "        {\n" +
            "          \"doc_count\": 2,\n" +
            "          \"key\": {\n" +
            "            \"status_code\": 100\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"doc_count\": 1,\n" +
            "          \"key\": {\n" +
            "            \"status_code\": 102\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"doc_count\": 1,\n" +
            "          \"key\": {\n" +
            "            \"status_code\": 201\n" +
            "          }\n" +
            "        }\n" +
            "      ],\n" +
            "      \"after_key\": {\n" +
            "        \"status_code\": 201\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}"

        val parser = XContentType.JSON.xContent()
            .createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                inputResultsStr
            )

        val inputResults = parser.map()

        var monitorRunResult = MonitorRunResult<BucketLevelTriggerRunResult>(monitor.name, Instant.now(), Instant.now())
        monitorRunResult = monitorRunResult.copy(inputResults = InputRunResults(listOf(inputResults)))
        val triggerCtx = BucketLevelTriggerExecutionContext(monitor, trigger, monitorRunResult)

        val bucketLevelTriggerRunResult = triggerService.runBucketLevelTrigger(monitor, trigger, triggerCtx)
        assertNull(bucketLevelTriggerRunResult.error)
    }
    fun `test run bucket level trigger with bucket  key as map`() {
        // create a bucket selector aggregation that groups results by a nested field using a map as the key
        val bucketSelectorExtAggregationBuilder = randomBucketSelectorExtAggregationBuilder(
            bucketsPathsMap = mutableMapOf(
                "nested_field" to "nested.field",
                "status_code" to "status.code"
            ),
            script = randomScript(source = "params._count > 0"),
            parentBucketPath = "status_code"
        )
        // create a bucket-level trigger that uses the bucket selector aggregation
        val trigger = randomBucketLevelTrigger(bucketSelector = bucketSelectorExtAggregationBuilder)
        // create a bucket-level monitor that includes the trigger
        val monitor = randomBucketLevelMonitor(triggers = listOf(trigger))

        // create input results that include a bucket with a map key
        val inputResultsStr = """
        {
            "_shards": {
                "total": 1,
                "failed": 0,
                "successful": 1,
                "skipped": 0
            },
            "aggregations": {
                "status_code": {
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0,
                    "buckets": [
                        {
                            "key": 404,
                            "doc_count": 1,
                            "nested_field": {
                                "field1": "value1",
                                "field2": "value2"
                            }
                        },
                        {
                            "key": 200,
                            "doc_count": 2,
                            "nested_field": {
                                "field1": "value3",
                                "field2": "value4"
                            }
                        }
                    ]
                },
                "${trigger.id}": {
                    "parent_bucket_path": "status_code",
                    "bucket_indices": [0, 1]
                }
            }
        }
        """.trimIndent()

        // parse the input results
        val parser = XContentType.JSON.xContent().createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, inputResultsStr
        )
        val inputResults = parser.map()

        // create a monitor run result using the input results
        var monitorRunResult = MonitorRunResult<BucketLevelTriggerRunResult>(monitor.name, Instant.now(), Instant.now())
        monitorRunResult = monitorRunResult.copy(inputResults = InputRunResults(listOf(inputResults)))

        // create a trigger context using the monitor, trigger, and monitor run result
        val triggerCtx = BucketLevelTriggerExecutionContext(monitor, trigger, monitorRunResult)

        // run the bucket-level trigger using the trigger service
        val bucketLevelTriggerRunResult = triggerService.runBucketLevelTrigger(monitor, trigger, triggerCtx)

        // verify that the bucket-level trigger run result has no error
        assertNull(bucketLevelTriggerRunResult.error)
    }
}
