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

        val inputResultsStr = "{\"_shards\":{\"total\":1,\"failed\":0,\"successful\":1,\"skipped\":0},\"hits\":{\"hits\":[{\"_index\":\"sample-http-responses\",\"_type\":\"http\",\"_source\":{\"status_code\":100,\"http_4xx\":0,\"http_3xx\":0,\"http_5xx\":0,\"http_2xx\":0,\"timestamp\":100000,\"http_1xx\":1},\"_id\":1,\"_score\":1}],\"total\":{\"value\":4,\"relation\":\"eq\"},\"max_score\":1},\"took\":37,\"timed_out\":false,\"aggregations\":{\"status_code\":{\"doc_count_error_upper_bound\":0,\"sum_other_doc_count\":0,\"buckets\":[{\"doc_count\":2,\"key\":100},{\"doc_count\":1,\"key\":102},{\"doc_count\":1,\"key\":201}]},\"${trigger.id}\":{\"parent_bucket_path\":\"status_code\",\"bucket_indices\":[0,1,2]}}}"

        val parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, inputResultsStr)

        val inputResults = parser.map()

        var monitorRunResult = MonitorRunResult<BucketLevelTriggerRunResult>(monitor.name, Instant.now(), Instant.now())
        monitorRunResult = monitorRunResult.copy(inputResults = InputRunResults(listOf(inputResults)))
        val triggerCtx = BucketLevelTriggerExecutionContext(monitor, trigger, monitorRunResult)

        val bucketLevelTriggerRunResult = triggerService.runBucketLevelTrigger(monitor, trigger, triggerCtx)
        assertNull(bucketLevelTriggerRunResult.error)
    }

    fun `test run bucket level trigger with bucket key as map`() {
        val bucketSelectorExtAggregationBuilder = randomBucketSelectorExtAggregationBuilder(
            bucketsPathsMap = mutableMapOf("_count" to "_count", "_key" to "_key"),
            script = randomScript(source = "params._count > 0"),
            parentBucketPath = "status_code"
        )
        val trigger = randomBucketLevelTrigger(bucketSelector = bucketSelectorExtAggregationBuilder)
        val monitor = randomBucketLevelMonitor(triggers = listOf(trigger))

        val inputResultsStr = "{\"_shards\":{\"total\":1, \"failed\":0, \"successful\":1, \"skipped\":0}, \"hits\":{\"hits\":[{\"_index\":\"sample-http-responses\", \"_type\":\"http\", \"_source\":{\"status_code\":100, \"http_4xx\":0, \"http_3xx\":0, \"http_5xx\":0, \"http_2xx\":0, \"timestamp\":100000, \"http_1xx\":1}, \"_id\":1, \"_score\":1.0}, {\"_index\":\"sample-http-responses\", \"_type\":\"http\", \"_source\":{\"status_code\":102, \"http_4xx\":0, \"http_3xx\":0, \"http_5xx\":0, \"http_2xx\":0, \"timestamp\":160000, \"http_1xx\":1}, \"_id\":2, \"_score\":1.0}, {\"_index\":\"sample-http-responses\", \"_type\":\"http\", \"_source\":{\"status_code\":100, \"http_4xx\":0, \"http_3xx\":0, \"http_5xx\":0, \"http_2xx\":0, \"timestamp\":220000, \"http_1xx\":1}, \"_id\":4, \"_score\":1.0}, {\"_index\":\"sample-http-responses\", \"_type\":\"http\", \"_source\":{\"status_code\":201, \"http_4xx\":0, \"http_3xx\":0, \"http_5xx\":0, \"http_2xx\":1, \"timestamp\":280000, \"http_1xx\":0}, \"_id\":5, \"_score\":1.0}], \"total\":{\"value\":4, \"relation\":\"eq\"}, \"max_score\":1.0}, \"took\":15, \"timed_out\":false, \"aggregations\":{\"${trigger.id}\":{\"parent_bucket_path\":\"status_code\", \"bucket_indices\":[0, 1, 2]}, \"status_code\":{\"buckets\":[{\"doc_count\":2, \"key\":{\"status_code\":100}}, {\"doc_count\":1, \"key\":{\"status_code\":102}}, {\"doc_count\":1, \"key\":{\"status_code\":201}}], \"after_key\":{\"status_code\":201}}}}"

        val parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, inputResultsStr)

        val inputResults = parser.map()

        var monitorRunResult = MonitorRunResult<BucketLevelTriggerRunResult>(monitor.name, Instant.now(), Instant.now())
        monitorRunResult = monitorRunResult.copy(inputResults = InputRunResults(listOf(inputResults)))
        val triggerCtx = BucketLevelTriggerExecutionContext(monitor, trigger, monitorRunResult)

        val bucketLevelTriggerRunResult = triggerService.runBucketLevelTrigger(monitor, trigger, triggerCtx)
        assertNull(bucketLevelTriggerRunResult.error)
    }
}
