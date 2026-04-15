/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.commons.alerting.aggregation.bucketselectorext.BucketSelectorExtAggregationBuilder
import org.opensearch.commons.alerting.model.Alert.State.ACTIVE
import org.opensearch.commons.alerting.model.Alert.State.COMPLETED
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.index.query.QueryBuilders
import org.opensearch.script.Script
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder
import org.opensearch.search.builder.SearchSourceBuilder

/**
 * Regression tests verifying that bucket-level monitors work correctly when
 * multi_tenant_trigger_eval_enabled is false (the default). The existing
 * BucketSelectorExt path must be completely unchanged.
 */
class RemoteBucketLevelTriggerRegressionIT : AlertingRestTestCase() {

    private val SETTING_KEY = AlertingSettings.MULTI_TENANT_TRIGGER_EVAL_ENABLED.key

    private fun buildCompositeInput(index: String): SearchInput {
        val query = QueryBuilders.rangeQuery("test_strict_date_time")
            .gt("{{period_end}}||-10d")
            .lte("{{period_end}}")
            .format("epoch_millis")
        val compositeSources = listOf(TermsValuesSourceBuilder("test_field").field("test_field"))
        val compositeAgg = CompositeAggregationBuilder("composite_agg", compositeSources)
        return SearchInput(indices = listOf(index), query = SearchSourceBuilder().size(0).query(query).aggregation(compositeAgg))
    }

    private fun buildTrigger(script: String = "params.docCount > 0"): org.opensearch.commons.alerting.model.BucketLevelTrigger {
        var trigger = randomBucketLevelTrigger()
        trigger = trigger.copy(
            bucketSelector = BucketSelectorExtAggregationBuilder(
                name = trigger.id,
                bucketsPathsMap = mapOf("docCount" to "_count"),
                script = Script(script),
                parentBucketPath = "composite_agg",
                filter = null
            )
        )
        return trigger
    }

    fun `test bucket level trigger flag disabled`() {
        // Explicitly disable — should use BucketSelectorExt path
        client().updateSettings(SETTING_KEY, false)
        try {
            val testIndex = createTestIndex()
            insertSampleTimeSerializedData(testIndex, listOf("test_value_1", "test_value_1", "test_value_2"))

            val input = buildCompositeInput(testIndex)
            val trigger = buildTrigger()
            val monitor = createMonitor(randomBucketLevelMonitor(inputs = listOf(input), enabled = false, triggers = listOf(trigger)))

            executeMonitor(monitor.id)
            val alerts = searchAlerts(monitor)
            assertEquals("Alerts not saved", 2, alerts.size)
            alerts.forEach { assertEquals(ACTIVE, it.state) }
        } finally {
            client().updateSettings(SETTING_KEY, false)
        }
    }

    fun `test bucket level trigger flag default is false`() {
        // Don't set the flag at all — verify default is false
        val settings = getClusterSettings()
        val flagValue = settings?.get(SETTING_KEY)
        assertTrue("Flag should default to false or be absent", flagValue == null || flagValue == "false")
    }

    fun `test bucket level trigger alert lifecycle flag disabled`() {
        client().updateSettings(SETTING_KEY, false)
        try {
            val testIndex = createTestIndex()
            insertSampleTimeSerializedData(testIndex, listOf("test_value_1", "test_value_1", "test_value_2"))

            val input = buildCompositeInput(testIndex)
            val trigger = buildTrigger()
            val monitor = createMonitor(randomBucketLevelMonitor(inputs = listOf(input), enabled = false, triggers = listOf(trigger)))

            // First execution — alerts created
            executeMonitor(monitor.id)
            var alerts = searchAlerts(monitor)
            assertEquals(2, alerts.size)
            alerts.forEach { assertEquals(ACTIVE, it.state) }

            // Delete docs for one bucket
            deleteDataWithDocIds(testIndex, listOf("1", "2"))

            // Second execution — one completed
            executeMonitor(monitor.id)
            alerts = searchAlerts(monitor, AlertIndices.ALL_ALERT_INDEX_PATTERN)
            val activeAlerts = alerts.filter { it.state == ACTIVE }
            val completedAlerts = alerts.filter { it.state == COMPLETED }
            assertEquals(1, activeAlerts.size)
            assertEquals(1, completedAlerts.size)
        } finally {
            client().updateSettings(SETTING_KEY, false)
        }
    }

    fun `test bucket level trigger toggle flag during execution`() {
        try {
            val testIndex = createTestIndex()
            insertSampleTimeSerializedData(testIndex, listOf("test_value_1", "test_value_1", "test_value_2"))

            val input = buildCompositeInput(testIndex)
            val trigger = buildTrigger()
            val monitor = createMonitor(randomBucketLevelMonitor(inputs = listOf(input), enabled = false, triggers = listOf(trigger)))

            // Execute with flag=false (BucketSelectorExt path)
            client().updateSettings(SETTING_KEY, false)
            val response1 = executeMonitor(monitor.id, params = DRYRUN_MONITOR)
            val output1 = entityAsMap(response1)
            @Suppress("UNCHECKED_CAST")
            val buckets1 = output1.objectMap("trigger_results").objectMap(trigger.id)["agg_result_buckets"] as Map<String, Any>
            assertEquals(2, buckets1.size)

            // Toggle to true (standard bucket_selector path)
            client().updateSettings(SETTING_KEY, true)
            val response2 = executeMonitor(monitor.id, params = DRYRUN_MONITOR)
            val output2 = entityAsMap(response2)
            @Suppress("UNCHECKED_CAST")
            val buckets2 = output2.objectMap("trigger_results").objectMap(trigger.id)["agg_result_buckets"] as Map<String, Any>
            assertEquals(2, buckets2.size)
        } finally {
            client().updateSettings(SETTING_KEY, false)
        }
    }

    fun `test bucket level trigger toggle flag single trigger`() {
        try {
            val testIndex = createTestIndex()
            insertSampleTimeSerializedData(testIndex, listOf("test_value_1", "test_value_1", "test_value_2"))

            val input = buildCompositeInput(testIndex)
            val trigger = buildTrigger(script = "params.docCount > 1") // only test_value_1
            val monitor = createMonitor(
                randomBucketLevelMonitor(inputs = listOf(input), enabled = false, triggers = listOf(trigger))
            )

            // Execute with flag=false (BucketSelectorExt path)
            client().updateSettings(SETTING_KEY, false)
            val response1 = executeMonitor(monitor.id, params = DRYRUN_MONITOR)
            val output1 = entityAsMap(response1)
            @Suppress("UNCHECKED_CAST")
            val buckets1 = output1.objectMap("trigger_results").objectMap(trigger.id)["agg_result_buckets"] as Map<String, Any>
            assertEquals("Flag off: should match 1 bucket", 1, buckets1.size)

            // Toggle to true (standard bucket_selector path)
            client().updateSettings(SETTING_KEY, true)
            val response2 = executeMonitor(monitor.id, params = DRYRUN_MONITOR)
            val output2 = entityAsMap(response2)
            @Suppress("UNCHECKED_CAST")
            val buckets2 = output2.objectMap("trigger_results").objectMap(trigger.id)["agg_result_buckets"] as Map<String, Any>
            assertEquals("Flag on: should match 1 bucket", 1, buckets2.size)
        } finally {
            client().updateSettings(SETTING_KEY, false)
        }
    }

    private fun getClusterSettings(): Map<String, String>? {
        val response = client().performRequest(org.opensearch.client.Request("GET", "/_cluster/settings?flat_settings=true"))
        @Suppress("UNCHECKED_CAST")
        val settings = entityAsMap(response)["defaults"] as? Map<String, String>
        return settings
    }

    @Suppress("UNCHECKED_CAST")
    private fun Map<String, Any>.objectMap(key: String): Map<String, Map<String, Any>> {
        return this[key] as Map<String, Map<String, Any>>
    }
}
