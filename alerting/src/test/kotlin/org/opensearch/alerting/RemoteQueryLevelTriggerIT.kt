/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.index.query.QueryBuilders
import org.opensearch.script.Script
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

/**
 * Integration tests for query-level trigger evaluation with the multi-tenant trigger eval flag enabled.
 * These tests verify that trigger scripts are correctly evaluated remotely via filter aggregations
 * on the customer's cluster instead of locally via ScriptService.
 */
class RemoteQueryLevelTriggerIT : AlertingRestTestCase() {

    private val SETTING_KEY = AlertingSettings.MULTI_TENANT_TRIGGER_EVAL_ENABLED.key

    private fun enableRemoteTriggerEval() {
        client().updateSettings(SETTING_KEY, true)
    }

    private fun disableRemoteTriggerEval() {
        client().updateSettings(SETTING_KEY, false)
    }

    /**
     * Helper: create a test index with numeric and keyword fields, index docs, return index name.
     */
    private fun createIndexWithDocs(
        docCount: Int = 3,
        fieldName: String = "cpu",
        values: List<Int> = (1..docCount).toList()
    ): String {
        val index = createTestIndex(
            randomAlphaOfLength(10).lowercase(),
            """
                "properties": {
                    "test_strict_date_time": { "type": "date", "format": "strict_date_time" },
                    "test_field": { "type": "keyword" },
                    "$fieldName": { "type": "integer" }
                }
            """
        )
        val twoMinsAgo = ZonedDateTime.now().minus(2, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.MILLIS)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(twoMinsAgo)
        values.forEachIndexed { i, v ->
            indexDoc(index, (i + 1).toString(), """{ "test_strict_date_time": "$testTime", "test_field": "val_$i", "$fieldName": $v }""")
        }
        return index
    }

    private fun buildInput(index: String, aggs: SearchSourceBuilder.() -> Unit = {}): SearchInput {
        val query = QueryBuilders.rangeQuery("test_strict_date_time")
            .gt("{{period_end}}||-10d")
            .lte("{{period_end}}")
            .format("epoch_millis")
        val ssb = SearchSourceBuilder().query(query)
        ssb.aggs()
        return SearchInput(indices = listOf(index), query = ssb)
    }

    // ---- Tests ----

    fun `test multi tenant trigger eval simple threshold`() {
        enableRemoteTriggerEval()
        try {
            val index = createIndexWithDocs(docCount = 5)
            val input = buildInput(index)
            val triggerScript = "return ctx.results[0].hits.total.value > 3"
            val trigger = randomQueryLevelTrigger(condition = Script(triggerScript), actions = emptyList())
            val monitor = randomQueryLevelMonitor(inputs = listOf(input), triggers = listOf(trigger))

            val response = executeMonitor(monitor, params = DRYRUN_MONITOR)
            val output = entityAsMap(response)

            assertEquals(monitor.name, output["monitor_name"])
            val triggerResult = output.objectMap("trigger_results").objectMap(trigger.id)
            assertEquals(true, triggerResult["triggered"].toString().toBoolean())
            assertTrue("Unexpected error", triggerResult["error"]?.toString().isNullOrEmpty())
        } finally {
            disableRemoteTriggerEval()
        }
    }

    fun `test multi tenant trigger eval simple threshold not triggered`() {
        enableRemoteTriggerEval()
        try {
            val index = createIndexWithDocs(docCount = 2)
            val input = buildInput(index)
            val triggerScript = "return ctx.results[0].hits.total.value > 100"
            val trigger = randomQueryLevelTrigger(condition = Script(triggerScript), actions = emptyList())
            val monitor = randomQueryLevelMonitor(inputs = listOf(input), triggers = listOf(trigger))

            val response = executeMonitor(monitor, params = DRYRUN_MONITOR)
            val output = entityAsMap(response)

            val triggerResult = output.objectMap("trigger_results").objectMap(trigger.id)
            assertEquals(false, triggerResult["triggered"].toString().toBoolean())
            assertTrue("Unexpected error", triggerResult["error"]?.toString().isNullOrEmpty())
        } finally {
            disableRemoteTriggerEval()
        }
    }

    fun `test multi tenant trigger eval aggregation value`() {
        enableRemoteTriggerEval()
        try {
            val index = createIndexWithDocs(values = listOf(80, 90, 95))
            val input = buildInput(index) {
                aggregation(AggregationBuilders.avg("avg_cpu").field("cpu"))
            }
            val triggerScript = "return ctx.results[0].aggregations.avg_cpu.value > 80"
            val trigger = randomQueryLevelTrigger(condition = Script(triggerScript), actions = emptyList())
            val monitor = randomQueryLevelMonitor(inputs = listOf(input), triggers = listOf(trigger))

            val response = executeMonitor(monitor, params = DRYRUN_MONITOR)
            val output = entityAsMap(response)

            val triggerResult = output.objectMap("trigger_results").objectMap(trigger.id)
            assertEquals(true, triggerResult["triggered"].toString().toBoolean())
            assertTrue("Unexpected error", triggerResult["error"]?.toString().isNullOrEmpty())
        } finally {
            disableRemoteTriggerEval()
        }
    }

    fun `test multi tenant trigger eval boolean logic`() {
        enableRemoteTriggerEval()
        try {
            val index = createIndexWithDocs(docCount = 5, values = listOf(10, 20, 30, 40, 50))
            val input = buildInput(index) {
                aggregation(AggregationBuilders.avg("avg_cpu").field("cpu"))
            }
            // Both conditions true: total > 3 AND avg > 20
            val triggerScript = "return ctx.results[0].hits.total.value > 3 && ctx.results[0].aggregations.avg_cpu.value > 20"
            val trigger = randomQueryLevelTrigger(condition = Script(triggerScript), actions = emptyList())
            val monitor = randomQueryLevelMonitor(inputs = listOf(input), triggers = listOf(trigger))

            val response = executeMonitor(monitor, params = DRYRUN_MONITOR)
            val output = entityAsMap(response)

            val triggerResult = output.objectMap("trigger_results").objectMap(trigger.id)
            assertEquals(true, triggerResult["triggered"].toString().toBoolean())
        } finally {
            disableRemoteTriggerEval()
        }
    }

    fun `test multi tenant trigger eval boolean logic or`() {
        enableRemoteTriggerEval()
        try {
            val index = createIndexWithDocs(docCount = 2)
            val input = buildInput(index)
            // First condition false (total not > 100), second true (total > 0)
            val triggerScript = "return ctx.results[0].hits.total.value > 100 || ctx.results[0].hits.total.value > 0"
            val trigger = randomQueryLevelTrigger(condition = Script(triggerScript), actions = emptyList())
            val monitor = randomQueryLevelMonitor(inputs = listOf(input), triggers = listOf(trigger))

            val response = executeMonitor(monitor, params = DRYRUN_MONITOR)
            val output = entityAsMap(response)

            val triggerResult = output.objectMap("trigger_results").objectMap(trigger.id)
            assertEquals(true, triggerResult["triggered"].toString().toBoolean())
        } finally {
            disableRemoteTriggerEval()
        }
    }

    fun `test multi tenant trigger eval loop over hits`() {
        enableRemoteTriggerEval()
        try {
            val index = createIndexWithDocs(values = listOf(10, 95, 20))
            val input = buildInput(index)
            // Loop over hits checking if any cpu value > 90
            val triggerScript = """
                for (hit in ctx.results[0].hits.hits) {
                    if (hit._source.cpu > 90) {
                        return true;
                    }
                }
                return false;
            """.trimIndent()
            val trigger = randomQueryLevelTrigger(condition = Script(triggerScript), actions = emptyList())
            val monitor = randomQueryLevelMonitor(inputs = listOf(input), triggers = listOf(trigger))

            val response = executeMonitor(monitor, params = DRYRUN_MONITOR)
            val output = entityAsMap(response)

            val triggerResult = output.objectMap("trigger_results").objectMap(trigger.id)
            assertEquals(true, triggerResult["triggered"].toString().toBoolean())
        } finally {
            disableRemoteTriggerEval()
        }
    }

    fun `test multi tenant trigger eval multiple triggers`() {
        enableRemoteTriggerEval()
        try {
            val index = createIndexWithDocs(docCount = 5, values = listOf(10, 20, 30, 40, 50))
            val input = buildInput(index) {
                aggregation(AggregationBuilders.avg("avg_cpu").field("cpu"))
            }

            // Trigger 1: fires (total > 3)
            val trigger1 = randomQueryLevelTrigger(
                condition = Script("return ctx.results[0].hits.total.value > 3"),
                actions = emptyList()
            )
            // Trigger 2: does not fire (total > 100)
            val trigger2 = randomQueryLevelTrigger(
                condition = Script("return ctx.results[0].hits.total.value > 100"),
                actions = emptyList()
            )
            // Trigger 3: fires (avg > 20)
            val trigger3 = randomQueryLevelTrigger(
                condition = Script("return ctx.results[0].aggregations.avg_cpu.value > 20"),
                actions = emptyList()
            )

            val monitor = randomQueryLevelMonitor(inputs = listOf(input), triggers = listOf(trigger1, trigger2, trigger3))
            val response = executeMonitor(monitor, params = DRYRUN_MONITOR)
            val output = entityAsMap(response)

            val triggerResults = output.objectMap("trigger_results")
            assertEquals(true, triggerResults.objectMap(trigger1.id)["triggered"].toString().toBoolean())
            assertEquals(false, triggerResults.objectMap(trigger2.id)["triggered"].toString().toBoolean())
            assertEquals(true, triggerResults.objectMap(trigger3.id)["triggered"].toString().toBoolean())
        } finally {
            disableRemoteTriggerEval()
        }
    }

    fun `test multi tenant trigger eval script error`() {
        enableRemoteTriggerEval()
        try {
            val index = createIndexWithDocs(docCount = 1)
            val input = buildInput(index)
            // Malformed script — references a field that doesn't exist in the response
            val triggerScript = "return ctx.results[0].nonexistent.field.value > 0"
            val trigger = randomQueryLevelTrigger(condition = Script(triggerScript), actions = emptyList())
            val monitor = createMonitor(
                randomQueryLevelMonitor(inputs = listOf(input), triggers = listOf(trigger))
            )

            val response = executeMonitor(monitor.id)
            val output = entityAsMap(response)

            @Suppress("UNCHECKED_CAST")
            val triggerResult = (output.objectMap("trigger_results")[trigger.id] as Map<String, Any>)
            // On error, trigger defaults to false (fail-closed) with error surfaced
            assertEquals(false, triggerResult["triggered"].toString().toBoolean())
            assertFalse("Expected error to be set", triggerResult["error"]?.toString().isNullOrEmpty())
        } finally {
            disableRemoteTriggerEval()
        }
    }

    fun `test multi tenant trigger eval large response`() {
        enableRemoteTriggerEval()
        try {
            val index = createTestIndex(
                randomAlphaOfLength(10).lowercase(),
                """
                    "properties": {
                        "test_strict_date_time": { "type": "date", "format": "strict_date_time" },
                        "test_field": { "type": "keyword" },
                        "cpu": { "type": "integer" }
                    }
                """
            )
            val twoMinsAgo = ZonedDateTime.now().minus(2, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.MILLIS)
            val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(twoMinsAgo)
            // Index 50 docs to produce a non-trivial response
            for (i in 1..50) {
                indexDoc(index, i.toString(), """{ "test_strict_date_time": "$testTime", "test_field": "val_$i", "cpu": $i }""")
            }

            val input = buildInput(index)
            val triggerScript = "return ctx.results[0].hits.total.value > 40"
            val trigger = randomQueryLevelTrigger(condition = Script(triggerScript), actions = emptyList())
            val monitor = randomQueryLevelMonitor(inputs = listOf(input), triggers = listOf(trigger))

            val response = executeMonitor(monitor, params = DRYRUN_MONITOR)
            val output = entityAsMap(response)

            val triggerResult = output.objectMap("trigger_results").objectMap(trigger.id)
            assertEquals(true, triggerResult["triggered"].toString().toBoolean())
            assertTrue("Unexpected error", triggerResult["error"]?.toString().isNullOrEmpty())
        } finally {
            disableRemoteTriggerEval()
        }
    }

    fun `test multi tenant trigger eval dry run`() {
        enableRemoteTriggerEval()
        try {
            val index = createIndexWithDocs(docCount = 3)
            val input = buildInput(index)
            val triggerScript = "return ctx.results[0].hits.total.value > 0"
            val trigger = randomQueryLevelTrigger(condition = Script(triggerScript), actions = emptyList())
            val monitor = randomQueryLevelMonitor(inputs = listOf(input), triggers = listOf(trigger))

            val response = executeMonitor(monitor, params = DRYRUN_MONITOR)
            val output = entityAsMap(response)

            val triggerResult = output.objectMap("trigger_results").objectMap(trigger.id)
            assertEquals(true, triggerResult["triggered"].toString().toBoolean())

            // Dryrun should not persist alerts — monitor was never saved so no ID
            val alerts = searchAlerts(monitor)
            assertEquals("Alert should not be saved for dryrun", 0, alerts.size)
        } finally {
            disableRemoteTriggerEval()
        }
    }

    // ---- Regression tests (flag=false) ----

    fun `test query level trigger flag disabled`() {
        // Flag is false by default — do NOT enable it
        val index = createIndexWithDocs(docCount = 3)
        val input = buildInput(index)
        val triggerScript = "return ctx.results[0].hits.total.value > 0"
        val trigger = randomQueryLevelTrigger(condition = Script(triggerScript), actions = emptyList())
        val monitor = randomQueryLevelMonitor(inputs = listOf(input), triggers = listOf(trigger))

        val response = executeMonitor(monitor, params = DRYRUN_MONITOR)
        val output = entityAsMap(response)

        val triggerResult = output.objectMap("trigger_results").objectMap(trigger.id)
        assertEquals(true, triggerResult["triggered"].toString().toBoolean())
        assertTrue("Unexpected error", triggerResult["error"]?.toString().isNullOrEmpty())
    }

    @Suppress("UNCHECKED_CAST")
    fun `test query level trigger flag default is false`() {
        val settings = client().getSettings(includeDefaults = true)
        val defaults = settings["defaults"] as Map<String, Any>
        assertEquals("false", defaults[SETTING_KEY].toString())
    }

    fun `test query level trigger toggle flag during execution`() {
        val index = createIndexWithDocs(docCount = 3)
        val input = buildInput(index)
        val triggerScript = "return ctx.results[0].hits.total.value > 0"
        val trigger = randomQueryLevelTrigger(condition = Script(triggerScript), actions = emptyList())
        val monitor = createMonitor(
            randomQueryLevelMonitor(inputs = listOf(input), triggers = listOf(trigger))
        )

        try {
            // Execute with flag=false (ScriptService path)
            val response1 = executeMonitor(monitor.id)
            val output1 = entityAsMap(response1)
            val result1 = output1.objectMap("trigger_results").objectMap(trigger.id)
            assertEquals(true, result1["triggered"].toString().toBoolean())
            assertTrue("Unexpected error with flag off", result1["error"]?.toString().isNullOrEmpty())

            // Toggle flag to true (remote eval path)
            enableRemoteTriggerEval()
            val response2 = executeMonitor(monitor.id)
            val output2 = entityAsMap(response2)
            val result2 = output2.objectMap("trigger_results").objectMap(trigger.id)
            assertEquals(true, result2["triggered"].toString().toBoolean())
            assertTrue("Unexpected error with flag on", result2["error"]?.toString().isNullOrEmpty())

            // Toggle flag back to false (ScriptService path again)
            disableRemoteTriggerEval()
            val response3 = executeMonitor(monitor.id)
            val output3 = entityAsMap(response3)
            val result3 = output3.objectMap("trigger_results").objectMap(trigger.id)
            assertEquals(true, result3["triggered"].toString().toBoolean())
            assertTrue("Unexpected error after toggling back", result3["error"]?.toString().isNullOrEmpty())
        } finally {
            disableRemoteTriggerEval()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun Map<String, Any>.objectMap(key: String): Map<String, Map<String, Any>> {
        return this[key] as Map<String, Map<String, Any>>
    }
}
