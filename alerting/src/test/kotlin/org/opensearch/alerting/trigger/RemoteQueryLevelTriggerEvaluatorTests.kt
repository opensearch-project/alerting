/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.trigger

import org.opensearch.alerting.trigger.RemoteQueryLevelTriggerEvaluator.TriggerData
import org.opensearch.test.OpenSearchTestCase

class RemoteQueryLevelTriggerEvaluatorTests : OpenSearchTestCase() {

    fun `test build eval request with single trigger`() {
        val triggers = listOf(
            TriggerData(
                id = "trigger-1",
                name = "test-trigger",
                script = "ctx.results[0].hits.total.value > 0"
            )
        )
        val searchResponse = mapOf("hits" to mapOf("total" to mapOf("value" to 5)))
        val request = RemoteQueryLevelTriggerEvaluator.buildEvalSearchSource(triggers, searchResponse)

        assertNotNull(request)
        val requestString = request.toString()
        assertTrue("Should contain trigger agg", requestString.contains("_query_trigger_trigger-1"))
        assertTrue("Should contain rewritten script", requestString.contains("params.results_0"))
    }

    fun `test build eval request with multiple triggers`() {
        val triggers = listOf(
            TriggerData(id = "t1", name = "trigger-1", script = "ctx.results[0].hits.total.value > 0"),
            TriggerData(id = "t2", name = "trigger-2", script = "ctx.results[0].hits.total.value > 100"),
            TriggerData(id = "t3", name = "trigger-3", script = "ctx.results[0].aggregations.avg_val.value > 50")
        )
        val searchResponse = mapOf("hits" to mapOf("total" to mapOf("value" to 5)))
        val request = RemoteQueryLevelTriggerEvaluator.buildEvalSearchSource(triggers, searchResponse)

        val requestString = request.toString()
        assertTrue("Should contain t1 agg", requestString.contains("_query_trigger_t1"))
        assertTrue("Should contain t2 agg", requestString.contains("_query_trigger_t2"))
        assertTrue("Should contain t3 agg", requestString.contains("_query_trigger_t3"))
    }

    fun `test parse eval response triggered`() {
        val triggerIds = listOf("trigger-1")
        val aggResults = mapOf(
            "_query_trigger_trigger-1" to mapOf<String, Any>("doc_count" to 5)
        )
        val results = RemoteQueryLevelTriggerEvaluator.parseEvalResponse(triggerIds, aggResults)

        assertTrue("trigger-1 should be triggered", results["trigger-1"]!!)
    }

    fun `test parse eval response not triggered`() {
        val triggerIds = listOf("trigger-1")
        val aggResults = mapOf(
            "_query_trigger_trigger-1" to mapOf<String, Any>("doc_count" to 0)
        )
        val results = RemoteQueryLevelTriggerEvaluator.parseEvalResponse(triggerIds, aggResults)

        assertFalse("trigger-1 should not be triggered", results["trigger-1"]!!)
    }

    fun `test parse eval response multiple triggers mixed`() {
        val triggerIds = listOf("t1", "t2")
        val aggResults = mapOf(
            "_query_trigger_t1" to mapOf<String, Any>("doc_count" to 3),
            "_query_trigger_t2" to mapOf<String, Any>("doc_count" to 0)
        )
        val results = RemoteQueryLevelTriggerEvaluator.parseEvalResponse(triggerIds, aggResults)

        assertTrue("t1 should be triggered", results["t1"]!!)
        assertFalse("t2 should not be triggered", results["t2"]!!)
    }

    fun `test parse eval response missing trigger defaults to triggered`() {
        val triggerIds = listOf("trigger-1")
        val aggResults = emptyMap<String, Map<String, Any>>()
        val results = RemoteQueryLevelTriggerEvaluator.parseEvalResponse(triggerIds, aggResults)

        assertTrue("Missing trigger should default to triggered (fail-open)", results["trigger-1"]!!)
    }
}
