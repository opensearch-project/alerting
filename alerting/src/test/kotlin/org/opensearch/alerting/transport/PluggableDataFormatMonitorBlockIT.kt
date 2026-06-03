/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.junit.Before
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.randomWorkflow
import org.opensearch.client.ResponseException
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.IntervalSchedule
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.PPLInput
import org.opensearch.commons.alerting.model.PPLTrigger
import org.opensearch.commons.alerting.model.QueryLevelTrigger
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.query.QueryBuilders
import org.opensearch.script.Script
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import java.time.Instant
import java.time.temporal.ChronoUnit

class PluggableDataFormatMonitorBlockIT : AlertingRestTestCase() {

    @Before
    fun checkPluggableDataformatEnabled() {
        assumeTrue(
            "Skipping — pluggable dataformat tests require feature flag enabled on cluster",
            System.getProperty("tests.pluggable_dataformat_enabled", "false").toBoolean()
        )
    }

    fun `test create doc-level monitor blocked on pluggable dataformat domain`() {
        val testIndex = createTestIndex()
        val monitor = Monitor(
            name = "test-doc-monitor",
            monitorType = Monitor.MonitorType.DOC_LEVEL_MONITOR.value,
            enabled = false,
            schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(),
            enabledTime = null,
            user = null,
            inputs = listOf(
                DocLevelMonitorInput(
                    "desc",
                    listOf(testIndex),
                    listOf(DocLevelQuery("query1", "query1", listOf(), """{"match_all": {}}"""))
                )
            ),
            triggers = emptyList(),
            uiMetadata = mapOf()
        )

        try {
            createMonitor(monitor)
            fail("Expected monitor creation to be blocked on pluggable dataformat domain")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
            val message = e.message ?: ""
            assertTrue(
                "Expected FORBIDDEN error for non-PPL monitor on pluggable dataformat domain, got: $message",
                message.contains("Monitor creation/update failed")
            )
        }
    }

    fun `test create PPL monitor not blocked by pluggable dataformat gate`() {
        val testIndex = createTestIndex()
        val monitor = Monitor(
            name = "test-ppl-monitor",
            monitorType = Monitor.MonitorType.PPL_MONITOR.value,
            enabled = false,
            schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(),
            enabledTime = null,
            user = null,
            inputs = listOf(
                PPLInput(
                    query = "source = $testIndex | head 10",
                    queryLanguage = PPLInput.QueryLanguage.PPL
                )
            ),
            triggers = listOf(
                PPLTrigger(
                    name = "test-trigger",
                    severity = "1",
                    actions = emptyList(),
                    conditionType = PPLTrigger.ConditionType.NUMBER_OF_RESULTS,
                    numResultsCondition = PPLTrigger.NumResultsCondition.GREATER_THAN,
                    numResultsValue = 0L,
                    customCondition = null
                )
            ),
            uiMetadata = mapOf()
        )

        try {
            createMonitor(monitor)
        } catch (e: Exception) {
            // PPL validation requires the SQL plugin which is not loaded in this test.
            // The important assertion is that the error is NOT from the pluggable dataformat gate.
            val message = e.message ?: ""
            assertFalse(
                "PPL monitor should not be blocked by pluggable dataformat gate, got: $message",
                message.contains("Monitor creation/update failed")
            )
        }
    }

    fun `test update query-level monitor blocked on pluggable dataformat domain`() {
        val testIndex = createTestIndex()
        val monitor = Monitor(
            name = "test-query-monitor",
            monitorType = Monitor.MonitorType.QUERY_LEVEL_MONITOR.value,
            enabled = false,
            schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(),
            enabledTime = null,
            user = null,
            inputs = listOf(
                SearchInput(
                    listOf(testIndex),
                    SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                )
            ),
            triggers = listOf(
                QueryLevelTrigger(
                    name = "trigger",
                    severity = "1",
                    condition = Script("return true"),
                    actions = emptyList()
                )
            ),
            uiMetadata = mapOf()
        )

        try {
            updateMonitor(monitor.copy(id = "some-monitor-id"))
            fail("Expected monitor update to be blocked on pluggable dataformat domain")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
            val message = e.message ?: ""
            assertTrue(
                "Expected FORBIDDEN error for non-PPL monitor update on pluggable dataformat domain, got: $message",
                message.contains("Monitor creation/update failed")
            )
        }
    }

    fun `test execute query-level monitor blocked on pluggable dataformat domain`() {
        val testIndex = createTestIndex()
        val monitor = Monitor(
            name = "test-query-monitor",
            monitorType = Monitor.MonitorType.QUERY_LEVEL_MONITOR.value,
            enabled = false,
            schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(),
            enabledTime = null,
            user = null,
            inputs = listOf(
                SearchInput(
                    listOf(testIndex),
                    SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                )
            ),
            triggers = listOf(
                QueryLevelTrigger(
                    name = "trigger",
                    severity = "1",
                    condition = Script("return true"),
                    actions = emptyList()
                )
            ),
            uiMetadata = mapOf()
        )

        try {
            executeMonitor(monitor, mapOf("dryrun" to "true"))
            fail("Expected monitor execution to be blocked on pluggable dataformat domain")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
            val message = e.message ?: ""
            assertTrue(
                "Expected FORBIDDEN error for non-PPL monitor execution on pluggable dataformat domain, got: $message",
                message.contains("Monitor execution failed")
            )
        }
    }

    fun `test execute PPL monitor not blocked by pluggable dataformat gate`() {
        val testIndex = createTestIndex()
        val monitor = Monitor(
            name = "test-ppl-monitor",
            monitorType = Monitor.MonitorType.PPL_MONITOR.value,
            enabled = false,
            schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(),
            enabledTime = null,
            user = null,
            inputs = listOf(
                PPLInput(
                    query = "source = $testIndex | head 10",
                    queryLanguage = PPLInput.QueryLanguage.PPL
                )
            ),
            triggers = listOf(
                PPLTrigger(
                    name = "test-trigger",
                    severity = "1",
                    actions = emptyList(),
                    conditionType = PPLTrigger.ConditionType.NUMBER_OF_RESULTS,
                    numResultsCondition = PPLTrigger.NumResultsCondition.GREATER_THAN,
                    numResultsValue = 0L,
                    customCondition = null
                )
            ),
            uiMetadata = mapOf()
        )

        try {
            executeMonitor(monitor, mapOf("dryrun" to "true"))
        } catch (e: Exception) {
            // PPL execution requires the SQL plugin which is not loaded in this test.
            // The important assertion is that the error is NOT from the pluggable dataformat gate.
            val message = e.message ?: ""
            assertFalse(
                "PPL monitor execution should not be blocked by pluggable dataformat gate, got: $message",
                message.contains("Monitor execution failed")
            )
        }
    }

    fun `test create bucket-level monitor blocked on pluggable dataformat domain`() {
        val testIndex = createTestIndex()
        val monitor = Monitor(
            name = "test-bucket-monitor",
            monitorType = Monitor.MonitorType.BUCKET_LEVEL_MONITOR.value,
            enabled = false,
            schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(),
            enabledTime = null,
            user = null,
            inputs = listOf(
                SearchInput(
                    listOf(testIndex),
                    SearchSourceBuilder()
                        .query(QueryBuilders.matchAllQuery())
                        .aggregation(
                            AggregationBuilders.terms("test_agg").field("test_field_1")
                        )
                )
            ),
            triggers = emptyList(),
            uiMetadata = mapOf()
        )

        try {
            createMonitor(monitor)
            fail("Expected monitor creation to be blocked on pluggable dataformat domain")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
            val message = e.message ?: ""
            assertTrue(
                "Expected FORBIDDEN error for non-PPL monitor, got: $message",
                message.contains("Monitor creation/update failed")
            )
        }
    }

    fun `test create cluster-metrics monitor not blocked on pluggable dataformat domain`() {
        val testIndex = createTestIndex()
        val monitor = Monitor(
            name = "test-cluster-metrics-monitor",
            monitorType = Monitor.MonitorType.CLUSTER_METRICS_MONITOR.value,
            enabled = false,
            schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(),
            enabledTime = null,
            user = null,
            inputs = listOf(
                SearchInput(
                    listOf(testIndex),
                    SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                )
            ),
            triggers = listOf(
                QueryLevelTrigger(
                    name = "trigger",
                    severity = "1",
                    condition = Script("return true"),
                    actions = emptyList()
                )
            ),
            uiMetadata = mapOf()
        )

        try {
            createMonitor(monitor)
        } catch (e: ResponseException) {
            val message = e.message ?: ""
            fail("Cluster metrics monitor creation should not fail, but got: $message")
        }
    }

    fun `test create workflow blocked on pluggable dataformat domain`() {
        try {
            createWorkflow(randomWorkflow(monitorIds = listOf("dummy-id")))
            fail("Expected workflow creation to be blocked on pluggable dataformat domain")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
            val message = e.message ?: ""
            assertTrue(
                "Expected FORBIDDEN error for workflow on pluggable dataformat domain, got: $message",
                message.contains("Monitor creation/update failed")
            )
        }
    }
}
