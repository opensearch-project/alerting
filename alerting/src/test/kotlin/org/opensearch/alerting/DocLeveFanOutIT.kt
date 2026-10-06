/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import org.apache.hc.core5.http.ContentType.APPLICATION_JSON
import org.apache.hc.core5.http.io.entity.StringEntity
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.action.ActionExecutionPolicy
import org.opensearch.commons.alerting.model.action.PerExecutionActionScope
import org.opensearch.test.OpenSearchTestCase
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.MILLIS
import java.util.concurrent.TimeUnit

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class DocLeveFanOutIT : AlertingRestTestCase() {

    fun `test execution reaches endtime before completing execution`() {
        val updateSettings1 = adminClient().updateSettings(AlertingSettings.FINDING_HISTORY_ENABLED.key, false)
        logger.info(updateSettings1)
        val testIndex = createTestIndex()
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))

        val actionExecutionScope = PerExecutionActionScope()
        val actionExecutionPolicy = ActionExecutionPolicy(actionExecutionScope)
        val actions = (0..randomInt(10)).map {
            randomActionWithPolicy(
                template = randomTemplateScript("Hello {{ctx.monitor.name}}"),
                destinationId = createDestination().id,
                actionExecutionPolicy = actionExecutionPolicy
            )
        }

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = actions)

        val monitor = createMonitor(
            randomDocumentLevelMonitor(
                inputs = listOf(docLevelInput),
                triggers = listOf(trigger)
            )
        )
        assertNotNull(monitor.id)
        executeMonitor(monitor.id)
        indexDoc(testIndex, "1", testDoc)
        indexDoc(testIndex, "2", testDoc)

        var response = executeMonitor(monitor.id)

        var output = entityAsMap(response)
        val findings1 = searchFindings(monitor)
        val findingsSize1 = findings1.size
        assertEquals(findingsSize1, 2)
        adminClient().updateSettings(AlertingSettings.DOC_LEVEL_MONITOR_EXECUTION_MAX_DURATION.key, TimeValue.timeValueNanos(1))
        executeMonitor(monitor.id)
        OpenSearchTestCase.waitUntil({
            return@waitUntil true
        }, 2, TimeUnit.SECONDS)
        adminClient().updateSettings(AlertingSettings.DOC_LEVEL_MONITOR_EXECUTION_MAX_DURATION.key, TimeValue.timeValueMinutes(4))
        indexDoc(testIndex, "3", testDoc)
        indexDoc(testIndex, "4", testDoc)
        executeMonitor(monitor.id)
        val findings = searchFindings(monitor)
        val findingsSize = findings.size
        assertEquals(findingsSize, 4)
    }

    fun `test fan-out nodes do not overwrite each other's last run context`() {
        val testIndex = createTestIndex(settings = shardedIndexSettings())
        val monitor = createUsWest2Monitor(testIndex)

        // Every run, each fan-out node reports the shards it read. If a node could also report the shards it did not
        // read, the run's last run context would lose another node's progress and the next run would read those
        // documents again.
        var indexed = 0
        repeat(4) {
            bulkIndexUsWest2Docs(testIndex, "r$it", 30)
            indexed += 30
            executeMonitor(monitor.id)
            val docIds = findingDocIds(monitor)
            assertEquals(indexed, docIds.size)
            assertEquals(indexed, docIds.toSet().size)
        }
    }

    fun `test doc level monitor finds new documents across fan-out nodes after its index is recreated`() {
        val testIndex = createTestIndex(settings = shardedIndexSettings())
        val monitor = createUsWest2Monitor(testIndex)
        bulkIndexUsWest2Docs(testIndex, "a", 600)
        executeMonitor(monitor.id)
        assertEquals(600, findingDocIds(monitor).size)

        // Every shard of the recreated index ends below its saved seq_no, so each node resets its shards to their
        // current end, skipping the documents already there. No node's response may undo another node's reset.
        deleteIndex(testIndex)
        createTestIndex(testIndex, shardedIndexSettings())
        bulkIndexUsWest2Docs(testIndex, "b", 120)
        executeMonitor(monitor.id)
        assertTrue(findingDocIds(monitor).none { it.startsWith("b") })

        bulkIndexUsWest2Docs(testIndex, "c", 12)
        executeMonitor(monitor.id)
        val newFindings = findingDocIds(monitor).filter { it.startsWith("c") }
        assertEquals((0 until 12).map { "c$it" }.sorted(), newFindings.sorted())
    }

    private fun shardedIndexSettings(): Settings =
        Settings.builder().put("index.number_of_shards", 6).put("index.number_of_replicas", 0).build()

    private fun createUsWest2Monitor(testIndex: String): Monitor {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        return createMonitor(
            randomDocumentLevelMonitor(
                inputs = listOf(DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))),
                triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN))
            )
        )
    }

    /** Related doc ids of all the monitor's findings, one entry per finding. */
    private fun findingDocIds(monitor: Monitor): List<String> {
        refreshIndex(AlertIndices.ALL_FINDING_INDEX_PATTERN)
        val request = """{ "size": 1000, "query": { "term": { "monitor_id": "${monitor.id}" } } }"""
        val response = adminClient().makeRequest(
            "GET", "${AlertIndices.ALL_FINDING_INDEX_PATTERN}/_search", StringEntity(request, APPLICATION_JSON)
        )
        @Suppress("UNCHECKED_CAST")
        val hits = (entityAsMap(response)["hits"] as Map<String, Any>)["hits"] as List<Map<String, Any>>
        @Suppress("UNCHECKED_CAST")
        return hits.flatMap { (it["_source"] as Map<String, Any>)["related_doc_ids"] as List<String> }
    }

    private fun bulkIndexUsWest2Docs(testIndex: String, idPrefix: String, count: Int) {
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val body = (0 until count).joinToString("\n", postfix = "\n") {
            """{ "index" : { "_index" : "$testIndex", "_id" : "$idPrefix$it" } }""" + "\n" +
                """{ "test_strict_date_time" : "$testTime", "test_field" : "us-west-2" }"""
        }
        val response = client().makeRequest("POST", "_bulk", mapOf("refresh" to "true"), StringEntity(body, APPLICATION_JSON))
        assertFalse(entityAsMap(response)["errors"] as Boolean)
    }
}
