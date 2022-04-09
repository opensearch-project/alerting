/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.core.model.DocLevelMonitorInput
import org.opensearch.alerting.core.model.DocLevelQuery
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.MILLIS

class DocumentMonitorRunnerIT : AlertingRestTestCase() {

    fun `test execute monitor with dryrun`() {

        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val index = createTestIndex()

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docReturningInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))

        val action = randomAction(template = randomTemplateScript("Hello {{ctx.monitor.name}}"), destinationId = createDestination().id)
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docReturningInput),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action)))
        )

        indexDoc(index, "1", testDoc)

        val response = executeMonitor(monitor, params = DRYRUN_MONITOR)

        val output = entityAsMap(response)
        assertEquals(monitor.name, output["monitor_name"])

        assertEquals(1, output.objectMap("trigger_results").values.size)

        for (triggerResult in output.objectMap("trigger_results").values) {
            assertEquals(1, triggerResult.objectMap("action_results").values.size)
            for (actionResult in triggerResult.objectMap("action_results").values) {
                @Suppress("UNCHECKED_CAST") val actionOutput = actionResult["output"] as Map<String, String>
                assertEquals("Hello ${monitor.name}", actionOutput["subject"])
                assertEquals("Hello ${monitor.name}", actionOutput["message"])
            }
        }

        val alerts = searchAlerts(monitor)
        assertEquals("Alert saved for test monitor", 0, alerts.size)
    }

    fun `test execute monitor returns search result with dryrun`() {
        val testIndex = createTestIndex()
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docReturningInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = randomDocumentLevelMonitor(inputs = listOf(docReturningInput), triggers = listOf(trigger))

        indexDoc(testIndex, "1", testDoc)
        indexDoc(testIndex, "5", testDoc)

        val response = executeMonitor(monitor, params = DRYRUN_MONITOR)

        val output = entityAsMap(response)

        assertEquals(monitor.name, output["monitor_name"])
        @Suppress("UNCHECKED_CAST")
        val searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        val matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 1, matchingDocsToQuery.size)
        assertTrue("Incorrect search result", matchingDocsToQuery.contains("5"))
    }

    fun `test execute monitor generates alerts and findings`() {
        putFindingMappings()
        val testIndex = createTestIndex()
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docReturningInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docReturningInput), triggers = listOf(trigger)))
        assertNotNull(monitor.id)

        indexDoc(testIndex, "1", testDoc)
        indexDoc(testIndex, "5", testDoc)

        val response = executeMonitor(monitor.id)

        val output = entityAsMap(response)

        assertEquals(monitor.name, output["monitor_name"])
        @Suppress("UNCHECKED_CAST")
        val searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        val matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 2, matchingDocsToQuery.size)
        assertTrue("Incorrect search result", matchingDocsToQuery.containsAll(listOf("1", "5")))

        val alerts = searchAlertsWithFilter(monitor)
        assertEquals("Alert saved for test monitor", 2, alerts.size)

        // TODO: modify findings such that there is a finding per document, so this test will need to be modified
        val findings = searchFindings(monitor)
        assertEquals("Findings saved for test monitor", 2, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
        assertTrue("Findings saved for test monitor", findings[1].relatedDocIds.contains("5"))
    }

    @Suppress("UNCHECKED_CAST")
    /** helper that returns a field in a json map whose values are all json objects */
    private fun Map<String, Any>.objectMap(key: String): Map<String, Map<String, Any>> {
        return this[key] as Map<String, Map<String, Any>>
    }
}
