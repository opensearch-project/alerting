/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.alerts

import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.ALWAYS_RUN
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.NEVER_RUN
import org.opensearch.alerting.core.model.DocLevelMonitorInput
import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.randomDocLevelMonitorInput
import org.opensearch.alerting.randomDocLevelTrigger
import org.opensearch.alerting.randomDocumentLevelMonitor
import org.opensearch.alerting.randomDocumentLevelTrigger
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.alerting.randomQueryLevelTrigger
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.xcontent.json.JsonXContent.jsonXContent
import org.opensearch.rest.RestStatus

class AlertIndicesIT : AlertingRestTestCase() {

    fun `test create alert index`() {
        executeMonitor(randomQueryLevelMonitor(triggers = listOf(randomQueryLevelTrigger(condition = ALWAYS_RUN))))

        assertIndexExists(AlertIndices.ALERT_INDEX)
        assertIndexExists(AlertIndices.ALERT_HISTORY_WRITE_INDEX)
    }

    fun `test create finding index`() {
        executeMonitor(randomDocumentLevelMonitor(triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN))))

        assertIndexExists(AlertIndices.FINDING_HISTORY_WRITE_INDEX)
    }

    fun `test update alert index mapping with new schema version`() {
        wipeAllODFEIndices()
        assertIndexDoesNotExist(AlertIndices.ALERT_INDEX)
        assertIndexDoesNotExist(AlertIndices.ALERT_HISTORY_WRITE_INDEX)

        putAlertMappings(
            AlertIndices.alertMapping().trimStart('{').trimEnd('}')
                .replace("\"schema_version\": 3", "\"schema_version\": 0")
        )
        assertIndexExists(AlertIndices.ALERT_INDEX)
        assertIndexExists(AlertIndices.ALERT_HISTORY_WRITE_INDEX)
        verifyIndexSchemaVersion(AlertIndices.ALERT_INDEX, 0)
        verifyIndexSchemaVersion(AlertIndices.ALERT_HISTORY_WRITE_INDEX, 0)
        wipeAllODFEIndices()
        executeMonitor(createRandomMonitor())
        assertIndexExists(AlertIndices.ALERT_INDEX)
        assertIndexExists(AlertIndices.ALERT_HISTORY_WRITE_INDEX)
        verifyIndexSchemaVersion(ScheduledJob.SCHEDULED_JOBS_INDEX, 5)
        verifyIndexSchemaVersion(AlertIndices.ALERT_INDEX, 3)
        verifyIndexSchemaVersion(AlertIndices.ALERT_HISTORY_WRITE_INDEX, 3)
    }

    fun `test update finding index mapping with new schema version`() {
        wipeAllODFEIndices()
        assertIndexDoesNotExist(AlertIndices.FINDING_HISTORY_WRITE_INDEX)

        putFindingMappings(
            AlertIndices.findingMapping().trimStart('{').trimEnd('}')
                .replace("\"schema_version\": 1", "\"schema_version\": 0")
        )
        assertIndexExists(AlertIndices.FINDING_HISTORY_WRITE_INDEX)
        verifyIndexSchemaVersion(AlertIndices.FINDING_HISTORY_WRITE_INDEX, 0)
        wipeAllODFEIndices()

        val testIndex = createTestIndex()
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docReturningInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val trueMonitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docReturningInput), triggers = listOf(trigger)))
        executeMonitor(trueMonitor.id)
        assertIndexExists(AlertIndices.FINDING_HISTORY_WRITE_INDEX)
        verifyIndexSchemaVersion(ScheduledJob.SCHEDULED_JOBS_INDEX, 5)
        verifyIndexSchemaVersion(AlertIndices.FINDING_HISTORY_WRITE_INDEX, 1)
    }

    fun `test alert index gets recreated automatically if deleted`() {
        wipeAllODFEIndices()
        assertIndexDoesNotExist(AlertIndices.ALERT_INDEX)
        val trueMonitor = randomQueryLevelMonitor(triggers = listOf(randomQueryLevelTrigger(condition = ALWAYS_RUN)))

        executeMonitor(trueMonitor)
        assertIndexExists(AlertIndices.ALERT_INDEX)
        assertIndexExists(AlertIndices.ALERT_HISTORY_WRITE_INDEX)
        wipeAllODFEIndices()
        assertIndexDoesNotExist(AlertIndices.ALERT_INDEX)
        assertIndexDoesNotExist(AlertIndices.ALERT_HISTORY_WRITE_INDEX)

        val executeResponse = executeMonitor(trueMonitor)
        val xcp = createParser(XContentType.JSON.xContent(), executeResponse.entity.content)
        val output = xcp.map()
        assertNull("Error running a monitor after wiping alert indices", output["error"])
    }

    fun `test finding index gets recreated automatically if deleted`() {
        wipeAllODFEIndices()
        assertIndexDoesNotExist(AlertIndices.FINDING_HISTORY_WRITE_INDEX)
        val testIndex = createTestIndex()
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docReturningInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val trueMonitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docReturningInput), triggers = listOf(trigger)))

        executeMonitor(trueMonitor.id)
        assertIndexExists(AlertIndices.FINDING_HISTORY_WRITE_INDEX)
        wipeAllODFEIndices()
        assertIndexDoesNotExist(AlertIndices.FINDING_HISTORY_WRITE_INDEX)

        createTestIndex(testIndex)
        val executeResponse = executeMonitor(trueMonitor)
        val xcp = createParser(XContentType.JSON.xContent(), executeResponse.entity.content)
        val output = xcp.map()
        assertNull("Error running a monitor after wiping finding indices", output["error"])
    }

    fun `test rollover alert history index`() {
        // Update the rollover check to be every 1 second and the index max age to be 1 second
        client().updateSettings(AlertingSettings.ALERT_HISTORY_ROLLOVER_PERIOD.key, "1s")
        client().updateSettings(AlertingSettings.ALERT_HISTORY_INDEX_MAX_AGE.key, "1s")

        val trueMonitor = randomQueryLevelMonitor(triggers = listOf(randomQueryLevelTrigger(condition = ALWAYS_RUN)))
        executeMonitor(trueMonitor)

        // Allow for a rollover index.
        Thread.sleep(2000)
        assertTrue("Did not find 3 alert indices", getAlertIndices().size >= 3)
    }

    fun `test rollover finding history index`() {
        // Update the rollover check to be every 1 second and the index max age to be 1 second
        client().updateSettings(AlertingSettings.FINDING_HISTORY_ROLLOVER_PERIOD.key, "1s")
        client().updateSettings(AlertingSettings.FINDING_HISTORY_INDEX_MAX_AGE.key, "1s")

        val trueMonitor = randomDocumentLevelMonitor(
            triggers = listOf(randomDocLevelTrigger(condition = ALWAYS_RUN)),
            inputs = listOf(randomDocLevelMonitorInput())
        )
        executeMonitor(trueMonitor)

        // Allow for a rollover index.
        Thread.sleep(2000)
        assertTrue("Did not find 2 alert indices", getFindingIndices().size >= 2)
    }

    fun `test alert history disabled`() {
        resetHistorySettings()

        val trigger1 = randomQueryLevelTrigger(condition = ALWAYS_RUN)
        val monitor1 = createMonitor(randomQueryLevelMonitor(triggers = listOf(trigger1)))
        executeMonitor(monitor1.id)

        // Check if alert is active
        val activeAlert1 = searchAlerts(monitor1)
        assertEquals("1 alert should be active", 1, activeAlert1.size)

        // Change trigger and re-execute monitor to mark alert as COMPLETED
        updateMonitor(monitor1.copy(triggers = listOf(trigger1.copy(condition = NEVER_RUN)), id = monitor1.id), true)
        executeMonitor(monitor1.id)

        val completedAlert1 = searchAlerts(monitor1, AlertIndices.ALL_ALERT_INDEX_PATTERN).single()
        assertNotNull("Alert is not completed", completedAlert1.endTime)

        assertEquals(1, getAlertHistoryDocCount())

        // Disable alert history
        client().updateSettings(AlertingSettings.ALERT_HISTORY_ENABLED.key, "false")

        val trigger2 = randomQueryLevelTrigger(condition = ALWAYS_RUN)
        val monitor2 = createMonitor(randomQueryLevelMonitor(triggers = listOf(trigger2)))
        executeMonitor(monitor2.id)

        // Check if second alert is active
        val activeAlert2 = searchAlerts(monitor2)
        assertEquals("1 alert should be active", 1, activeAlert2.size)

        // Mark second alert as COMPLETED
        updateMonitor(monitor2.copy(triggers = listOf(trigger2.copy(condition = NEVER_RUN)), id = monitor2.id), true)
        executeMonitor(monitor2.id)

        // For the second alert, since history is now disabled, searching for the completed alert should return an empty List
        // since a COMPLETED alert will be removed from the alert index and not added to the history index
        val completedAlert2 = searchAlerts(monitor2, AlertIndices.ALL_ALERT_INDEX_PATTERN)
        assertTrue("Alert is not completed", completedAlert2.isEmpty())

        // Get history entry count again and ensure the new alert was not added
        assertEquals(1, getAlertHistoryDocCount())
    }

    fun `test short retention period`() {
        resetHistorySettings()

        // Create monitor and execute
        val trigger = randomQueryLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomQueryLevelMonitor(triggers = listOf(trigger)))
        executeMonitor(monitor.id)

        // Check if alert is active and alert index is created
        val activeAlert = searchAlerts(monitor)
        assertEquals("1 alert should be active", 1, activeAlert.size)
        assertEquals("Did not find 2 alert indices", 2, getAlertIndices().size)
        // History index is created but is empty
        assertEquals(0, getAlertHistoryDocCount())

        // Mark alert as COMPLETED
        updateMonitor(monitor.copy(triggers = listOf(trigger.copy(condition = NEVER_RUN)), id = monitor.id), true)
        executeMonitor(monitor.id)

        // Verify alert is completed
        val completedAlert = searchAlerts(monitor, AlertIndices.ALL_ALERT_INDEX_PATTERN).single()
        assertNotNull("Alert is not completed", completedAlert.endTime)

        // The completed alert should be removed from the active alert index and added to the history index
        assertEquals(1, getAlertHistoryDocCount())

        // Update rollover check and max docs as well as decreasing the retention period
        client().updateSettings(AlertingSettings.ALERT_HISTORY_ROLLOVER_PERIOD.key, "1s")
        client().updateSettings(AlertingSettings.ALERT_HISTORY_MAX_DOCS.key, 1)
        client().updateSettings(AlertingSettings.ALERT_HISTORY_RETENTION_PERIOD.key, "1s")

        // Give some time for history to be rolled over and cleared
        Thread.sleep(5000)

        // Given the max_docs and retention settings above, the history index will rollover and the non-write index will be deleted.
        // This leaves two indices: alert index and an empty history write index
        assertEquals("Did not find 2 alert indices", 2, getAlertIndices().size)
        assertEquals(0, getAlertHistoryDocCount())
    }

    fun `test short finding retention period`() {
        resetHistorySettings()

        // Create monitor and execute
        val testIndex = createTestIndex()
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docReturningInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docReturningInput), triggers = listOf(trigger)))

        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_field" : "us-west-2"
        }"""
        indexDoc(testIndex, "1", testDoc)

        executeMonitor(monitor.id)

        // Check if alert is active and alert index is created
        val activeAlert = searchAlerts(monitor)
        assertEquals("1 alert should be active", 1, activeAlert.size)
        assertEquals("Did not find 2 alert indices", 2, getAlertIndices().size)
        // History index is created but is empty
        assertEquals(0, getAlertHistoryDocCount())

        // Mark doc level alert as Acknowledged
        acknowledgeAlerts(monitor, activeAlert[0])

        // Verify alert is completed
        val ackAlert = searchAlerts(monitor, AlertIndices.ALL_ALERT_INDEX_PATTERN).single()
        assertNotNull("Alert is not acknowledged", ackAlert.acknowledgedTime)

        // The completed alert should be removed from the active alert index and added to the history index
        assertEquals(1, getAlertHistoryDocCount())

        // Update rollover check and max docs as well as decreasing the retention period
        client().updateSettings(AlertingSettings.ALERT_HISTORY_ROLLOVER_PERIOD.key, "1s")
        client().updateSettings(AlertingSettings.ALERT_HISTORY_MAX_DOCS.key, 1)
        client().updateSettings(AlertingSettings.ALERT_HISTORY_RETENTION_PERIOD.key, "1s")

        // Give some time for history to be rolled over and cleared
        Thread.sleep(5000)

        // Given the max_docs and retention settings above, the history index will rollover and the non-write index will be deleted.
        // This leaves two indices: alert index and an empty history write index
        assertEquals("Did not find 2 alert indices", 2, getAlertIndices().size)
        assertEquals(0, getAlertHistoryDocCount())
    }

    private fun assertIndexExists(index: String) {
        val response = client().makeRequest("HEAD", index)
        assertEquals("Index $index does not exist.", RestStatus.OK, response.restStatus())
    }

    private fun assertIndexDoesNotExist(index: String) {
        val response = client().makeRequest("HEAD", index)
        assertEquals("Index $index does not exist.", RestStatus.NOT_FOUND, response.restStatus())
    }

    private fun resetHistorySettings() {
        client().updateSettings(AlertingSettings.ALERT_HISTORY_ENABLED.key, "true")
        client().updateSettings(AlertingSettings.ALERT_HISTORY_ROLLOVER_PERIOD.key, "60s")
        client().updateSettings(AlertingSettings.ALERT_HISTORY_RETENTION_PERIOD.key, "60s")
        client().updateSettings(AlertingSettings.FINDING_HISTORY_ENABLED.key, "true")
        client().updateSettings(AlertingSettings.FINDING_HISTORY_ROLLOVER_PERIOD.key, "60s")
        client().updateSettings(AlertingSettings.FINDING_HISTORY_RETENTION_PERIOD.key, "60s")
    }

    private fun getAlertIndices(): List<String> {
        val response = client().makeRequest("GET", "/_cat/indices/${AlertIndices.ALL_ALERT_INDEX_PATTERN}?format=json")
        val xcp = createParser(XContentType.JSON.xContent(), response.entity.content)
        val responseList = xcp.list()
        val indices = mutableListOf<String>()
        responseList.filterIsInstance<Map<String, Any>>().forEach { indices.add(it["index"] as String) }

        return indices
    }

    private fun getFindingIndices(): List<String> {
        val response = client().makeRequest("GET", "/_cat/indices/${AlertIndices.ALL_FINDING_INDEX_PATTERN}?format=json")
        val xcp = createParser(XContentType.JSON.xContent(), response.entity.content)
        val responseList = xcp.list()
        val indices = mutableListOf<String>()
        responseList.filterIsInstance<Map<String, Any>>().forEach { indices.add(it["index"] as String) }

        return indices
    }

    private fun getAlertHistoryDocCount(): Long {
        val request = """
            {
                "query": {
                    "match_all": {}
                }
            }
        """.trimIndent()
        val response = adminClient().makeRequest(
            "POST", "${AlertIndices.ALERT_HISTORY_ALL}/_search", emptyMap(),
            StringEntity(request, APPLICATION_JSON)
        )
        assertEquals("Request to get alert history failed", RestStatus.OK, response.restStatus())
        return SearchResponse.fromXContent(createParser(jsonXContent, response.entity.content)).hits.totalHits!!.value
    }
}
