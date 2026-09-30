/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.resthandler

import org.apache.hc.core5.http.ContentType.APPLICATION_JSON
import org.apache.hc.core5.http.io.entity.StringEntity
import org.opensearch.alerting.ALERTING_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.cleanup.AlertCleanupService
import org.opensearch.alerting.cleanup.AlertCleanupTask
import org.opensearch.alerting.cleanup.CleanupScope
import org.opensearch.alerting.core.settings.ScheduledJobSettings
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.randomAlert
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.core.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase
import java.util.concurrent.TimeUnit

/**
 * Multi-node coverage for the durable cleanup task that makes alert cleanup survive the loss of the node performing it.
 * Run with `./gradlew :alerting:integTest -PnumNodes=3`.
 *
 * [DeletedMonitorAlertsCleanupIT] covers the cleanup on the path where everything works: the delete announces, a node
 * wins the lock, and the alerts move. The tests here cover the path where the announcement never reaches a node that
 * could act on it -- the state a node also leaves behind by dying mid-drain, and the state that used to mean the alerts
 * were orphaned forever, because nothing but the in-flight coroutine knew they were owed.
 *
 * The announcement is suppressed by disabling the job sweeper, which is what invokes it: the delete path itself only
 * *records* the task. That produces a genuinely abandoned task without killing a node, and the assertion in phase one
 * (that no alert has moved) is what proves the task really is abandoned rather than merely being drained slowly.
 */
class AlertCleanupTaskIT : AlertingRestTestCase() {

    fun `test an announcement that never arrives leaves a cleanup task that the resume pass finishes`() {
        // Park the resume pass first, so phase one observes the abandoned task instead of racing a pass that is already
        // scheduled. Cluster settings are persistent, so every node's pass is rescheduled before the monitor exists.
        client().updateSettings(AlertingSettings.ALERT_CLEANUP_RESUME_INTERVAL.key, PARKED_INTERVAL)
        client().updateSettings(ScheduledJobSettings.SWEEPER_ENABLED.key, false)
        putAlertMappings()

        val monitor = createRandomMonitor(refresh = true)
        seedActiveAlerts(monitor.id) { randomAlert(monitor) }
        assertEquals("Test setup did not seed the expected number of alerts", ALERT_COUNT, countAlerts(monitor.id))

        val deleteResponse = client().makeRequest("DELETE", "$ALERTING_BASE_URI/${monitor.id}")
        assertEquals("Delete request not successful", RestStatus.OK, deleteResponse.restStatus())

        // Phase one: the delete has committed and nothing is draining. What the alerts hang off now is the task.
        val taskId = AlertCleanupTask.taskId(monitor.id, CleanupScope.MONITOR)
        val task = cleanupTask(taskId)
        assertNotNull("The delete recorded no cleanup task, so nothing can ever find these alerts", task)
        task!!
        assertEquals("The task must name the job exactly rather than leaving it to be inferred", monitor.id, task["job_id"])
        assertEquals(CleanupScope.MONITOR.name, task["scope"])
        // Resolved from the monitor while it still existed: at drain time the monitor is gone and cannot be consulted.
        assertEquals(AlertIndices.ALERT_INDEX, task["alert_index"])
        assertEquals(AlertIndices.ALERT_HISTORY_WRITE_INDEX, task["alert_history_index"])
        assertEquals("The whole job is gone, so no trigger survives", emptyList<String>(), task["surviving_trigger_ids"])
        assertEquals("A deleted job must not be confused with a job that lost its last trigger", true, task["job_deleted"])
        assertEquals("Nothing has been drained, so the cursor must still be at the start", -1, (task["cursor"] as Number).toInt())
        assertEquals(0, (task["moved_count"] as Number).toInt())
        assertEquals(
            "An alert moved before the resume pass fired, so the task under test was not abandoned",
            ALERT_COUNT,
            countAlerts(monitor.id)
        )
        assertEquals(0, countAlerts(monitor.id, AlertIndices.ALERT_HISTORY_WRITE_INDEX))

        // Phase two: let the resume pass fire. No node has been told about this task; a node has to find it itself.
        client().updateSettings(AlertingSettings.ALERT_CLEANUP_RESUME_INTERVAL.key, LIVE_INTERVAL)

        OpenSearchTestCase.waitUntil({
            countAlerts(monitor.id) == 0 && countAlerts(monitor.id, AlertIndices.ALERT_HISTORY_WRITE_INDEX) == ALERT_COUNT
        }, CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)

        assertEquals(
            "The resume pass left active alerts behind in ${AlertIndices.ALERT_INDEX} for the deleted monitor ${monitor.id}",
            0,
            countAlerts(monitor.id)
        )
        assertEquals(
            "The resume pass did not move every alert to ${AlertIndices.ALERT_HISTORY_WRITE_INDEX}",
            ALERT_COUNT,
            countAlerts(monitor.id, AlertIndices.ALERT_HISTORY_WRITE_INDEX)
        )

        val historyAlerts = searchAlerts(monitor, AlertIndices.ALERT_HISTORY_WRITE_INDEX, size = ALERT_COUNT)
        assertEquals("Search over the history index disagrees with _count", ALERT_COUNT, historyAlerts.size)
        assertEquals(
            "Moved alerts must be recorded as DELETED",
            emptyList<Alert.State>(),
            historyAlerts.map { it.state }.filter { it != Alert.State.DELETED }.distinct()
        )

        // A finished task must be removed, or every subsequent pass re-offers it and re-acquires its lock forever.
        OpenSearchTestCase.waitUntil({ cleanupTask(taskId) == null }, CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        assertNull("A drained cleanup task was left behind for [$taskId]", cleanupTask(taskId))
    }

    fun `test a cleanup driven by the announcement leaves no task behind`() {
        // The resume pass is parked for the whole test, so the cleanup observed here can only be the announcement's.
        client().updateSettings(AlertingSettings.ALERT_CLEANUP_RESUME_INTERVAL.key, PARKED_INTERVAL)
        client().updateSettings(ScheduledJobSettings.SWEEPER_ENABLED.key, true)
        putAlertMappings()

        val monitor = createRandomMonitor(refresh = true)
        seedActiveAlerts(monitor.id) { randomAlert(monitor) }
        assertEquals("Test setup did not seed the expected number of alerts", ALERT_COUNT, countAlerts(monitor.id))

        val deleteResponse = client().makeRequest("DELETE", "$ALERTING_BASE_URI/${monitor.id}")
        assertEquals("Delete request not successful", RestStatus.OK, deleteResponse.restStatus())

        OpenSearchTestCase.waitUntil({
            countAlerts(monitor.id) == 0 && countAlerts(monitor.id, AlertIndices.ALERT_HISTORY_WRITE_INDEX) == ALERT_COUNT
        }, CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)

        assertEquals(
            "Active alerts were left behind in ${AlertIndices.ALERT_INDEX} for the deleted monitor ${monitor.id}",
            0,
            countAlerts(monitor.id)
        )
        assertEquals(
            "Not every alert of the deleted monitor reached ${AlertIndices.ALERT_HISTORY_WRITE_INDEX}",
            ALERT_COUNT,
            countAlerts(monitor.id, AlertIndices.ALERT_HISTORY_WRITE_INDEX)
        )

        val taskId = AlertCleanupTask.taskId(monitor.id, CleanupScope.MONITOR)
        OpenSearchTestCase.waitUntil({ cleanupTask(taskId) == null }, CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        assertNull("The announced cleanup finished but left its task behind for [$taskId]", cleanupTask(taskId))
        assertEquals(
            "A monitor delete recorded a task for a scope with no alerts to move",
            0,
            countCleanupTasks()
        )
    }

    /**
     * Bulk-indexes [ALERT_COUNT] ACTIVE alerts with deterministic ids derived from [idPrefix]. [alert] is invoked once
     * per alert; its `id` and `state` are overwritten.
     */
    private fun seedActiveAlerts(idPrefix: String, alert: () -> Alert): List<Alert> {
        val alerts = (1..ALERT_COUNT).map {
            alert().copy(id = "$idPrefix-alert-$it", state = Alert.State.ACTIVE)
        }
        return createAlerts(alerts)
    }

    /**
     * The `_source` of the cleanup task document [taskId], or null if there is none.
     *
     * Read with `ignore_unavailable`, because the task index is created on the first task the cluster ever owes and so
     * may legitimately not exist yet. Task writes use an immediate refresh, so a task that exists is always visible.
     */
    private fun cleanupTask(taskId: String): Map<String, Any>? {
        val body = """{ "size" : 1, "query" : { "ids" : { "values" : ["$taskId"] } } }"""
        val response = adminClient().makeRequest(
            "POST", "/${AlertCleanupService.CLEANUP_TASK_INDEX}/_search", mapOf("ignore_unavailable" to "true"),
            StringEntity(body, APPLICATION_JSON)
        )
        assertEquals("Cleanup task search failed", RestStatus.OK, response.restStatus())
        val hits = (response.asMap()["hits"] as Map<*, *>)["hits"] as List<*>
        @Suppress("UNCHECKED_CAST")
        return (hits.singleOrNull() as Map<String, Any>?)?.get("_source") as Map<String, Any>?
    }

    /** How many cleanup tasks the cluster currently owes, across every job and scope. */
    private fun countCleanupTasks(): Int {
        val response = adminClient().makeRequest(
            "GET", "/${AlertCleanupService.CLEANUP_TASK_INDEX}/_count", mapOf("ignore_unavailable" to "true")
        )
        assertEquals("Cleanup task count failed", RestStatus.OK, response.restStatus())
        return (response.asMap()["count"] as Number).toInt()
    }

    companion object {
        /**
         * More than two full `AlertMover.MOVE_ALERTS_PAGE_SIZE` (100) pages, so the resume pass is exercised against a
         * task that has to page rather than one that finishes in a single search.
         */
        private const val ALERT_COUNT = 250

        /** Long enough that the resume pass cannot fire during the test, so an abandoned task stays abandoned. */
        private const val PARKED_INTERVAL = "30m"

        /** Short enough to keep the test quick; the pass is rescheduled from now, not from the last firing. */
        private const val LIVE_INTERVAL = "3s"

        private const val CLEANUP_TIMEOUT_SECONDS = 60L
    }
}
