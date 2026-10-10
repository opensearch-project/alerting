/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.resthandler

import org.opensearch.alerting.ALERTING_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.core.settings.ScheduledJobSettings
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.randomAlert
import org.opensearch.alerting.randomChainedAlertTrigger
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.alerting.randomQueryLevelTrigger
import org.opensearch.alerting.randomWorkflow
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.core.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase
import java.util.concurrent.TimeUnit

/**
 * Multi-node coverage for the cleanup of ACTIVE alerts belonging to a deleted monitor, a removed trigger, or a deleted
 * workflow. Run with `./gradlew :alerting:integTest -PnumNodes=3`.
 *
 * The alert counts here are deliberately larger than `AlertMover.MOVE_ALERTS_PAGE_SIZE` so the cleanup has to page. The
 * pre-fix `AlertMover.moveAlerts` issued a single search with no `size`, which OpenSearch defaults to 10, and did not
 * loop -- so every monitor with more than 10 open alerts permanently leaked the remainder into the live alerts index.
 * Asserting exact counts (never `>=`) is what makes these tests fail on the unpatched build: an unpatched run moves
 * exactly 10 of [ALERT_COUNT] alerts.
 *
 * Counts are read with `_count` rather than `searchAlerts(...).size` because a search is subject to both the default
 * size of 10 and `index.max_result_window`, either of which would silently cap the observed number of alerts and make
 * the assertions meaningless.
 */
class DeletedMonitorAlertsCleanupIT : AlertingRestTestCase() {

    fun `test deleting a monitor moves all of its active alerts to history`() {
        client().updateSettings(ScheduledJobSettings.SWEEPER_ENABLED.key, true)
        putAlertMappings()

        val monitor = createRandomMonitor(refresh = true)
        seedActiveAlerts(monitor.id) { randomAlert(monitor) }
        assertEquals("Test setup did not seed the expected number of alerts", ALERT_COUNT, countAlerts(monitor.id))

        val deleteResponse = client().makeRequest("DELETE", "$ALERTING_BASE_URI/${monitor.id}")
        assertEquals("Delete request not successful", RestStatus.OK, deleteResponse.restStatus())

        awaitCleanup(monitor.id)

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

        val historyAlerts = searchAlerts(monitor, AlertIndices.ALERT_HISTORY_WRITE_INDEX, size = ALERT_COUNT)
        assertEquals("Search over the history index disagrees with _count", ALERT_COUNT, historyAlerts.size)
        assertEquals(
            "Moved alerts must be recorded as DELETED",
            emptyList<Alert.State>(),
            historyAlerts.map { it.state }.filter { it != Alert.State.DELETED }.distinct()
        )
    }

    fun `test removing a trigger moves all of that trigger's active alerts to history`() {
        client().updateSettings(ScheduledJobSettings.SWEEPER_ENABLED.key, true)
        putAlertMappings()

        val triggerToRemove = randomQueryLevelTrigger()
        val triggerToKeep = randomQueryLevelTrigger()
        val monitor = createMonitor(randomQueryLevelMonitor(triggers = listOf(triggerToRemove, triggerToKeep)), refresh = true)

        // randomAlert(monitor) attaches a freshly generated trigger, so the trigger id has to be set explicitly for the
        // alert to be considered part of the trigger being removed.
        seedActiveAlerts(monitor.id) { randomAlert(monitor).copy(triggerId = triggerToRemove.id) }
        val keptAlert = createAlert(randomAlert(monitor).copy(triggerId = triggerToKeep.id, state = Alert.State.ACTIVE))
        assertEquals("Test setup did not seed the expected number of alerts", ALERT_COUNT + 1, countAlerts(monitor.id))

        val updateResponse = client().makeRequest(
            "PUT", "$ALERTING_BASE_URI/${monitor.id}", emptyMap(),
            monitor.copy(triggers = listOf(triggerToKeep)).toHttpEntity()
        )
        assertEquals("Update request not successful", RestStatus.OK, updateResponse.restStatus())

        OpenSearchTestCase.waitUntil({
            countAlerts(monitor.id) == 1 && countAlerts(monitor.id, AlertIndices.ALERT_HISTORY_WRITE_INDEX) == ALERT_COUNT
        }, CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)

        assertEquals(
            "Active alerts of the removed trigger were left behind in ${AlertIndices.ALERT_INDEX}",
            1,
            countAlerts(monitor.id)
        )
        assertEquals(
            "Not every alert of the removed trigger reached ${AlertIndices.ALERT_HISTORY_WRITE_INDEX}",
            ALERT_COUNT,
            countAlerts(monitor.id, AlertIndices.ALERT_HISTORY_WRITE_INDEX)
        )

        val remaining = searchAlerts(monitor, size = ALERT_COUNT + 1)
        assertEquals("The surviving trigger's alert was moved", listOf(keptAlert.id), remaining.map { it.id })
        assertEquals("The surviving trigger's alert must stay ACTIVE", Alert.State.ACTIVE, remaining.single().state)
    }

    fun `test deleting a workflow moves its chained alerts but preserves a live delegate monitor's alerts`() {
        client().updateSettings(ScheduledJobSettings.SWEEPER_ENABLED.key, true)
        putAlertMappings()

        val delegateMonitor = createRandomMonitor(refresh = true)
        val chainedAlertTrigger = randomChainedAlertTrigger()
        val workflow = createWorkflow(
            randomWorkflow(monitorIds = listOf(delegateMonitor.id), triggers = listOf(chainedAlertTrigger))
        )

        // Chained alerts belong to the workflow: they carry the workflow id and an empty monitor id.
        seedActiveAlerts(workflow.id, routing = workflow.id) {
            randomAlert(delegateMonitor).copy(
                monitorId = "",
                workflowId = workflow.id,
                triggerId = chainedAlertTrigger.id
            )
        }
        // The delegate monitor's own alerts also carry the workflow id, but they belong to a monitor that is still
        // live. Moving them to the history index would be data loss, so the cleanup must leave them alone.
        seedActiveAlerts(delegateMonitor.id) { randomAlert(delegateMonitor).copy(workflowId = workflow.id) }

        assertEquals("Test setup did not seed the expected chained alerts", ALERT_COUNT, countAlerts(""))
        assertEquals("Test setup did not seed the expected delegate alerts", ALERT_COUNT, countAlerts(delegateMonitor.id))

        deleteWorkflow(workflow, deleteDelegates = false)

        awaitCleanup("")

        assertEquals(
            "Chained alerts were left behind in ${AlertIndices.ALERT_INDEX} for the deleted workflow ${workflow.id}",
            0,
            countAlerts("")
        )
        assertEquals(
            "Not every chained alert reached ${AlertIndices.ALERT_HISTORY_WRITE_INDEX}",
            ALERT_COUNT,
            countAlerts("", AlertIndices.ALERT_HISTORY_WRITE_INDEX)
        )
        assertEquals(
            "A live delegate monitor's alerts were moved out of ${AlertIndices.ALERT_INDEX}",
            ALERT_COUNT,
            countAlerts(delegateMonitor.id)
        )
        assertEquals(
            "A live delegate monitor's alerts were copied into ${AlertIndices.ALERT_HISTORY_WRITE_INDEX}",
            0,
            countAlerts(delegateMonitor.id, AlertIndices.ALERT_HISTORY_WRITE_INDEX)
        )
    }

    /**
     * Bulk-indexes [ALERT_COUNT] ACTIVE alerts with deterministic ids derived from [idPrefix]. [alert] is invoked once
     * per alert; its `id` and `state` are overwritten.
     */
    private fun seedActiveAlerts(idPrefix: String, routing: String? = null, alert: () -> Alert): List<Alert> {
        val alerts = (1..ALERT_COUNT).map {
            alert().copy(id = "$idPrefix-alert-$it", state = Alert.State.ACTIVE)
        }
        return createAlerts(alerts, routing = routing)
    }

    /** Waits for every alert of [monitorId] to leave the live index and land in the history index. */
    private fun awaitCleanup(monitorId: String) {
        OpenSearchTestCase.waitUntil({
            countAlerts(monitorId) == 0 && countAlerts(monitorId, AlertIndices.ALERT_HISTORY_WRITE_INDEX) == ALERT_COUNT
        }, CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    }

    companion object {
        /**
         * More than two full `AlertMover.MOVE_ALERTS_PAGE_SIZE` (100) pages, so a fix that merely raised the search
         * `size` to some larger constant without looping would still fail here. Written as a literal rather than
         * derived from that constant so that this test also compiles -- and fails -- against the unpatched source.
         */
        private const val ALERT_COUNT = 250

        private const val CLEANUP_TIMEOUT_SECONDS = 60L
    }
}
