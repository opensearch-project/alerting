/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.WarningsHandler
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.IntervalSchedule
import org.opensearch.commons.alerting.model.PPLTrigger
import org.opensearch.test.OpenSearchTestCase
import java.time.temporal.ChronoUnit.MINUTES
import java.util.concurrent.TimeUnit

class OrphanedAlertSweeperIT : AlertingRestTestCase() {

    fun `test orphaned alert is moved to history after monitor deletion`() {
        // Setup
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(2, MINUTES, "abc", 5)

        // Create and execute a monitor to generate an alert
        val monitor = createMonitor(
            randomPPLMonitor(
                enabled = true,
                schedule = IntervalSchedule(interval = 1, unit = MINUTES),
                triggers = listOf(
                    randomPPLTrigger(
                        conditionType = PPLTrigger.ConditionType.NUMBER_OF_RESULTS,
                        numResultsCondition = PPLTrigger.NumResultsCondition.GREATER_THAN,
                        numResultsValue = 0L,
                        customCondition = null
                    )
                ),
                query = "source = $TEST_INDEX_NAME | head 10"
            )
        )
        executeMonitor(monitor.id)
        val alerts = searchAlerts(monitor)
        assertEquals("Alert should have been generated", 1, alerts.size)
        assertEquals("Alert should be ACTIVE", Alert.State.ACTIVE, alerts.single().state)

        // Disable the JobSweeper so that postDelete doesn't fire and clean up alerts
        client().updateSettings("plugins.scheduled_jobs.enabled", false)

        // Delete the monitor directly from the config index (simulating the case where
        // JobSweeper.postDelete doesn't fire)
        val deleteRequest = Request("DELETE", "/.opendistro-alerting-config/_doc/${monitor.id}?refresh=true")
        val options = RequestOptions.DEFAULT.toBuilder()
        options.setWarningsHandler(WarningsHandler.PERMISSIVE)
        deleteRequest.options = options.build()
        adminClient().performRequest(deleteRequest)

        // Verify the alert is still in the active index (orphaned)
        val orphanedAlerts = searchAlerts(monitor, AlertIndices.ALERT_INDEX)
        assertEquals("Alert should still be in active index (orphaned)", 1, orphanedAlerts.size)

        // Re-enable the JobSweeper and enable the orphaned alert sweeper with a short period
        client().updateSettings("plugins.scheduled_jobs.enabled", true)
        client().updateSettings(AlertingSettings.ORPHANED_ALERT_SWEEP_ENABLED.key, true)
        client().updateSettings(AlertingSettings.ORPHANED_ALERT_SWEEP_PERIOD.key, "1s")

        // Wait for the sweeper to clean up the orphaned alert
        OpenSearchTestCase.waitUntil({
            val activeAlerts = searchAlerts(monitor, AlertIndices.ALERT_INDEX)
            activeAlerts.isEmpty()
        }, 30, TimeUnit.SECONDS)

        // Verify the alert is gone from active index
        val activeAlerts = searchAlerts(monitor, AlertIndices.ALERT_INDEX)
        assertTrue("Orphaned alert should be removed from active index", activeAlerts.isEmpty())

        // Verify the alert is in history with DELETED state
        val historyAlerts = searchAlerts(monitor, AlertIndices.ALL_ALERT_INDEX_PATTERN)
        assertEquals("Alert should be in history", 1, historyAlerts.size)
        assertEquals("Alert should be in DELETED state", Alert.State.DELETED, historyAlerts.single().state)
    }

    fun `test acknowledged orphaned alert is moved to history after monitor deletion`() {
        // Setup
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(2, MINUTES, "abc", 5)

        // Create, execute, and acknowledge
        val monitor = createMonitor(
            randomPPLMonitor(
                enabled = true,
                schedule = IntervalSchedule(interval = 1, unit = MINUTES),
                triggers = listOf(
                    randomPPLTrigger(
                        conditionType = PPLTrigger.ConditionType.NUMBER_OF_RESULTS,
                        numResultsCondition = PPLTrigger.NumResultsCondition.GREATER_THAN,
                        numResultsValue = 0L,
                        customCondition = null
                    )
                ),
                query = "source = $TEST_INDEX_NAME | head 10"
            )
        )
        executeMonitor(monitor.id)
        val activeAlert = searchAlerts(monitor).single()
        acknowledgeAlerts(monitor, activeAlert)
        val ackedAlerts = searchAlerts(monitor)
        assertEquals("Alert should be ACKNOWLEDGED", Alert.State.ACKNOWLEDGED, ackedAlerts.single().state)

        // Disable the JobSweeper so that postDelete doesn't fire and clean up alerts
        client().updateSettings("plugins.scheduled_jobs.enabled", false)

        // Delete the monitor directly
        val deleteRequest = Request("DELETE", "/.opendistro-alerting-config/_doc/${monitor.id}?refresh=true")
        val options = RequestOptions.DEFAULT.toBuilder()
        options.setWarningsHandler(WarningsHandler.PERMISSIVE)
        deleteRequest.options = options.build()
        adminClient().performRequest(deleteRequest)

        // Verify the alert is still in the active index (orphaned)
        val orphanedAlerts = searchAlerts(monitor, AlertIndices.ALERT_INDEX)
        assertEquals("Alert should still be in active index (orphaned)", 1, orphanedAlerts.size)

        // Re-enable the JobSweeper and enable orphaned alert sweeper
        client().updateSettings("plugins.scheduled_jobs.enabled", true)
        client().updateSettings(AlertingSettings.ORPHANED_ALERT_SWEEP_ENABLED.key, true)
        client().updateSettings(AlertingSettings.ORPHANED_ALERT_SWEEP_PERIOD.key, "1s")

        // Wait for cleanup
        OpenSearchTestCase.waitUntil({
            val remaining = searchAlerts(monitor, AlertIndices.ALERT_INDEX)
            remaining.isEmpty()
        }, 30, TimeUnit.SECONDS)

        // Verify moved to history as DELETED
        val historyAlerts = searchAlerts(monitor, AlertIndices.ALL_ALERT_INDEX_PATTERN)
        assertEquals("Alert should be in history", 1, historyAlerts.size)
        assertEquals("Alert should be in DELETED state", Alert.State.DELETED, historyAlerts.single().state)
    }

    fun `test non-orphaned alerts are not affected by sweeper`() {
        // Setup
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(2, MINUTES, "abc", 5)

        // Create and execute a monitor (DON'T delete it)
        val monitor = createMonitor(
            randomPPLMonitor(
                enabled = true,
                schedule = IntervalSchedule(interval = 1, unit = MINUTES),
                triggers = listOf(
                    randomPPLTrigger(
                        conditionType = PPLTrigger.ConditionType.NUMBER_OF_RESULTS,
                        numResultsCondition = PPLTrigger.NumResultsCondition.GREATER_THAN,
                        numResultsValue = 0L,
                        customCondition = null
                    )
                ),
                query = "source = $TEST_INDEX_NAME | head 10"
            )
        )
        executeMonitor(monitor.id)
        assertEquals("Alert should exist", 1, searchAlerts(monitor).size)

        // Enable sweeper with short period
        client().updateSettings(AlertingSettings.ORPHANED_ALERT_SWEEP_ENABLED.key, true)
        client().updateSettings(AlertingSettings.ORPHANED_ALERT_SWEEP_PERIOD.key, "1s")

        // Wait a few sweep cycles
        OpenSearchTestCase.waitUntil({ false }, 5, TimeUnit.SECONDS)

        // Alert should still be active (monitor still exists)
        val alerts = searchAlerts(monitor)
        assertEquals("Non-orphaned alert should remain", 1, alerts.size)
        assertEquals("Alert should still be ACTIVE", Alert.State.ACTIVE, alerts.single().state)
    }
}
