/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.IntervalSchedule
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant
import java.time.temporal.ChronoUnit

class RestExecuteMonitorActionTests : OpenSearchTestCase() {

    fun `test validateDataSources rejects custom queryIndex`() {
        val action = RestExecuteMonitorAction()
        val monitor = createMonitor(
            dataSources = DataSources(queryIndex = "attacker-controlled-index")
        )

        val exception = expectThrows(java.lang.reflect.InvocationTargetException::class.java) {
            invokeValidateDataSources(action, monitor)
        }
        assertTrue(exception.cause is IllegalArgumentException)
        assertEquals("Custom Data Sources are not allowed.", exception.cause!!.message)
    }

    fun `test validateDataSources rejects custom findingsIndex`() {
        val action = RestExecuteMonitorAction()
        val monitor = createMonitor(
            dataSources = DataSources(findingsIndex = "attacker-findings-index")
        )

        val exception = expectThrows(java.lang.reflect.InvocationTargetException::class.java) {
            invokeValidateDataSources(action, monitor)
        }
        assertTrue(exception.cause is IllegalArgumentException)
        assertEquals("Custom Data Sources are not allowed.", exception.cause!!.message)
    }

    fun `test validateDataSources rejects custom alertsIndex`() {
        val action = RestExecuteMonitorAction()
        val monitor = createMonitor(
            dataSources = DataSources(alertsIndex = "attacker-alerts-index")
        )

        val exception = expectThrows(java.lang.reflect.InvocationTargetException::class.java) {
            invokeValidateDataSources(action, monitor)
        }
        assertTrue(exception.cause is IllegalArgumentException)
        assertEquals("Custom Data Sources are not allowed.", exception.cause!!.message)
    }

    fun `test validateDataSources allows default data sources`() {
        val action = RestExecuteMonitorAction()
        val monitor = createMonitor(
            dataSources = DataSources(
                queryIndex = ScheduledJob.DOC_LEVEL_QUERIES_INDEX,
                findingsIndex = AlertIndices.FINDING_HISTORY_WRITE_INDEX,
                alertsIndex = AlertIndices.ALERT_INDEX
            )
        )

        // Should not throw
        invokeValidateDataSources(action, monitor)
    }

    private fun createMonitor(dataSources: DataSources = DataSources()): Monitor {
        return Monitor(
            name = "test",
            monitorType = Monitor.MonitorType.DOC_LEVEL_MONITOR.value,
            enabled = false,
            schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(),
            enabledTime = null,
            user = null,
            inputs = listOf(DocLevelMonitorInput("desc", listOf("test-index"), emptyList())),
            triggers = emptyList(),
            uiMetadata = mapOf(),
            dataSources = dataSources
        )
    }

    private fun invokeValidateDataSources(action: RestExecuteMonitorAction, monitor: Monitor) {
        val method = action.javaClass.getDeclaredMethod("validateDataSources", Monitor::class.java)
        method.isAccessible = true
        method.invoke(action, monitor)
    }
}
