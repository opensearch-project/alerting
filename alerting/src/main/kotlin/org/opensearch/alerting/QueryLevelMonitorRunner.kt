/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.QueryLevelTriggerRunResult
import org.opensearch.alerting.opensearchapi.InjectorContextElement
import org.opensearch.alerting.opensearchapi.withClosableContext
import org.opensearch.alerting.script.QueryLevelTriggerExecutionContext
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.isADMonitor
import org.opensearch.alerting.workflow.WorkflowRunContext
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.QueryLevelTrigger
import java.time.Instant

object QueryLevelMonitorRunner : MonitorRunner() {
    private val logger = LogManager.getLogger(javaClass)

    override suspend fun runMonitor(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryrun: Boolean,
        workflowRunContext: WorkflowRunContext?,
        executionId: String
    ): MonitorRunResult<QueryLevelTriggerRunResult> {
        val roles = MonitorRunnerService.getRolesForMonitor(monitor)
        logger.debug("Running monitor: ${monitor.name} with roles: $roles Thread: ${Thread.currentThread().name}")

        if (periodStart == periodEnd) {
            logger.warn("Start and end time are the same: $periodStart. This monitor will probably only run once.")
        }

        var monitorResult = MonitorRunResult<QueryLevelTriggerRunResult>(monitor.name, periodStart, periodEnd)
        val currentAlerts = try {
            monitorCtx.alertIndices!!.createOrUpdateAlertIndex(monitor.dataSources)
            monitorCtx.alertIndices!!.createOrUpdateInitialAlertHistoryIndex(monitor.dataSources)
            monitorCtx.alertService!!.loadCurrentAlertsForQueryLevelMonitor(monitor, workflowRunContext)
        } catch (e: Exception) {
            // We can't save ERROR alerts to the index here as we don't know if there are existing ACTIVE alerts
            val id = if (monitor.id.trim().isEmpty()) "_na_" else monitor.id
            logger.error("Error loading alerts for monitor: $id", e)
            return monitorResult.copy(error = e)
        }
        if (!isADMonitor(monitor)) {
            withClosableContext(InjectorContextElement(monitor.id, monitorCtx.settings!!, monitorCtx.threadPool!!.threadContext, roles)) {
                monitorResult = monitorResult.copy(
                    inputResults = monitorCtx.inputService!!.collectInputResults(monitor, periodStart, periodEnd, null, workflowRunContext)
                )
            }
        } else {
            monitorResult = monitorResult.copy(
                inputResults = monitorCtx.inputService!!.collectInputResultsForADMonitor(monitor, periodStart, periodEnd)
            )
        }

        val updatedAlerts = mutableListOf<Alert>()
        val triggerResults = mutableMapOf<String, QueryLevelTriggerRunResult>()
        for (trigger in monitor.triggers) {
            val currentAlert = currentAlerts[trigger]
            val triggerCtx = QueryLevelTriggerExecutionContext(monitor, trigger as QueryLevelTrigger, monitorResult, currentAlert)
            val triggerResult = when (monitor.monitorType) {
                Monitor.MonitorType.QUERY_LEVEL_MONITOR ->
                    monitorCtx.triggerService!!.runQueryLevelTrigger(monitor, trigger, triggerCtx)
                Monitor.MonitorType.CLUSTER_METRICS_MONITOR -> {
                    val remoteMonitoringEnabled = monitorCtx.clusterService!!.clusterSettings.get(AlertingSettings.REMOTE_MONITORING_ENABLED)
                    logger.debug("Remote monitoring enabled: {}", remoteMonitoringEnabled)
                    if (remoteMonitoringEnabled) monitorCtx.triggerService!!.runClusterMetricsTrigger(monitor, trigger, triggerCtx, monitorCtx.clusterService!!)
                    else monitorCtx.triggerService!!.runQueryLevelTrigger(monitor, trigger, triggerCtx)
                }
                else ->
                    throw IllegalArgumentException("Unsupported monitor type: ${monitor.monitorType.name}.")
            }

            triggerResults[trigger.id] = triggerResult

            if (monitorCtx.triggerService!!.isQueryLevelTriggerActionable(triggerCtx, triggerResult, workflowRunContext)) {
                val actionCtx = triggerCtx.copy(error = monitorResult.error ?: triggerResult.error)
                for (action in trigger.actions) {
                    triggerResult.actionResults[action.id] = this.runAction(action, actionCtx, monitorCtx, monitor, dryrun)
                }
            }

            val updatedAlert = monitorCtx.alertService!!.composeQueryLevelAlert(
                triggerCtx,
                triggerResult,
                monitorResult.alertError() ?: triggerResult.alertError(),
                executionId,
                workflowRunContext
            )
            if (updatedAlert != null) updatedAlerts += updatedAlert
        }

        // Don't save alerts if this is a test monitor
        if (!dryrun && monitor.id != Monitor.NO_ID) {
            monitorCtx.retryPolicy?.let {
                monitorCtx.alertService!!.saveAlerts(
                    monitor.dataSources,
                    updatedAlerts,
                    it,
                    routingId = monitor.id
                )
            }
        }
        return monitorResult.copy(triggerResults = triggerResults)
    }
}
