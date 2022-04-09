package org.opensearch.alerting

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.elasticapi.InjectorContextElement
import org.opensearch.alerting.model.ActionRunResult
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.AlertingConfigAccessor
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.QueryLevelTrigger
import org.opensearch.alerting.model.QueryLevelTriggerRunResult
import org.opensearch.alerting.model.action.Action
import org.opensearch.alerting.script.QueryLevelTriggerExecutionContext
import org.opensearch.alerting.script.TriggerExecutionContext
import org.opensearch.alerting.util.isADMonitor
import org.opensearch.alerting.util.isAllowed
import org.opensearch.common.Strings
import java.time.Instant

object QueryLevelMonitorRunner : MonitorRunner {
    private val logger = LogManager.getLogger(javaClass)

    override suspend fun runMonitor(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryrun: Boolean
    ): MonitorRunResult<QueryLevelTriggerRunResult> {
        val roles = MonitorRunnerService.getRolesForMonitor(monitor)
        logger.debug("Running monitor: ${monitor.name} with roles: $roles Thread: ${Thread.currentThread().name}")

        if (periodStart == periodEnd) {
            logger.warn("Start and end time are the same: $periodStart. This monitor will probably only run once.")
        }

        var monitorResult = MonitorRunResult<QueryLevelTriggerRunResult>(monitor.name, periodStart, periodEnd)
        val currentAlerts = try {
            monitorCtx.alertIndices!!.createOrUpdateAlertIndex()
            monitorCtx.alertIndices!!.createOrUpdateInitialAlertHistoryIndex()
            monitorCtx.alertService!!.loadCurrentAlertsForQueryLevelMonitor(monitor)
        } catch (e: Exception) {
            // We can't save ERROR alerts to the index here as we don't know if there are existing ACTIVE alerts
            val id = if (monitor.id.trim().isEmpty()) "_na_" else monitor.id
            logger.error("Error loading alerts for monitor: $id", e)
            return monitorResult.copy(error = e)
        }
        if (!isADMonitor(monitor)) {
            runBlocking(InjectorContextElement(monitor.id, monitorCtx.settings!!, monitorCtx.threadPool!!.threadContext, roles)) {
                monitorResult = monitorResult.copy(
                    inputResults = monitorCtx.inputService!!.collectInputResults(monitor, periodStart, periodEnd)
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
            val triggerResult = monitorCtx.triggerService!!.runQueryLevelTrigger(monitor, trigger, triggerCtx)
            triggerResults[trigger.id] = triggerResult

            if (monitorCtx.triggerService!!.isQueryLevelTriggerActionable(triggerCtx, triggerResult)) {
                val actionCtx = triggerCtx.copy(error = monitorResult.error ?: triggerResult.error)
                for (action in trigger.actions) {
                    triggerResult.actionResults[action.id] = this.runAction(action, actionCtx, monitorCtx, dryrun)
                }
            }

            val updatedAlert = monitorCtx.alertService!!.composeQueryLevelAlert(
                triggerCtx, triggerResult,
                monitorResult.alertError() ?: triggerResult.alertError()
            )
            if (updatedAlert != null) updatedAlerts += updatedAlert
        }

        // Don't save alerts if this is a test monitor
        if (!dryrun && monitor.id != Monitor.NO_ID) {
            monitorCtx.retryPolicy?.let { monitorCtx.alertService!!.saveAlerts(updatedAlerts, it) }
        }
        return monitorResult.copy(triggerResults = triggerResults)
    }

    override suspend fun runAction(
        action: Action,
        ctx: TriggerExecutionContext,
        monitorCtx: MonitorRunnerExecutionContext,
        dryrun: Boolean
    ): ActionRunResult {
        return try {
            if (!MonitorRunnerService.isActionActionable(action, (ctx as QueryLevelTriggerExecutionContext).alert)) {
                return ActionRunResult(action.id, action.name, mapOf(), true, null, null)
            }
            val actionOutput = mutableMapOf<String, String>()
            actionOutput[Action.SUBJECT] = if (action.subjectTemplate != null)
                MonitorRunnerService.compileTemplate(action.subjectTemplate, ctx)
            else ""
            actionOutput[Action.MESSAGE] = MonitorRunnerService.compileTemplate(action.messageTemplate, ctx)
            if (Strings.isNullOrEmpty(actionOutput[Action.MESSAGE])) {
                throw IllegalStateException("Message content missing in the Destination with id: ${action.destinationId}")
            }
            if (!dryrun) {
                withContext(Dispatchers.IO) {
                    val destination = AlertingConfigAccessor.getDestinationInfo(
                        monitorCtx.client!!,
                        monitorCtx.xContentRegistry!!,
                        action.destinationId
                    )
                    if (!destination.isAllowed(monitorCtx.allowList)) {
                        throw IllegalStateException("Monitor contains a Destination type that is not allowed: ${destination.type}")
                    }

                    val destinationCtx = monitorCtx.destinationContextFactory!!.getDestinationContext(destination)
                    actionOutput[Action.MESSAGE_ID] = destination.publish(
                        actionOutput[Action.SUBJECT],
                        actionOutput[Action.MESSAGE]!!,
                        destinationCtx,
                        monitorCtx.hostDenyList
                    )
                }
            }
            ActionRunResult(action.id, action.name, actionOutput, false, MonitorRunnerService.currentTime(), null)
        } catch (e: Exception) {
            ActionRunResult(action.id, action.name, mapOf(), false, MonitorRunnerService.currentTime(), e)
        }
    }
}
