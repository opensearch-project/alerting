/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.model.AlertContext
import org.opensearch.alerting.opensearchapi.InjectorContextElement
import org.opensearch.alerting.opensearchapi.withClosableContext
import org.opensearch.alerting.script.QueryLevelTriggerExecutionContext
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.trigger.RemoteQueryLevelTriggerEvaluator
import org.opensearch.alerting.util.CommentsUtils
import org.opensearch.alerting.util.isADMonitor
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorRunResult
import org.opensearch.commons.alerting.model.PPLInput
import org.opensearch.commons.alerting.model.PPLTrigger
import org.opensearch.commons.alerting.model.PPLTrigger.ConditionType
import org.opensearch.commons.alerting.model.QueryLevelTrigger
import org.opensearch.commons.alerting.model.QueryLevelTriggerRunResult
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.alerting.model.WorkflowRunContext
import org.opensearch.commons.alerting.util.isPPLMonitor
import org.opensearch.transport.TransportService
import java.time.Instant
import java.util.Locale
import kotlin.collections.set

object QueryLevelMonitorRunner : MonitorRunner() {
    private val logger = LogManager.getLogger(javaClass)

    override suspend fun runMonitor(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryrun: Boolean,
        manual: Boolean,
        workflowRunContext: WorkflowRunContext?,
        executionId: String,
        transportService: TransportService
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

        if (isADMonitor(monitor)) {
            monitorResult = monitorResult.copy(
                inputResults = monitorCtx.inputService!!.collectInputResultsForADMonitor(monitor, periodStart, periodEnd)
            )
        } else if (monitor.isPPLMonitor()) {
            withClosableContext(
                InjectorContextElement(
                    monitor.id,
                    monitorCtx.settings!!,
                    monitorCtx.threadPool!!.threadContext,
                    monitor.user?.roles,
                    monitor.user
                )
            ) {
                monitorResult = monitorResult.copy(
                    inputResults = monitorCtx.inputService!!.collectInputResultsForPPLMonitor(monitor, monitorCtx, transportService)
                )
            }
        } else {
            withClosableContext(
                InjectorContextElement(
                    monitor.id,
                    monitorCtx.settings!!,
                    monitorCtx.threadPool!!.threadContext,
                    roles,
                    monitor.user
                )
            ) {
                monitorResult = monitorResult.copy(
                    inputResults = monitorCtx.inputService!!.collectInputResults(
                        monitor,
                        periodStart,
                        periodEnd,
                        null,
                        workflowRunContext
                    )
                )
            }
        }

        val updatedAlerts = mutableListOf<Alert>()
        val triggerResults = mutableMapOf<String, QueryLevelTriggerRunResult>()

        // When multi-tenant trigger eval is enabled, batch-evaluate all query-level triggers
        // remotely on the user's cluster instead of running Painless locally
        val remoteTriggerResults = if (
            monitorCtx.multiTenantTriggerEvalEnabled &&
            Monitor.MonitorType.valueOf(monitor.monitorType.uppercase(Locale.ROOT)) == Monitor.MonitorType.QUERY_LEVEL_MONITOR &&
            monitorResult.inputResults.results.isNotEmpty()
        ) {
            val searchInput = monitor.inputs[0] as SearchInput
            val queryLevelTriggers = monitor.triggers.filterIsInstance<QueryLevelTrigger>()
            RemoteQueryLevelTriggerEvaluator.evaluate(
                monitorCtx.client!!,
                searchInput.indices,
                queryLevelTriggers,
                monitorResult.inputResults.results[0]
            )
        } else {
            null
        }

        val maxComments = monitorCtx.clusterService!!.clusterSettings.get(AlertingSettings.MAX_COMMENTS_PER_NOTIFICATION)
        val alertsToExecuteActionsForIds = currentAlerts.mapNotNull { it.value }.map { it.id }
        val allAlertsComments = CommentsUtils.getCommentsForAlertNotification(
            monitorCtx.client!!,
            alertsToExecuteActionsForIds,
            maxComments
        )

        for (trigger in monitor.triggers) {
            val currentAlert = currentAlerts[trigger]
            val currentAlertContext = currentAlert?.let {
                AlertContext(alert = currentAlert, comments = allAlertsComments[currentAlert.id])
            }

            val triggerCtx = QueryLevelTriggerExecutionContext(
                monitor,
                trigger,
                monitorResult,
                currentAlertContext,
                monitorCtx.clusterService!!.clusterSettings
            )
            val triggerResult = if (remoteTriggerResults != null) {
                // Use pre-computed remote evaluation results
                remoteTriggerResults[trigger.id]
                    ?: QueryLevelTriggerRunResult(trigger.name, false, null)
            } else {
                when (Monitor.MonitorType.valueOf(monitor.monitorType.uppercase(Locale.ROOT))) {
                    Monitor.MonitorType.QUERY_LEVEL_MONITOR ->
                        monitorCtx.triggerService!!.runQueryLevelTrigger(monitor, trigger as QueryLevelTrigger, triggerCtx)
                    Monitor.MonitorType.CLUSTER_METRICS_MONITOR -> {
                        val remoteMonitoringEnabled =
                            monitorCtx.clusterService!!.clusterSettings.get(AlertingSettings.CROSS_CLUSTER_MONITORING_ENABLED)
                        logger.debug("Remote monitoring enabled: {}", remoteMonitoringEnabled)
                        if (remoteMonitoringEnabled)
                            monitorCtx.triggerService!!.runClusterMetricsTrigger(
                                monitor, trigger as QueryLevelTrigger, triggerCtx, monitorCtx.clusterService!!
                            )
                        else monitorCtx.triggerService!!.runQueryLevelTrigger(monitor, trigger as QueryLevelTrigger, triggerCtx)
                    }
                    Monitor.MonitorType.PPL_MONITOR -> {
                        val pplTrigger = trigger as PPLTrigger

                        if (pplTrigger.conditionType == ConditionType.NUMBER_OF_RESULTS) {
                            // number of results trigger case
                            monitorCtx.triggerService!!.runPplNumResultsTrigger(
                                pplTrigger,
                                monitorResult.inputResults.pplBaseQueryNumResults
                            )
                        } else {
                            // custom condition trigger case
                            monitorCtx.triggerService!!.runPplCustomTrigger(
                                monitor,
                                pplTrigger,
                                (monitor.inputs[0] as PPLInput).query,
                                monitorCtx,
                                transportService
                            )
                        }
                    }
                    else ->
                        throw IllegalArgumentException("Unsupported monitor type: ${monitor.monitorType}.")
                }
            }

            triggerResults[trigger.id] = triggerResult

            // PPL Alerting:
            // what query results get stored in the Alert and Notification depends on the trigger type.
            // if number of results: evaluated on base query, so base query results are included.
            // if custom: the custom trigger ran its own query (base query + custom condition), so that query's results are included
            // trigger is not PPLTrigger: this is a query-level monitor run, simply include an empty list
            val pplQueryResultsToInclude = if (trigger is PPLTrigger && trigger.conditionType == ConditionType.NUMBER_OF_RESULTS) {
                monitorResult.inputResults.pplBaseQueryResults
            } else if (trigger is PPLTrigger && trigger.conditionType == ConditionType.CUSTOM) {
                triggerResult.pplCustomQueryResults
            } else {
                listOf()
            }

            // PPL Alerting:
            // triggerCtx hasn't been populated yet with the correct PPL query results
            // to include in the notification, so populate that here.
            // if this is a query-level monitor run, triggerCtx.pplQueryResults simply
            // stays an empty list
            val postRunTriggerCtx = triggerCtx.copy(
                pplQueryResults = pplQueryResultsToInclude
            )

            if (monitorCtx.triggerService!!.isQueryLevelTriggerActionable(triggerCtx, triggerResult, workflowRunContext)) {
                val actionCtx = postRunTriggerCtx.copy(error = monitorResult.error ?: triggerResult.error)
                for (action in trigger.actions) {
                    triggerResult.actionResults[action.id] = this.runAction(action, actionCtx, monitorCtx, monitor, dryrun)
                }
            }

            val updatedAlert = monitorCtx.alertService!!.composeQueryLevelAlert(
                postRunTriggerCtx,
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
