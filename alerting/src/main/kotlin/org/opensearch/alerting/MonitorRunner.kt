/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.AlertingV2Utils.getConfigAndSendNotification
import org.opensearch.alerting.opensearchapi.InjectorContextElement
import org.opensearch.alerting.opensearchapi.withClosableContext
import org.opensearch.alerting.script.QueryLevelTriggerExecutionContext
import org.opensearch.alerting.script.TriggerExecutionContext
import org.opensearch.alerting.util.use
import org.opensearch.commons.alerting.model.ActionRunResult
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorRunResult
import org.opensearch.commons.alerting.model.WorkflowRunContext
import org.opensearch.commons.alerting.model.action.Action
import org.opensearch.core.common.Strings
import org.opensearch.transport.TransportService
import java.time.Instant

abstract class MonitorRunner {

    abstract suspend fun runMonitor(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryRun: Boolean,
        workflowRunContext: WorkflowRunContext? = null,
        executionId: String,
        transportService: TransportService
    ): MonitorRunResult<*>

    suspend fun runAction(
        action: Action,
        ctx: TriggerExecutionContext,
        monitorCtx: MonitorRunnerExecutionContext,
        monitor: Monitor,
        dryrun: Boolean
    ): ActionRunResult {
        return try {
            if (ctx is QueryLevelTriggerExecutionContext && !MonitorRunnerService.isActionActionable(action, ctx.alert?.alert)) {
                return ActionRunResult(action.id, action.name, mapOf(), true, null, null)
            }
            val actionOutput = mutableMapOf<String, String>()
            actionOutput[Action.SUBJECT] = if (action.subjectTemplate != null)
                MonitorRunnerService.compileTemplate(action.subjectTemplate!!, ctx)
            else ""
            actionOutput[Action.MESSAGE] = MonitorRunnerService.compileTemplate(action.messageTemplate, ctx)
            if (Strings.isNullOrEmpty(actionOutput[Action.MESSAGE])) {
                throw IllegalStateException("Message content missing in the Destination with id: ${action.destinationId}")
            }
            if (!dryrun) {
                val client = monitorCtx.client
                client!!.threadPool().threadContext.stashContext().use {
                    withClosableContext(
                        InjectorContextElement(
                            monitor.id,
                            monitorCtx.settings!!,
                            monitorCtx.threadPool!!.threadContext,
                            monitor.user?.roles,
                            monitor.user
                        )
                    ) {
                        actionOutput[Action.MESSAGE_ID] = getConfigAndSendNotification(
                            action,
                            monitorCtx,
                            actionOutput[Action.SUBJECT],
                            actionOutput[Action.MESSAGE]!!
                        )
                    }
                }
            }
            ActionRunResult(action.id, action.name, actionOutput, false, MonitorRunnerService.currentTime(), null)
        } catch (e: Exception) {
            ActionRunResult(action.id, action.name, mapOf(), false, MonitorRunnerService.currentTime(), e)
        }
    }
}
