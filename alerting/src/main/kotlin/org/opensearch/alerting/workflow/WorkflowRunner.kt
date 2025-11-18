/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.workflow

import org.opensearch.alerting.AlertingV2Utils.getConfigAndSendNotification
import org.opensearch.alerting.MonitorRunnerExecutionContext
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.opensearchapi.InjectorContextElement
import org.opensearch.alerting.opensearchapi.withClosableContext
import org.opensearch.alerting.script.ChainedAlertTriggerExecutionContext
import org.opensearch.alerting.util.use
import org.opensearch.commons.alerting.model.ActionRunResult
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.model.WorkflowRunResult
import org.opensearch.commons.alerting.model.action.Action
import org.opensearch.core.common.Strings
import org.opensearch.script.Script
import org.opensearch.script.TemplateScript
import org.opensearch.transport.TransportService
import java.time.Instant

abstract class WorkflowRunner {
    abstract suspend fun runWorkflow(
        workflow: Workflow,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryRun: Boolean,
        transportService: TransportService
    ): WorkflowRunResult

    suspend fun runAction(
        action: Action,
        ctx: ChainedAlertTriggerExecutionContext,
        monitorCtx: MonitorRunnerExecutionContext,
        workflow: Workflow,
        dryrun: Boolean
    ): ActionRunResult {
        return try {
            if (!MonitorRunnerService.isActionActionable(action, ctx.alert)) {
                return ActionRunResult(action.id, action.name, mapOf(), true, null, null)
            }
            val actionOutput = mutableMapOf<String, String>()
            actionOutput[Action.SUBJECT] = if (action.subjectTemplate != null) {
                compileTemplate(action.subjectTemplate!!, ctx)
            } else ""
            actionOutput[Action.MESSAGE] = compileTemplate(action.messageTemplate, ctx)
            if (Strings.isNullOrEmpty(actionOutput[Action.MESSAGE])) {
                throw IllegalStateException("Message content missing in the Destination with id: ${action.destinationId}")
            }
            if (!dryrun) {
                val client = monitorCtx.client
                client!!.threadPool().threadContext.stashContext().use {
                    withClosableContext(
                        InjectorContextElement(
                            workflow.id,
                            monitorCtx.settings!!,
                            monitorCtx.threadPool!!.threadContext,
                            workflow.user?.roles,
                            workflow.user
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

    internal fun compileTemplate(template: Script, ctx: ChainedAlertTriggerExecutionContext): String {
        return MonitorRunnerService.monitorCtx.scriptService!!.compile(template, TemplateScript.CONTEXT)
            .newInstance(template.params + mapOf("ctx" to ctx.asTemplateArg()))
            .execute()
    }
}
