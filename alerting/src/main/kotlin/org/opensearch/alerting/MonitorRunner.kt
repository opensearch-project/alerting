/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.opensearchapi.InjectorContextElement
import org.opensearch.alerting.opensearchapi.withClosableContext
import org.opensearch.alerting.script.QueryLevelTriggerExecutionContext
import org.opensearch.alerting.script.TriggerExecutionContext
import org.opensearch.alerting.service.MonitorJobPoller
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.ArnHelper
import org.opensearch.alerting.util.getConfigAndSendNotification
import org.opensearch.alerting.util.use
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.alerting.model.ActionRunResult
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorRunResult
import org.opensearch.commons.alerting.model.WorkflowRunContext
import org.opensearch.commons.alerting.model.action.Action
import org.opensearch.commons.utils.currentTenantId
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
                val userStr = client!!.threadPool().threadContext
                    .getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
                val tenantId = currentTenantId()
                client.threadPool().threadContext.stashContext().use {
                    client.threadPool().threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, userStr)
                    tenantId?.let { client.threadPool().threadContext.putHeader(AlertingPlugin.TENANT_ID_HEADER, it) }
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

    protected fun reinjectHeaders(monitor: Monitor, monitorCtx: MonitorRunnerExecutionContext) {
        val target = monitor.target ?: return
        val threadContext = monitorCtx.client!!.threadPool().threadContext
        val settings = monitorCtx.settings!!

        if (threadContext.getHeader(MonitorJobPoller.IS_BACKGROUND_JOB_HEADER) == null) {
            threadContext.putHeader(MonitorJobPoller.IS_BACKGROUND_JOB_HEADER, "true")
        }
        if (threadContext.getHeader(MonitorJobPoller.OPENSEARCH_ENDPOINT_HEADER) == null) {
            threadContext.putHeader(MonitorJobPoller.OPENSEARCH_ENDPOINT_HEADER, target.endpoint)
        }
        if (threadContext.getHeader(MonitorJobPoller.SERVICE_NAME_HEADER) == null) {
            val serviceNameMap = AlertingSettings.TARGET_TYPE_TO_SERVICE_NAME.get(settings)
                .let { it.keySet().associateWith { key -> it.get(key) } }
            threadContext.putHeader(MonitorJobPoller.SERVICE_NAME_HEADER, serviceNameMap[target.type] ?: target.type)
        }
        if (threadContext.getHeader(MonitorJobPoller.REGION_HEADER) == null) {
            threadContext.putHeader(MonitorJobPoller.REGION_HEADER, AlertingSettings.REMOTE_METADATA_REGION.get(settings) ?: "")
        }

        if (target.arn.isNotBlank()) {
            val (accountId, resourceId) = ArnHelper.parseArn(target.arn)
            val accountHeader = AlertingSettings.TENANT_ACCOUNT_ID_HEADER.get(settings) ?: ""
            if (accountHeader.isNotEmpty() && threadContext.getTransient<String>(accountHeader) == null) {
                threadContext.putTransient(accountHeader, accountId)
            }
            val resourceHeaders = AlertingSettings.TENANT_RESOURCE_ID_HEADER.get(settings)
            val resourceHeader = resourceHeaders?.get(target.type)
            if (!resourceHeader.isNullOrEmpty() && threadContext.getTransient<String>(resourceHeader) == null) {
                threadContext.putTransient(resourceHeader, resourceId)
            }
        }
    }
}
