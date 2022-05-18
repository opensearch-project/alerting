/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.OpenSearchSecurityException
import org.opensearch.alerting.action.GetDestinationsAction
import org.opensearch.alerting.action.GetDestinationsRequest
import org.opensearch.alerting.action.GetDestinationsResponse
import org.opensearch.alerting.model.ActionRunResult
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.MonitorMetadata
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.Table
import org.opensearch.alerting.model.action.Action
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.opensearchapi.InjectorContextElement
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.opensearchapi.withClosableContext
import org.opensearch.alerting.script.QueryLevelTriggerExecutionContext
import org.opensearch.alerting.script.TriggerExecutionContext
import org.opensearch.alerting.util.destinationmigration.NotificationActionConfigs
import org.opensearch.alerting.util.destinationmigration.NotificationApiUtils.Companion.getNotificationConfigInfo
import org.opensearch.alerting.util.destinationmigration.createMessageContent
import org.opensearch.alerting.util.destinationmigration.getTitle
import org.opensearch.alerting.util.destinationmigration.publishLegacyNotification
import org.opensearch.alerting.util.destinationmigration.sendNotification
import org.opensearch.alerting.util.isAllowed
import org.opensearch.alerting.util.isTestAction
import org.opensearch.client.node.NodeClient
import org.opensearch.common.Strings
import org.opensearch.commons.notifications.model.NotificationConfigInfo
import java.time.Instant

abstract class MonitorRunner {

    abstract suspend fun runMonitor(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryRun: Boolean
    ): MonitorRunResult<*>

    suspend fun runAction(
        action: Action,
        ctx: TriggerExecutionContext,
        monitorCtx: MonitorRunnerExecutionContext,
        monitor: Monitor,
        dryrun: Boolean
    ): ActionRunResult {
        return try {
            if (ctx is QueryLevelTriggerExecutionContext && !MonitorRunnerService.isActionActionable(action, ctx.alert)) {
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
                val roles = MonitorRunnerService.getRolesForMonitor(monitor)
                withClosableContext(
                    InjectorContextElement(monitor.id, monitorCtx.settings!!, monitorCtx.threadPool!!.threadContext, roles)
                ) {
                    actionOutput[Action.MESSAGE_ID] = getConfigAndSendNotification(
                        action,
                        monitorCtx,
                        actionOutput[Action.SUBJECT],
                        actionOutput[Action.MESSAGE]!!
                    )
                }
            }
            ActionRunResult(action.id, action.name, actionOutput, false, MonitorRunnerService.currentTime(), null)
        } catch (e: Exception) {
            ActionRunResult(action.id, action.name, mapOf(), false, MonitorRunnerService.currentTime(), e)
        }
    }

    protected suspend fun getConfigAndSendNotification(
        action: Action,
        monitorCtx: MonitorRunnerExecutionContext,
        subject: String?,
        message: String
    ): String {
        val config = getConfigForNotificationAction(action, monitorCtx)
        if (config.destination == null && config.channel == null) {
            throw IllegalStateException("Unable to find a Notification Channel or Destination config with id [${action.id}]")
        }

        // Adding a check on TEST_ACTION Destination type here to avoid supporting it as a LegacyBaseMessage type
        // just for Alerting integration tests
        if (config.destination?.isTestAction() == true) {
            return "test action"
        }

        if (config.destination?.isAllowed(monitorCtx.allowList) == false) {
            throw IllegalStateException(
                "Monitor contains a Destination type that is not allowed: ${config.destination.type}"
            )
        }

        var actionResponseContent = ""
        actionResponseContent = config.channel
            ?.sendNotification(
                monitorCtx.client!!,
                config.channel.getTitle(subject),
                config.channel.createMessageContent(subject, message)
            ) ?: actionResponseContent

        actionResponseContent = config.destination
            ?.buildLegacyBaseMessage(subject, message, monitorCtx.destinationContextFactory!!.getDestinationContext(config.destination))
            ?.publishLegacyNotification(monitorCtx.client!!)
            ?: actionResponseContent

        return actionResponseContent
    }

    /**
     * The "destination" ID referenced in a Monitor Action could either be a Notification config or a Destination config
     * depending on whether the background migration process has already migrated it from a Destination to a Notification config.
     *
     * To cover both of these cases, the Notification config will take precedence and if it is not found, the Destination will be retrieved.
     */
    private suspend fun getConfigForNotificationAction(
        action: Action,
        monitorCtx: MonitorRunnerExecutionContext
    ): NotificationActionConfigs {
        var destination: Destination? = null
        var notificationPermissionException: Exception? = null

        var channel: NotificationConfigInfo? = null
        try {
            channel = getNotificationConfigInfo(monitorCtx.client as NodeClient, action.destinationId)
        } catch (e: OpenSearchSecurityException) {
            notificationPermissionException = e
        }

        // If the channel was not found, try to retrieve the Destination
        if (channel == null) {
            destination = try {
                val table = Table(
                    "asc",
                    "destination.name.keyword",
                    null,
                    1,
                    0,
                    null
                )
                val getDestinationsRequest = GetDestinationsRequest(
                    action.destinationId,
                    0L,
                    null,
                    table,
                    "ALL"
                )

                val getDestinationsResponse: GetDestinationsResponse = monitorCtx.client!!.suspendUntil {
                    monitorCtx.client!!.execute(GetDestinationsAction.INSTANCE, getDestinationsRequest, it)
                }
                getDestinationsResponse.destinations.firstOrNull()
            } catch (e: IllegalStateException) {
                // Catching the exception thrown when the Destination was not found so the NotificationActionConfigs object can be returned
                null
            } catch (e: OpenSearchSecurityException) {
                if (notificationPermissionException != null)
                    throw notificationPermissionException
                else
                    throw e
            }

            if (destination == null && notificationPermissionException != null)
                throw notificationPermissionException
        }

        return NotificationActionConfigs(destination, channel)
    }

    protected fun createMonitorMetadata(monitorId: String): MonitorMetadata {
        return MonitorMetadata("$monitorId-metadata", monitorId, emptyList(), emptyMap())
    }
}
