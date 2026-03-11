/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.lucene.search.TotalHits
import org.apache.lucene.search.TotalHits.Relation
import org.opensearch.OpenSearchSecurityException
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.search.ShardSearchFailure
import org.opensearch.alerting.AlertingPlugin.Companion.MONITOR_BASE_URI
import org.opensearch.alerting.AlertingPlugin.Companion.MONITOR_V2_BASE_URI
import org.opensearch.alerting.action.GetDestinationsAction
import org.opensearch.alerting.action.GetDestinationsRequest
import org.opensearch.alerting.action.GetDestinationsResponse
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.util.destinationmigration.NotificationActionConfigs
import org.opensearch.alerting.util.destinationmigration.NotificationApiUtils.Companion.getNotificationConfigInfo
import org.opensearch.alerting.util.destinationmigration.getTitle
import org.opensearch.alerting.util.destinationmigration.publishLegacyNotification
import org.opensearch.alerting.util.destinationmigration.sendNotification
import org.opensearch.alerting.util.isAllowed
import org.opensearch.alerting.util.isTestAction
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Table
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.model.action.Action
import org.opensearch.commons.notifications.model.NotificationConfigInfo
import org.opensearch.index.IndexNotFoundException
import org.opensearch.search.SearchHits
import org.opensearch.search.aggregations.InternalAggregations
import org.opensearch.search.internal.InternalSearchResponse
import org.opensearch.search.profile.SearchProfileShardResults
import org.opensearch.search.suggest.Suggest
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.client.node.NodeClient
import java.util.Collections

object AlertingV2Utils {
    // Validates that the given scheduled job is a Monitor
    // returns the exception to pass into actionListener.onFailure if not.
    fun validateMonitorV1(scheduledJob: ScheduledJob): Exception? {
        if (scheduledJob is MonitorV2) {
            return IllegalStateException(
                "The ID given corresponds to an Alerting V2 Monitor, but a V1 Monitor was expected. " +
                    "If you wish to operate on a V2 Monitor (e.g. PPL Monitor), please use " +
                    "the Alerting V2 APIs with endpoint prefix: $MONITOR_V2_BASE_URI."
            )
        } else if (scheduledJob !is Monitor && scheduledJob !is Workflow) {
            return IllegalStateException(
                "The ID given corresponds to a scheduled job of unknown type: ${scheduledJob.javaClass.name}. " +
                    "Please validate the ID and ensure it corresponds to a valid Monitor."
            )
        }
        return null
    }

    // Validates that the given scheduled job is a MonitorV2
    // returns the exception to pass into actionListener.onFailure if not.
    fun validateMonitorV2(scheduledJob: ScheduledJob): Exception? {
        if (scheduledJob is Monitor || scheduledJob is Workflow) {
            return IllegalStateException(
                "The ID given corresponds to an Alerting V1 Monitor, but a V2 Monitor was expected. " +
                    "If you wish to operate on a V1 Monitor (e.g. Per Query, Per Document, etc), please use " +
                    "the Alerting V1 APIs with endpoint prefix: $MONITOR_BASE_URI."
            )
        } else if (scheduledJob !is MonitorV2) {
            return IllegalStateException(
                "The ID given corresponds to a scheduled job of unknown type: ${scheduledJob.javaClass.name}. " +
                    "Please validate the ID and ensure it corresponds to a valid Monitor."
            )
        }
        return null
    }

    // Checks if the exception is caused by an IndexNotFoundException (directly or nested).
    // Used in Get and Search monitor functionalities to determine whether a "no results"
    // response should be returned
    fun isIndexNotFoundException(e: Exception): Boolean {
        if (e is IndexNotFoundException) {
            return true
        }
        if (e is RemoteTransportException) {
            val cause = e.cause
            if (cause is IndexNotFoundException) {
                return true
            }
        }
        return false
    }

    // Used in Get and Search monitor functionalities to return a "no results" response
    fun getEmptySearchResponse(): SearchResponse {
        val internalSearchResponse = InternalSearchResponse(
            SearchHits(emptyArray(), TotalHits(0L, Relation.EQUAL_TO), 0.0f),
            InternalAggregations.from(Collections.emptyList()),
            Suggest(Collections.emptyList()),
            SearchProfileShardResults(Collections.emptyMap()),
            false,
            false,
            0
        )

        return SearchResponse(
            internalSearchResponse,
            "",
            0,
            0,
            0,
            0,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        )
    }

    suspend fun getConfigAndSendNotification(
        action: Action,
        monitorCtx: MonitorRunnerExecutionContext,
        subject: String?,
        message: String
    ): String {
        val config = getConfigForNotificationAction(action, monitorCtx)
        if (config.destination == null && config.channel == null) {
            throw IllegalStateException("Unable to find a Notification Channel or Destination config with id [${action.destinationId}]")
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
                message
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
}
