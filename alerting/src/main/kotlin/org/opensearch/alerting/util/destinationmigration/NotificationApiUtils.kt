/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.destinationmigration

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchSecurityException
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.opensearchapi.retryForNotification
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.common.Strings
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.destination.message.LegacyBaseMessage
import org.opensearch.commons.notifications.NotificationsPluginInterface
import org.opensearch.commons.notifications.action.CreateNotificationConfigRequest
import org.opensearch.commons.notifications.action.CreateNotificationConfigResponse
import org.opensearch.commons.notifications.action.GetNotificationConfigRequest
import org.opensearch.commons.notifications.action.GetNotificationConfigResponse
import org.opensearch.commons.notifications.action.LegacyPublishNotificationRequest
import org.opensearch.commons.notifications.action.LegacyPublishNotificationResponse
import org.opensearch.commons.notifications.action.SendNotificationResponse
import org.opensearch.commons.notifications.model.ChannelMessage
import org.opensearch.commons.notifications.model.ConfigType
import org.opensearch.commons.notifications.model.EventSource
import org.opensearch.commons.notifications.model.NotificationConfigInfo
import org.opensearch.commons.notifications.model.SeverityType
import org.opensearch.rest.RestStatus

class NotificationApiUtils {

    companion object {

        private val logger = LogManager.getLogger(NotificationApiUtils::class)

        private val defaultRetryPolicy =
            BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(100), 2)

        /**
         * Gets a NotificationConfigInfo object by ID if it exists.
         */
        suspend fun getNotificationConfigInfo(client: NodeClient, id: String): NotificationConfigInfo? {
            return try {
                val res: GetNotificationConfigResponse = getNotificationConfig(client, GetNotificationConfigRequest(setOf(id)))
                res.searchResult.objectList.firstOrNull()
            } catch (e: OpenSearchSecurityException) {
                throw e
            } catch (e: OpenSearchStatusException) {
                if (e.status() == RestStatus.NOT_FOUND) {
                    logger.debug("Notification config [$id] was not found")
                }
                null
            }
        }

        private suspend fun getNotificationConfig(
            client: NodeClient,
            getNotificationConfigRequest: GetNotificationConfigRequest
        ): GetNotificationConfigResponse {
            val getNotificationConfigResponse: GetNotificationConfigResponse = NotificationsPluginInterface.suspendUntil {
                this.getNotificationConfig(
                    client,
                    getNotificationConfigRequest,
                    it
                )
            }
            return getNotificationConfigResponse
        }

        suspend fun createNotificationConfig(
            client: NodeClient,
            createNotificationConfigRequest: CreateNotificationConfigRequest,
            retryPolicy: BackoffPolicy = defaultRetryPolicy
        ): CreateNotificationConfigResponse {
            lateinit var createNotificationConfigResponse: CreateNotificationConfigResponse
            val userStr = client.threadPool().threadContext
                .getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
            client.threadPool().threadContext.stashContext().use {
                client.threadPool().threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, userStr)
                retryPolicy.retryForNotification(logger) {
                    createNotificationConfigResponse = NotificationsPluginInterface.suspendUntil {
                        this.createNotificationConfig(
                            client,
                            createNotificationConfigRequest,
                            it
                        )
                    }
                }
            }
            return createNotificationConfigResponse
        }
    }
}

/**
 * Extension function for publishing a notification to a legacy destination.
 *
 * We now support the new channels from the Notification plugin. However, we still need to support
 * the old legacy destinations that have not been migrated to Notification configs. To accommodate this even after removing the
 * notification logic in Alerting, we have a separate API in the NotificationsPluginInterface that allows
 * us to publish these old legacy ones directly.
 */
suspend fun LegacyBaseMessage.publishLegacyNotification(client: Client): String {
    val baseMessage = this
    val res: LegacyPublishNotificationResponse = NotificationsPluginInterface.suspendUntil {
        this.publishLegacyNotification(
            (client as NodeClient),
            LegacyPublishNotificationRequest(baseMessage),
            it
        )
    }
    validateResponseStatus(RestStatus.fromCode(res.destinationResponse.statusCode), res.destinationResponse.responseContent)
    return res.destinationResponse.responseContent
}

/**
 * Extension function for publishing a notification to a channel in the Notification plugin.
 */
suspend fun NotificationConfigInfo.sendNotification(client: Client, title: String, compiledMessage: String): String {
    val config = this
    val res: SendNotificationResponse = NotificationsPluginInterface.suspendUntil {
        this.sendNotification(
            (client as NodeClient),
            EventSource(title, config.configId, SeverityType.INFO),
            ChannelMessage(compiledMessage, null, null),
            listOf(config.configId),
            it
        )
    }
    validateResponseStatus(res.getStatus(), res.notificationEvent.toString())
    return res.notificationEvent.toString()
}

/**
 * For most channel types, a placeholder Alerting title will be used but the email channel will
 * use the subject, so it appears as the actual subject of the email.
 */
fun NotificationConfigInfo.getTitle(subject: String?): String {
    val defaultTitle = "Alerting-Notification Action"
    if (this.notificationConfig.configType == ConfigType.EMAIL) {
        return if (subject.isNullOrEmpty()) defaultTitle else subject
    }

    return defaultTitle
}

fun NotificationConfigInfo.createMessageContent(subject: String?, message: String): String {
    // For Email Channels, the subject is not passed in the main message since it's used as the title
    if (this.notificationConfig.configType == ConfigType.EMAIL) {
        return constructMessageContent("", message)
    }

    return constructMessageContent(subject, message)
}

/**
 * Similar to Destinations, this is a generic utility method for constructing message content from
 * a subject and message body when sending through Notifications since the Action definition in Monitors can have both.
 */
private fun constructMessageContent(subject: String?, message: String): String {
    return if (Strings.isNullOrEmpty(subject)) message else "$subject \n\n $message"
}

/**
 * All valid response statuses.
 */
private val VALID_RESPONSE_STATUS = setOf(
    RestStatus.OK.status, RestStatus.CREATED.status, RestStatus.ACCEPTED.status,
    RestStatus.NON_AUTHORITATIVE_INFORMATION.status, RestStatus.NO_CONTENT.status,
    RestStatus.RESET_CONTENT.status, RestStatus.PARTIAL_CONTENT.status,
    RestStatus.MULTI_STATUS.status
)

@Throws(OpenSearchStatusException::class)
fun validateResponseStatus(restStatus: RestStatus, responseContent: String) {
    if (!VALID_RESPONSE_STATUS.contains(restStatus.status)) {
        throw OpenSearchStatusException("Failed: $responseContent", restStatus)
    }
}

/**
 * Small data class used to hold either a Destination or a Notification channel config.
 * This is used since an ID being referenced in a Monitor action could be either config depending on if
 * it's prior to or after migration.
 */
data class NotificationActionConfigs(val destination: Destination?, val channel: NotificationConfigInfo?)
