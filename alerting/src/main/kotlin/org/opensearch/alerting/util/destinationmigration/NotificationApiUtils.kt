/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.destinationmigration

import org.apache.logging.log4j.LogManager
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.alerting.opensearchapi.retryForNotification
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.client.node.NodeClient
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.notifications.NotificationsPluginInterface
import org.opensearch.commons.notifications.action.CreateNotificationConfigRequest
import org.opensearch.commons.notifications.action.CreateNotificationConfigResponse
import org.opensearch.commons.notifications.action.DeleteNotificationConfigRequest
import org.opensearch.commons.notifications.action.DeleteNotificationConfigResponse
import org.opensearch.commons.notifications.action.GetNotificationConfigRequest
import org.opensearch.commons.notifications.action.GetNotificationConfigResponse
import org.opensearch.commons.notifications.action.SendNotificationRequest
import org.opensearch.commons.notifications.action.SendNotificationResponse
import org.opensearch.commons.notifications.action.UpdateNotificationConfigRequest
import org.opensearch.commons.notifications.action.UpdateNotificationConfigResponse

class NotificationApiUtils {

    companion object {

        private val logger = LogManager.getLogger(NotificationApiUtils::class)

        private val defaultRetryPolicy =
            BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(100), 2)

        suspend fun getNotificationConfig(
            client: NodeClient,
            getNotificationConfigRequest: GetNotificationConfigRequest,
            retryPolicy: BackoffPolicy = defaultRetryPolicy
        ): GetNotificationConfigResponse {
            lateinit var getNotificationConfigResponse: GetNotificationConfigResponse
            val userStr = client.threadPool().threadContext
                .getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
            client.threadPool().threadContext.stashContext().use {
                client.threadPool().threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, userStr)
                retryPolicy.retryForNotification(logger) {
                    getNotificationConfigResponse = NotificationsPluginInterface.suspendUntil {
                        this.getNotificationConfig(
                            client,
                            getNotificationConfigRequest,
                            it
                        )
                    }
                }
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

        suspend fun updateNotificationConfig(
            client: NodeClient,
            updateNotificationConfigRequest: UpdateNotificationConfigRequest,
            retryPolicy: BackoffPolicy = defaultRetryPolicy
        ): UpdateNotificationConfigResponse {
            lateinit var updateNotificationConfigResponse: UpdateNotificationConfigResponse
            val userStr = client.threadPool()
                .threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
            client.threadPool().threadContext.stashContext().use {
                client.threadPool().threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, userStr)
                retryPolicy.retryForNotification(logger) {
                    updateNotificationConfigResponse = NotificationsPluginInterface.suspendUntil {
                        this.updateNotificationConfig(
                            client,
                            updateNotificationConfigRequest,
                            it
                        )
                    }
                }
            }
            return updateNotificationConfigResponse
        }

        suspend fun deleteNotificationConfig(
            client: NodeClient,
            deleteNotificationConfigRequest: DeleteNotificationConfigRequest,
            retryPolicy: BackoffPolicy = defaultRetryPolicy
        ): DeleteNotificationConfigResponse {
            lateinit var deleteNotificationConfigResponse: DeleteNotificationConfigResponse
            val userStr = client.threadPool()
                .threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
            client.threadPool().threadContext.stashContext().use {
                client.threadPool().threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, userStr)
                retryPolicy.retryForNotification(logger) {
                    deleteNotificationConfigResponse = NotificationsPluginInterface.suspendUntil {
                        this.deleteNotificationConfig(
                            client,
                            deleteNotificationConfigRequest,
                            it
                        )
                    }
                }
            }
            return deleteNotificationConfigResponse
        }

        suspend fun sendNotification(
            client: NodeClient,
            sendNotificationRequest: SendNotificationRequest,
            retryPolicy: BackoffPolicy = defaultRetryPolicy
        ): SendNotificationResponse {
            lateinit var sendNotificationResponse: SendNotificationResponse
            val userStr = client.threadPool().threadContext
                .getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
            client.threadPool().threadContext.stashContext().use {
                client.threadPool().threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, userStr)
                retryPolicy.retryForNotification(logger) {
                    sendNotificationResponse = NotificationsPluginInterface.suspendUntil {
                        this.sendNotification(
                            client,
                            sendNotificationRequest.eventSource,
                            sendNotificationRequest.channelMessage,
                            sendNotificationRequest.channelIds,
                            it
                        )
                    }
                }
            }
            return sendNotificationResponse
        }
    }
}
