/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.destinationmigration

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.alerting.elasticapi.retryForNotification
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

        fun getNotificationConfig(
            client: NodeClient,
            getNotificationConfigRequest: GetNotificationConfigRequest,
            retryPolicy: BackoffPolicy = defaultRetryPolicy
        ): GetNotificationConfigResponse {
            var getNotificationConfigResponse: GetNotificationConfigResponse? = null
            var exception: Exception?
            var completed = false
            val userStr = client.threadPool().threadContext
                .getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
            client.threadPool().threadContext.stashContext().use {
                client.threadPool().threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, userStr)
                retryPolicy.retryForNotification {
                    exception = null
                    NotificationsPluginInterface.getNotificationConfig(
                        client,
                        getNotificationConfigRequest,
                        object : ActionListener<GetNotificationConfigResponse> {
                            override fun onResponse(response: GetNotificationConfigResponse) {
                                getNotificationConfigResponse = response
                                logger.debug("Retrieved notification(s) successfully: $response")
                                completed = true
                            }

                            override fun onFailure(e: Exception) {
                                logger.error("Failed to retrieve Notification due to: ${e.message}", e)
                                exception = e
                                completed = true
                            }
                        }
                    )
                    while (!completed) {
                        Thread.sleep(100)
                    }
                    completed = false
                    if (exception != null) {
                        throw exception as Exception
                    }
                }
            }
            return getNotificationConfigResponse!!
        }

        fun createNotificationConfig(
            client: NodeClient,
            createNotificationConfigRequest: CreateNotificationConfigRequest,
            retryPolicy: BackoffPolicy = defaultRetryPolicy
        ): CreateNotificationConfigResponse {
            var createNotificationConfigResponse: CreateNotificationConfigResponse? = null
            var exception: Exception?
            var completed = false
            val userStr = client.threadPool().threadContext
                .getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
            client.threadPool().threadContext.stashContext().use {
                client.threadPool().threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, userStr)
                retryPolicy.retryForNotification {
                    exception = null
                    NotificationsPluginInterface.createNotificationConfig(
                        client,
                        createNotificationConfigRequest,
                        object : ActionListener<CreateNotificationConfigResponse> {
                            override fun onResponse(response: CreateNotificationConfigResponse) {
                                createNotificationConfigResponse = response
                                logger.debug("Created notification successfully: $response")
                                completed = true
                            }

                            override fun onFailure(e: Exception) {
                                logger.error("Failed to create Notification due to: ${e.message}", e)
                                exception = e
                                completed = true
                            }
                        }
                    )
                    while (!completed) {
                        Thread.sleep(100)
                    }
                    completed = false
                    if (exception != null) {
                        throw exception as Exception
                    }
                }
            }
            return createNotificationConfigResponse!!
        }

        fun updateNotificationConfig(
            client: NodeClient,
            updateNotificationConfigRequest: UpdateNotificationConfigRequest,
            retryPolicy: BackoffPolicy = defaultRetryPolicy
        ): UpdateNotificationConfigResponse {
            var updateNotificationConfigResponse: UpdateNotificationConfigResponse? = null
            var completed = false
            var exception: Exception?
            val userStr = client.threadPool()
                .threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
            client.threadPool().threadContext.stashContext().use {
                client.threadPool().threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, userStr)
                retryPolicy.retryForNotification {
                    exception = null
                    NotificationsPluginInterface.updateNotificationConfig(
                        client,
                        updateNotificationConfigRequest,
                        object : ActionListener<UpdateNotificationConfigResponse> {
                            override fun onResponse(response: UpdateNotificationConfigResponse) {
                                updateNotificationConfigResponse = response
                                logger.debug("Updated notification successfully: $response")
                                completed = true
                            }

                            override fun onFailure(e: Exception) {
                                logger.error("Failed to update Notification due to: ${e.message}", e)
                                exception = e
                                completed = true
                            }
                        }
                    )
                    while (!completed) {
                        Thread.sleep(100)
                    }
                    completed = false
                    if (exception != null) {
                        throw exception as Exception
                    }
                }
            }
            return updateNotificationConfigResponse!!
        }

        fun deleteNotificationConfig(
            client: NodeClient,
            deleteNotificationConfigRequest: DeleteNotificationConfigRequest,
            retryPolicy: BackoffPolicy = defaultRetryPolicy
        ): DeleteNotificationConfigResponse {
            var deleteNotificationConfigResponse: DeleteNotificationConfigResponse? = null
            var completed = false
            var exception: Exception?
            val userStr = client.threadPool()
                .threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
            client.threadPool().threadContext.stashContext().use {
                client.threadPool().threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, userStr)
                retryPolicy.retryForNotification {
                    exception = null
                    NotificationsPluginInterface.deleteNotificationConfig(
                        client,
                        deleteNotificationConfigRequest,
                        object : ActionListener<DeleteNotificationConfigResponse> {
                            override fun onResponse(response: DeleteNotificationConfigResponse) {
                                deleteNotificationConfigResponse = response
                                logger.debug("Deleted notification successfully: $response")
                                completed = true
                            }

                            override fun onFailure(e: Exception) {
                                logger.error("Failed to delete Notification due to: ${e.message}", e)
                                exception = e
                                completed = true
                            }
                        }
                    )
                    while (!completed) {
                        Thread.sleep(100)
                    }
                    completed = false
                    if (exception != null) {
                        throw exception as Exception
                    }
                }
            }
            return deleteNotificationConfigResponse!!
        }

        fun sendNotification(
            client: NodeClient,
            sendNotificationRequest: SendNotificationRequest,
            retryPolicy: BackoffPolicy = defaultRetryPolicy
        ): SendNotificationResponse {
            var sendNotificationResponse: SendNotificationResponse? = null
            var completed = false
            var exception: Exception?
            val userStr = client.threadPool().threadContext
                .getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
            client.threadPool().threadContext.stashContext().use {
                client.threadPool().threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, userStr)
                retryPolicy.retryForNotification {
                    exception = null
                    NotificationsPluginInterface.sendNotification(
                        client,
                        sendNotificationRequest.eventSource,
                        sendNotificationRequest.channelMessage,
                        sendNotificationRequest.channelIds,
                        object : ActionListener<SendNotificationResponse> {
                            override fun onResponse(response: SendNotificationResponse) {
                                sendNotificationResponse = response
                                logger.debug("Sent notification successfully: $response")
                                completed = true
                            }

                            override fun onFailure(e: Exception) {
                                logger.error("Failed to send Notification due to: ${e.message}", e)
                                exception = e
                                completed = true
                            }
                        }
                    )
                    while (!completed) {
                        Thread.sleep(100)
                    }
                    completed = false
                    if (exception != null) {
                        throw exception as Exception
                    }
                }
            }
            return sendNotificationResponse!!
        }
    }
}
