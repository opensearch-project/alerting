package org.opensearch.alerting.util

import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.callbackFlow
import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.client.node.NodeClient
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
import java.lang.RuntimeException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

//TODO consider adding retry policies
class NotificationAPIUtils {

    companion object {

        private val logger = LogManager.getLogger(javaClass)

        fun getNotificationConfig(client: NodeClient, getNotificationConfigRequest: GetNotificationConfigRequest): GetNotificationConfigResponse? {
            var getNotificationConfigResponse: GetNotificationConfigResponse? = null
            var completed = false
            NotificationsPluginInterface.getNotificationConfig(client, getNotificationConfigRequest,
                object : ActionListener<GetNotificationConfigResponse> {
                    override fun onResponse(response: GetNotificationConfigResponse) {
                        getNotificationConfigResponse = response
                        completed = true
                    }
                    override fun onFailure(e: Exception) {
                        logger.error("Failed to retrieve Notification due to: ${e.message}", e)
                        completed = true
                        throw e
                    }
                }
            )
            while(!completed) {
                Thread.sleep(100)
            }
            return getNotificationConfigResponse
        }

        fun createNotificationConfig(client: NodeClient, createNotificationConfigRequest: CreateNotificationConfigRequest): CreateNotificationConfigResponse {
            var createNotificationConfigResponse: CreateNotificationConfigResponse? = null
            var exception: Exception? = null
            var completed = false
            NotificationsPluginInterface.createNotificationConfig(client, createNotificationConfigRequest,
                object : ActionListener<CreateNotificationConfigResponse> {
                    override fun onResponse(response: CreateNotificationConfigResponse) {
                        createNotificationConfigResponse = response
                        logger.info("created notification successfully: $response")
                        completed = true
                    }

                    override fun onFailure(e: Exception) {
                        logger.error("Failed to create Notification due to: ${e.message}", e)
                        exception = e
                        completed = true
                    }
                }
            )
            while(!completed) {
                Thread.sleep(100)
            }
            if (exception != null) throw exception as Exception
            if (createNotificationConfigResponse == null) {
                throw RuntimeException("Some reason cannot create notification")
            }
            return createNotificationConfigResponse!!
        }

        fun updateNotificationConfig(client: NodeClient, updateNotificationConfigRequest: UpdateNotificationConfigRequest): UpdateNotificationConfigResponse {
            var updateNotificationConfigResponse: UpdateNotificationConfigResponse? = null
            var completed = false
            NotificationsPluginInterface.updateNotificationConfig(client, updateNotificationConfigRequest,
                object : ActionListener<UpdateNotificationConfigResponse> {
                    override fun onResponse(response: UpdateNotificationConfigResponse) {
                        updateNotificationConfigResponse = response
                        completed = true
                    }
                    override fun onFailure(e: Exception) {
                        completed = true
                        throw e
                    }
                }
            )
            while(!completed) {
                Thread.sleep(100)
            }
            return updateNotificationConfigResponse!!
        }

        fun deleteNotificationConfig(client: NodeClient, deleteNotificationConfigRequest: DeleteNotificationConfigRequest): DeleteNotificationConfigResponse {
            var deleteNotificationConfigResponse: DeleteNotificationConfigResponse? = null
            var completed = false
            NotificationsPluginInterface.deleteNotificationConfig(client, deleteNotificationConfigRequest,
                object : ActionListener<DeleteNotificationConfigResponse> {
                    override fun onResponse(response: DeleteNotificationConfigResponse) {
                        deleteNotificationConfigResponse = response
                        completed = true
                    }
                    override fun onFailure(e: Exception) {
                        completed = true
                        throw e
                    }
                }
            )
            while(!completed) {
                Thread.sleep(100)
            }
            return deleteNotificationConfigResponse!!
        }

        fun sendNotification(client: NodeClient, sendNotificationRequest: SendNotificationRequest): SendNotificationResponse {
            var sendNotificationResponse: SendNotificationResponse? = null
            var completed = false
            NotificationsPluginInterface.sendNotification(client, sendNotificationRequest.eventSource, sendNotificationRequest.channelMessage, sendNotificationRequest.channelIds,
                object : ActionListener<SendNotificationResponse> {
                    override fun onResponse(response: SendNotificationResponse) {
                        sendNotificationResponse = response
                        completed = true
                    }
                    override fun onFailure(e: Exception) {
                        completed = true
                        throw e
                    }
                }
            )
            while(!completed) {
                Thread.sleep(100)
            }
            return sendNotificationResponse!!
        }
    }
}
