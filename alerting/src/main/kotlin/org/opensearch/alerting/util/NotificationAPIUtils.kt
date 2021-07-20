/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.alerting.elasticapi.retry
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.client.node.NodeClient
import org.opensearch.common.unit.TimeValue
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

//TODO consider adding retry policies
class NotificationAPIUtils {

    companion object {

        private val logger = LogManager.getLogger(NotificationAPIUtils::class)

        private val retryPolicy =
            BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(50), 3)

        fun getNotificationConfig(client: NodeClient, getNotificationConfigRequest: GetNotificationConfigRequest): GetNotificationConfigResponse? {
            var getNotificationConfigResponse: GetNotificationConfigResponse? = null
            var exception: Exception? = null
            var completed = false
            retryPolicy.retry {
                NotificationsPluginInterface.getNotificationConfig(client, getNotificationConfigRequest,
                    object : ActionListener<GetNotificationConfigResponse> {
                        override fun onResponse(response: GetNotificationConfigResponse) {
                            getNotificationConfigResponse = response
                            logger.debug("Retrieved notification(s) successfully: $response")
                            completed = true
                        }

                        override fun onFailure(e: Exception) {
                            logger.error("Failed to retrieve Notification due to: ${e.message}", e)
                            completed = true
                            exception = e
                        }
                    }
                )
                if (exception != null) throw exception as Exception
            }
            while(!completed) {
                Thread.sleep(100)
            }
            if (exception != null) throw exception as Exception
            return getNotificationConfigResponse
        }

        fun createNotificationConfig(client: NodeClient, createNotificationConfigRequest: CreateNotificationConfigRequest): CreateNotificationConfigResponse {
            var createNotificationConfigResponse: CreateNotificationConfigResponse? = null
            var exception: Exception? = null
            var completed = false
            retryPolicy.retry {
                NotificationsPluginInterface.createNotificationConfig(client, createNotificationConfigRequest,
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
                if (exception != null) throw exception as Exception
            }
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
            var exception: Exception? = null
            retryPolicy.retry {
                NotificationsPluginInterface.updateNotificationConfig(client, updateNotificationConfigRequest,
                    object : ActionListener<UpdateNotificationConfigResponse> {
                        override fun onResponse(response: UpdateNotificationConfigResponse) {
                            updateNotificationConfigResponse = response
                            logger.debug("Updated notification successfully: $response")
                            completed = true
                        }

                        override fun onFailure(e: Exception) {
                            logger.error("Failed to update Notification due to: ${e.message}", e)
                            completed = true
                            exception = e
                        }
                    }
                )
                if (exception != null) throw exception as Exception
            }
            while(!completed) {
                Thread.sleep(100)
            }
            if (exception != null) throw exception as Exception
            return updateNotificationConfigResponse!!
        }

        fun deleteNotificationConfig(client: NodeClient, deleteNotificationConfigRequest: DeleteNotificationConfigRequest): DeleteNotificationConfigResponse {
            var deleteNotificationConfigResponse: DeleteNotificationConfigResponse? = null
            var completed = false
            var exception: Exception? = null
            retryPolicy.retry {
                NotificationsPluginInterface.deleteNotificationConfig(client, deleteNotificationConfigRequest,
                    object : ActionListener<DeleteNotificationConfigResponse> {
                        override fun onResponse(response: DeleteNotificationConfigResponse) {
                            deleteNotificationConfigResponse = response
                            logger.debug("Deleted notification successfully: $response")
                            completed = true
                        }

                        override fun onFailure(e: Exception) {
                            logger.error("Failed to delete Notification due to: ${e.message}", e)
                            completed = true
                            exception = e
                        }
                    }
                )
                if (exception != null) throw exception as Exception
            }
            while(!completed) {
                Thread.sleep(100)
            }
            if (exception != null) throw exception as Exception
            return deleteNotificationConfigResponse!!
        }

        fun sendNotification(client: NodeClient, sendNotificationRequest: SendNotificationRequest): SendNotificationResponse {
            var sendNotificationResponse: SendNotificationResponse? = null
            var completed = false
            var exception: Exception? = null
            var count = 0
            retryPolicy.retry {
                NotificationsPluginInterface.sendNotification(client, sendNotificationRequest.eventSource, sendNotificationRequest.channelMessage, sendNotificationRequest.channelIds,
                    object : ActionListener<SendNotificationResponse> {
                        override fun onResponse(response: SendNotificationResponse) {
                            sendNotificationResponse = response
                            logger.info("Sent notification successfully: $response")
                            completed = true
                        }

                        override fun onFailure(e: Exception) {
                            logger.error("Failed to send Notification due to: ${e.message}", e)
                            completed = true
                            exception = e
                        }
                    }
                )
                count += 1
                if (exception != null) {
                    logger.error("1 - Failed to send Notification due to:", exception)
                    throw exception as Exception
                }
            }
            logger.info("Send try is: $count")
            while(!completed) {
                Thread.sleep(100)
            }
            if (exception != null) {
                logger.error("2 - Failed to send Notification due to:", exception)
                throw exception as Exception
            }
            return sendNotificationResponse!!
        }
    }
}
