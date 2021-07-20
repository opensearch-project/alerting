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
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.alerting.transport

import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.DeleteEmailAccountAction
import org.opensearch.alerting.action.DeleteEmailAccountRequest
import org.opensearch.alerting.actionconverter.DestinationActionsConverter
import org.opensearch.alerting.actionconverter.EmailAccountActionsConverter.Companion.convertDeleteEmailAccountRequestToDeleteNotificationConfigRequest
import org.opensearch.alerting.actionconverter.EmailAccountActionsConverter.Companion.convertDeleteNotificationConfigResponseToDeleteResponse
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.DestinationType
import org.opensearch.alerting.util.NotificationAPIUtils
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.commons.notifications.NotificationsPluginInterface
import org.opensearch.commons.notifications.action.DeleteNotificationConfigResponse
import org.opensearch.commons.notifications.action.NotificationsActions
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportDeleteEmailAccountAction @Inject constructor(
    transportService: TransportService,
    val client: NodeClient,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings
) : HandledTransportAction<DeleteEmailAccountRequest, DeleteResponse>(
    DeleteEmailAccountAction.NAME, transportService, actionFilters, ::DeleteEmailAccountRequest
) {

    @Volatile private var allowList = ALLOW_LIST.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) { allowList = it }
    }

    override fun doExecute(task: Task, request: DeleteEmailAccountRequest, actionListener: ActionListener<DeleteResponse>) {

        if (!allowList.contains(DestinationType.EMAIL.value)) {
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException(
                        "This API is blocked since Destination type [${DestinationType.EMAIL}] is not allowed",
                        RestStatus.FORBIDDEN
                    )
                )
            )
            return
        }

        try {
            val deleteNotificationConfigResponse = NotificationAPIUtils.deleteNotificationConfig(client, convertDeleteEmailAccountRequestToDeleteNotificationConfigRequest(request))
            actionListener.onResponse(convertDeleteNotificationConfigResponseToDeleteResponse(deleteNotificationConfigResponse))
        } catch (e: Exception) {
            actionListener.onFailure(AlertingException.wrap(e))
        }
//
//
//        NotificationsPluginInterface.deleteNotificationConfig(client, convertDeleteEmailAccountRequestToDeleteNotificationConfigRequest(request),
//            object : ActionListener<DeleteNotificationConfigResponse> {
//                override fun onResponse(response: DeleteNotificationConfigResponse) {
//                    val deleteResponse = convertDeleteNotificationConfigResponseToDeleteResponse(response)
//                    actionListener.onResponse(deleteResponse)
//                }
//                override fun onFailure(e: Exception) {
//                    actionListener.onFailure(AlertingException.wrap(e))
//                }
//            }
//        )


//        val deleteRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, request.emailAccountID)
//            .setRefreshPolicy(request.refreshPolicy)
//        client.threadPool().threadContext.stashContext().use {
//            client.delete(
//                deleteRequest,
//                object : ActionListener<DeleteResponse> {
//                    override fun onResponse(response: DeleteResponse) {
//                        actionListener.onResponse(response)
//                    }
//
//                    override fun onFailure(t: Exception) {
//                        actionListener.onFailure(t)
//                    }
//                }
//            )
//        }
    }
}
