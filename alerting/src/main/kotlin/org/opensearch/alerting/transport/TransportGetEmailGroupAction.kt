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

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.GetEmailGroupAction
import org.opensearch.alerting.action.GetEmailGroupRequest
import org.opensearch.alerting.action.GetEmailGroupResponse
import org.opensearch.alerting.actionconverter.EmailGroupActionsConverter
import org.opensearch.alerting.actionconverter.EmailGroupActionsConverter.Companion.convertGetEmailGroupRequestToGetNotificationConfigRequest
import org.opensearch.alerting.actionconverter.EmailGroupActionsConverter.Companion.convertGetNotificationConfigResponseToGetEmailGroupResponse
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.DestinationType
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.commons.notifications.NotificationsPluginInterface
import org.opensearch.commons.notifications.action.GetNotificationConfigResponse
import org.opensearch.commons.notifications.action.NotificationsActions
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportGetEmailGroupAction::class.java)

class TransportGetEmailGroupAction @Inject constructor(
    transportService: TransportService,
    val client: NodeClient,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetEmailGroupRequest, GetEmailGroupResponse>(
    GetEmailGroupAction.NAME, transportService, actionFilters, ::GetEmailGroupRequest
) {

    @Volatile private var allowList = ALLOW_LIST.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) { allowList = it }
    }

    override fun doExecute(
        task: Task,
        getEmailGroupRequest: GetEmailGroupRequest,
        actionListener: ActionListener<GetEmailGroupResponse>
    ) {

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

        NotificationsPluginInterface.getNotificationConfig(client, convertGetEmailGroupRequestToGetNotificationConfigRequest(getEmailGroupRequest),
            object : ActionListener<GetNotificationConfigResponse> {
                override fun onResponse(response: GetNotificationConfigResponse) {
                    val getEmailGroupResponse = convertGetNotificationConfigResponseToGetEmailGroupResponse(response)
                    actionListener.onResponse(getEmailGroupResponse)
                }
                override fun onFailure(e: Exception) {
                    actionListener.onFailure(AlertingException.wrap(e))
                }
            }
        )


//        try {
//            val response = client.suspendUntil {
//                client.execute(NotificationsActions.GET_NOTIFICATION_CONFIG_ACTION_TYPE, GetEmailGroupConverter.convertGetAlertRequestToNotificationRequest(getEmailGroupRequest), it)
//            }
//            return GetEmailGroupConverter.convertGetNotificationResponseToAlertResponse(response)
//        } catch (e: Exception) {
//
//        }
//
//        val getRequest = GetRequest(SCHEDULED_JOBS_INDEX, getEmailGroupRequest.emailGroupID)
//            .version(getEmailGroupRequest.version)
//            .fetchSourceContext(getEmailGroupRequest.srcContext)
//        client.threadPool().threadContext.stashContext().use {
//            client.get(
//                getRequest,
//                object : ActionListener<GetResponse> {
//                    override fun onResponse(response: GetResponse) {
//                        if (!response.isExists) {
//                            actionListener.onFailure(
//                                AlertingException.wrap(
//                                    OpenSearchStatusException("Email Group not found.", RestStatus.NOT_FOUND)
//                                )
//                            )
//                            return
//                        }
//
//                        var emailGroup: EmailGroup? = null
//                        if (!response.isSourceEmpty) {
//                            XContentHelper.createParser(
//                                xContentRegistry, LoggingDeprecationHandler.INSTANCE,
//                                response.sourceAsBytesRef, XContentType.JSON
//                            ).use { xcp ->
//                                emailGroup = EmailGroup.parseWithType(xcp, response.id, response.version)
//                            }
//                        }
//
//                        actionListener.onResponse(
//                            GetEmailGroupResponse(
//                                response.id, response.version, response.seqNo, response.primaryTerm,
//                                RestStatus.OK, emailGroup
//                            )
//                        )
//                    }
//
//                    override fun onFailure(e: Exception) {
//                        actionListener.onFailure(e)
//                    }
//                }
//            )
//        }
    }
}
