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
import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.IndexEmailGroupAction
import org.opensearch.alerting.action.IndexEmailGroupRequest
import org.opensearch.alerting.action.IndexEmailGroupResponse
import org.opensearch.alerting.actionconverter.EmailGroupActionsConverter
import org.opensearch.alerting.actionconverter.EmailGroupActionsConverter.Companion.convertIndexEmailGroupRequestToCreateNotificationConfigRequest
import org.opensearch.alerting.actionconverter.EmailGroupActionsConverter.Companion.convertIndexEmailGroupRequestToUpdateNotificationConfigRequest
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.settings.AlertingSettings.Companion.INDEX_TIMEOUT
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.NotificationAPIUtils
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.commons.notifications.action.BaseResponse
import org.opensearch.commons.notifications.action.GetNotificationConfigRequest
import org.opensearch.rest.RestRequest
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportIndexEmailGroupAction::class.java)

class TransportIndexEmailGroupAction @Inject constructor(
    transportService: TransportService,
    val client: NodeClient,
    actionFilters: ActionFilters,
    val scheduledJobIndices: ScheduledJobIndices,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<IndexEmailGroupRequest, IndexEmailGroupResponse>(
    IndexEmailGroupAction.NAME, transportService, actionFilters, ::IndexEmailGroupRequest
) {

    @Volatile private var indexTimeout = INDEX_TIMEOUT.get(settings)
    @Volatile private var allowList = ALLOW_LIST.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(INDEX_TIMEOUT) { indexTimeout = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) { allowList = it }
    }

    override fun doExecute(task: Task, request: IndexEmailGroupRequest, actionListener: ActionListener<IndexEmailGroupResponse>) {
        try {
            val notificationResponse: BaseResponse
            val configId: String
            if (request.method == RestRequest.Method.PUT) {
                notificationResponse = NotificationAPIUtils.updateNotificationConfig(
                    client,
                    convertIndexEmailGroupRequestToUpdateNotificationConfigRequest(request)
                )
                configId = notificationResponse.configId
            } else {
                notificationResponse = NotificationAPIUtils.createNotificationConfig(
                    client,
                    convertIndexEmailGroupRequestToCreateNotificationConfigRequest(request)
                )
                configId = notificationResponse.configId
            }
            val getNotificationConfigRequest = GetNotificationConfigRequest(setOf(configId!!), 0, 1, null, null, emptyMap())
            val getNotificationConfigResponse = NotificationAPIUtils.getNotificationConfig(client, getNotificationConfigRequest)
            actionListener.onResponse(
                EmailGroupActionsConverter.convertToIndexEmailGroupResponse(
                    configId,
                    getNotificationConfigResponse
                )
            )
//            if (request.method == RestRequest.Method.PUT) {
//                actionListener.onResponse(
//                    convertUpdateNotificationConfigResponseToIndexEmailGroupResponse(
//                        notificationResponse as UpdateNotificationConfigResponse,
//                        getNotificationConfigResponse
//                    )
//                )
//            } else {
//                actionListener.onResponse(
//                    convertCreateNotificationConfigResponseToIndexEmailGroupResponse(
//                        notificationResponse as CreateNotificationConfigResponse,
//                        getNotificationConfigResponse
//                    )
//                )
//            }
        } catch (e: Exception) {
            actionListener.onFailure(AlertingException.wrap(e))
        }
    }
}
