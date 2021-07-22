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
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.DeleteDestinationAction
import org.opensearch.alerting.action.DeleteDestinationRequest
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertDeleteDestinationRequestToDeleteNotificationConfigRequest
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertDeleteNotificationConfigResponseToDeleteResponse
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.NotificationAPIUtils
import org.opensearch.alerting.util.checkFilterByUserBackendRoles
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportDeleteDestinationAction::class.java)

class TransportDeleteDestinationAction @Inject constructor(
    transportService: TransportService,
    val client: NodeClient,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<DeleteDestinationRequest, DeleteResponse>(
    DeleteDestinationAction.NAME, transportService, actionFilters, ::DeleteDestinationRequest
) {

    @Volatile private var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.FILTER_BY_BACKEND_ROLES) { filterByEnabled = it }
    }

    override fun doExecute(task: Task, request: DeleteDestinationRequest, actionListener: ActionListener<DeleteResponse>) {
        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
        log.debug("User and roles string from thread context: $userStr")
        val user: User? = User.parse(userStr)

        if (!checkFilterByUserBackendRoles(filterByEnabled, user, actionListener)) {
            return
        }

        try {
            val deleteNotificationConfigResponse = NotificationAPIUtils.deleteNotificationConfig(
                client,
                convertDeleteDestinationRequestToDeleteNotificationConfigRequest(request)
            )
            actionListener.onResponse(convertDeleteNotificationConfigResponseToDeleteResponse(deleteNotificationConfigResponse))
        } catch (e: Exception) {
            log.error("Cannot delete destination due to: ${e.message}", e)
            actionListener.onFailure(AlertingException.wrap(e))
        }
    }
}
