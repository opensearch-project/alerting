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

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.IndexDestinationAction
import org.opensearch.alerting.action.IndexDestinationRequest
import org.opensearch.alerting.action.IndexDestinationResponse
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertIndexDestinationRequestToCreateNotificationConfigRequest
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertIndexDestinationRequestToUpdateNotificationConfigRequest
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertToIndexDestinationResponse
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.DestinationSettings
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
import org.opensearch.commons.notifications.action.BaseResponse
import org.opensearch.commons.notifications.action.GetNotificationConfigRequest
import org.opensearch.rest.RestRequest
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportIndexDestinationAction::class.java)

class TransportIndexDestinationAction @Inject constructor(
    transportService: TransportService,
    val client: NodeClient,
    actionFilters: ActionFilters,
    val scheduledJobIndices: ScheduledJobIndices,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<IndexDestinationRequest, IndexDestinationResponse>(
    IndexDestinationAction.NAME, transportService, actionFilters, ::IndexDestinationRequest
) {

    @Volatile private var indexTimeout = AlertingSettings.INDEX_TIMEOUT.get(settings)
    @Volatile private var allowList = DestinationSettings.ALLOW_LIST.get(settings)
    @Volatile private var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.INDEX_TIMEOUT) { indexTimeout = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(DestinationSettings.ALLOW_LIST) { allowList = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.FILTER_BY_BACKEND_ROLES) { filterByEnabled = it }
    }

    override fun doExecute(task: Task, request: IndexDestinationRequest, actionListener: ActionListener<IndexDestinationResponse>) {
        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
        log.debug("User and roles string from thread context: $userStr")
        val user: User? = User.parse(userStr)

        if (!checkFilterByUserBackendRoles(filterByEnabled, user, actionListener)) {
            return
        }

        try {
            val notificationResponse: BaseResponse
            val configId: String
            if (request.method == RestRequest.Method.PUT) {

                //client.threadPool().threadContext.stashContext().use
                notificationResponse = NotificationAPIUtils.updateNotificationConfig(
                    client,
                    convertIndexDestinationRequestToUpdateNotificationConfigRequest(request)
                )
                configId = notificationResponse.configId
            } else {
                val createRequest = convertIndexDestinationRequestToCreateNotificationConfigRequest(request)
                notificationResponse = NotificationAPIUtils.createNotificationConfig(client, createRequest)
                configId = notificationResponse.configId
            }

            val getNotificationConfigRequest = GetNotificationConfigRequest(setOf(configId), 0, 1, null, null, emptyMap())
            val getNotificationConfigResponse = NotificationAPIUtils.getNotificationConfig(client, getNotificationConfigRequest)
            actionListener.onResponse(
                convertToIndexDestinationResponse(configId, getNotificationConfigResponse)
            )

//            if (request.method == RestRequest.Method.PUT) {
//                actionListener.onResponse(
//                    convertToIndexDestinationResponse(
//                        (notificationResponse.configId as UpdateNotificationConfigResponse).configId,
//                        getNotificationConfigResponse
//                    )
//                )
//            } else {
//                actionListener.onResponse(
//                    convertToIndexDestinationResponse(
//                        (notificationResponse as CreateNotificationConfigResponse).configId,
//                        getNotificationConfigResponse
//                    )
//                )
//            }
        } catch (e: Exception) {
            log.error("Failed to index destination due to", e)
            actionListener.onFailure(AlertingException.wrap(e))
        }
    }
}
