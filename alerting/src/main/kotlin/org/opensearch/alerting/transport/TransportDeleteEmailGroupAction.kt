/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.DeleteEmailGroupAction
import org.opensearch.alerting.action.DeleteEmailGroupRequest
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.DestinationType
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportDeleteEmailGroupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings
) : HandledTransportAction<DeleteEmailGroupRequest, DeleteResponse>(
    DeleteEmailGroupAction.NAME, transportService, actionFilters, ::DeleteEmailGroupRequest
) {

    @Volatile private var allowList = ALLOW_LIST.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) { allowList = it }
    }

    override fun doExecute(task: Task, request: DeleteEmailGroupRequest, actionListener: ActionListener<DeleteResponse>) {

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

        val deleteRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, request.emailGroupID)
            .setRefreshPolicy(request.refreshPolicy)
        client.threadPool().threadContext.stashContext().use {
            client.delete(
                deleteRequest,
                object : ActionListener<DeleteResponse> {
                    override fun onResponse(response: DeleteResponse) {
                        actionListener.onResponse(response)
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(t)
                    }
                }
            )
        }
    }
}
