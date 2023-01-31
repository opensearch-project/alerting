/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.GetWorkflowRequest
import org.opensearch.commons.alerting.action.GetWorkflowResponse
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportGetWorkflowAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    val clusterService: ClusterService,
    settings: Settings
) : HandledTransportAction<GetWorkflowRequest, GetWorkflowResponse>(
    AlertingActions.GET_WORKFLOW_ACTION_NAME, transportService, actionFilters, ::GetWorkflowRequest
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, getWorkflowRequest: GetWorkflowRequest, actionListener: ActionListener<GetWorkflowResponse>) {
        val user = readUserFromThreadContext(client)

        val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, getWorkflowRequest.workflowId)
            .version(getWorkflowRequest.version)
            .fetchSourceContext(getWorkflowRequest.srcContext)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }

        /*
         * Remove security context before you call elasticsearch api's. By this time, permissions required
         * to call this api are validated.
         * Once system-indices [https://github.com/opendistro-for-elasticsearch/security/issues/666] is done, we
         * might further improve this logic. Also change try to kotlin-use for auto-closable.
         */
        client.threadPool().threadContext.stashContext().use {
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            actionListener.onFailure(
                                AlertingException.wrap(
                                    OpenSearchStatusException(
                                        "Workflow not found.",
                                        RestStatus.NOT_FOUND
                                    )
                                )
                            )
                            return
                        }

                        var workflow: Workflow? = null
                        if (!response.isSourceEmpty) {
                            XContentHelper.createParser(
                                xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                                response.sourceAsBytesRef, XContentType.JSON
                            ).use { xcp ->
                                workflow = ScheduledJob.parse(xcp, response.id, response.version) as Workflow

                                // security is enabled and filterby is enabled
                                if (!checkUserPermissionsWithResource(
                                        user,
                                        workflow?.user,
                                        actionListener,
                                        "workflow",
                                        getWorkflowRequest.workflowId
                                    )
                                ) {
                                    return
                                }
                            }
                        }

                        actionListener.onResponse(
                            GetWorkflowResponse(
                                response.id,
                                response.version,
                                response.seqNo,
                                response.primaryTerm,
                                RestStatus.OK,
                                workflow
                            )
                        )
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        }
    }
}