/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.ActionRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.workflow.CompositeWorkflowRunner
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.ExecuteStreamingWorkflowRequest
import org.opensearch.commons.alerting.action.ExecuteStreamingWorkflowResponse
import org.opensearch.commons.alerting.action.GetWorkflowRequest
import org.opensearch.commons.alerting.model.StreamingIndex
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.rest.RestRequest
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportExecuteStreamingWorkflowAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportExecuteStreamingWorkflowAction @Inject constructor(
    transportService: TransportService,
    private val client: Client,
    val settings: Settings,
    val clusterService: ClusterService,
    actionFilters: ActionFilters,
    val transportGetWorkflowAction: TransportGetWorkflowAction
) : HandledTransportAction<ActionRequest, ExecuteStreamingWorkflowResponse>(
    AlertingActions.EXECUTE_STREAMING_WORKFLOW_ACTION_NAME, transportService, actionFilters, ::ExecuteStreamingWorkflowRequest
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(
        task: Task,
        request: ActionRequest,
        actionListener: ActionListener<ExecuteStreamingWorkflowResponse>,
    ) {
        if (!validateUser()) {
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException(
                        "User is not authorized to to perform this action. Contact administrator",
                        RestStatus.FORBIDDEN
                    )
                )
            )
            return
        }

        val executeStreamingWorkflowRequest = recreateRequestIfNeeded(request)

        client.threadPool().threadContext.stashContext().use {
            scope.launch {
                val workflow: Workflow? = getWorkflow(executeStreamingWorkflowRequest.workflowId, actionListener)

                if (workflow != null) {
                    runStreamingWorkflow(workflow, executeStreamingWorkflowRequest.indices, actionListener)
                } else {
                    actionListener.onFailure(
                        AlertingException.wrap(
                            ResourceNotFoundException(
                                "Workflow with id ${executeStreamingWorkflowRequest.workflowId} not found",
                                RestStatus.NOT_FOUND
                            )
                        )
                    )
                }
            }
        }
    }

    private fun validateUser(): Boolean {
        val user: User? = readUserFromThreadContext(client)

        // If security is enabled, only allow the admin user to call this API
        return user == null || isAdmin(user)
    }

    private fun recreateRequestIfNeeded(request: ActionRequest): ExecuteStreamingWorkflowRequest {
        return request as? ExecuteStreamingWorkflowRequest
            ?: recreateObject(request) { ExecuteStreamingWorkflowRequest(it) }
    }

    private suspend fun getWorkflow(workflowId: String, actionListener: ActionListener<ExecuteStreamingWorkflowResponse>): Workflow? {
        try {
            val getWorkflowResponse = transportGetWorkflowAction.client.suspendUntil {
                val getWorkflowRequest = GetWorkflowRequest(workflowId, RestRequest.Method.GET)
                execute(AlertingActions.GET_WORKFLOW_ACTION_TYPE, getWorkflowRequest, it)
            }

            return getWorkflowResponse.workflow!!
        } catch (e: Exception) {
            actionListener.onFailure(AlertingException.wrap(e))
        }

        return null
    }

    private suspend fun runStreamingWorkflow(
        workflow: Workflow,
        streamingIndices: List<StreamingIndex>,
        actionListener: ActionListener<ExecuteStreamingWorkflowResponse>
    ) {
        try {
            val workflowRunResult = CompositeWorkflowRunner.runStreamingWorkflow(
                workflow,
                MonitorRunnerService.monitorCtx,
                false,
                streamingIndices
            )

            if (workflowRunResult.error != null) {
                actionListener.onFailure(workflowRunResult.error)
            } else {
                actionListener.onResponse(ExecuteStreamingWorkflowResponse(RestStatus.OK))
            }
        } catch (e: Exception) {
            actionListener.onFailure(AlertingException.wrap(e))
        }
    }
}
