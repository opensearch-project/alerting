/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.opensearchapi.suspendUntil
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
import org.opensearch.commons.alerting.action.DeleteWorkflowRequest
import org.opensearch.commons.alerting.action.DeleteWorkflowResponse
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.recreateObject
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportDeleteWorkflowAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<ActionRequest, DeleteWorkflowResponse>(
    AlertingActions.DELETE_WORKFLOW_ACTION_NAME, transportService, actionFilters, ::DeleteWorkflowRequest
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: ActionRequest, actionListener: ActionListener<DeleteWorkflowResponse>) {
        val transformedRequest = request as? DeleteWorkflowRequest
            ?: recreateObject(request) { DeleteWorkflowRequest(it) }

        val user = readUserFromThreadContext(client)
        val deleteRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, transformedRequest.workflowId)
            .setRefreshPolicy(transformedRequest.refreshPolicy)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }

        GlobalScope.launch(Dispatchers.IO + CoroutineName("DeleteWorkflowAction")) {
            DeleteWorkflowHandler(client, actionListener, deleteRequest, user, transformedRequest.workflowId).resolveUserAndStart()
        }
    }

    inner class DeleteWorkflowHandler(
        private val client: Client,
        private val actionListener: ActionListener<DeleteWorkflowResponse>,
        private val deleteRequest: DeleteRequest,
        private val user: User?,
        private val workflowId: String
    ) {
        suspend fun resolveUserAndStart() {
            try {
                val workflow = getWorkflow()

                val canDelete = user == null ||
                    !doFilterForUser(user) ||
                    checkUserPermissionsWithResource(
                        user,
                        workflow.user,
                        actionListener,
                        "workflow",
                        workflowId
                    )

                if (canDelete) {
                    val deleteResponse = deleteWorkflow(workflow)
                    // TODO - uncomment once the workflow metadata is added
                    // deleteMetadata(workflow)
                    actionListener.onResponse(DeleteWorkflowResponse(deleteResponse.id, deleteResponse.version))
                } else {
                    actionListener.onFailure(
                        AlertingException(
                            "Not allowed to delete this workflow!",
                            RestStatus.FORBIDDEN,
                            IllegalStateException()
                        )
                    )
                }
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun getWorkflow(): Workflow {
            val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, workflowId)

            val getResponse: GetResponse = client.suspendUntil { get(getRequest, it) }
            if (getResponse.isExists == false) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException("Workflow with $workflowId is not found", RestStatus.NOT_FOUND)
                    )
                )
            }
            val xcp = XContentHelper.createParser(
                xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                getResponse.sourceAsBytesRef, XContentType.JSON
            )
            return ScheduledJob.parse(xcp, getResponse.id, getResponse.version) as Workflow
        }

        private suspend fun deleteWorkflow(workflow: Workflow): DeleteResponse {
            return client.suspendUntil { delete(deleteRequest, it) }
        }

        private suspend fun deleteMetadata(workflow: Workflow) {
            val deleteRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, "${workflow.id}-metadata")
            val deleteResponse: DeleteResponse = client.suspendUntil { delete(deleteRequest, it) }
        }
    }
}
