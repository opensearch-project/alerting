/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.use
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.GetWorkflowRequest
import org.opensearch.commons.alerting.action.GetWorkflowResponse
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.IndexNotFoundException
import org.opensearch.remote.metadata.client.GetDataObjectRequest
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.remote.metadata.common.SdkClientUtils
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

class TransportGetWorkflowAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    val sdkClient: SdkClient,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    val clusterService: ClusterService,
    settings: Settings
) : HandledTransportAction<GetWorkflowRequest, GetWorkflowResponse>(
    AlertingActions.GET_WORKFLOW_ACTION_NAME, transportService, actionFilters, ::GetWorkflowRequest
),
    SecureTransportAction {

    private val log = LogManager.getLogger(javaClass)

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, getWorkflowRequest: GetWorkflowRequest, actionListener: ActionListener<GetWorkflowResponse>) {
        val user = readUserFromThreadContext(client)

        val getRequest = GetDataObjectRequest.builder()
            .index(ScheduledJob.SCHEDULED_JOBS_INDEX)
            .id(getWorkflowRequest.workflowId)
            .build()

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }

        client.threadPool().threadContext.stashContext().use {
            sdkClient.getDataObjectAsync(getRequest)
                .whenComplete(
                    SdkClientUtils.wrapGetCompletion(object : ActionListener<GetResponse> {
                        override fun onResponse(response: GetResponse) {
                            if (!response.isExists) {
                                log.error("Workflow with ${getWorkflowRequest.workflowId} not found")
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
                                    val compositeMonitor = ScheduledJob.parse(xcp, response.id, response.version)
                                    if (compositeMonitor is Workflow) {
                                        workflow = compositeMonitor
                                    } else {
                                        log.error("Wrong monitor type returned")
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
                            log.error("Getting the workflow failed", t)

                            if (t is IndexNotFoundException || t is OpenSearchException && t.cause is IndexNotFoundException) {
                                actionListener.onFailure(
                                    OpenSearchStatusException(
                                        "Workflow not found",
                                        RestStatus.NOT_FOUND
                                    )
                                )
                            } else {
                                actionListener.onFailure(AlertingException.wrap(t))
                            }
                        }
                    })
                )
        }
    }
}
