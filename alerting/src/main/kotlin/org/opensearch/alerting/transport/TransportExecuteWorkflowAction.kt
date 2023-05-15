/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.action.ExecuteWorkflowAction
import org.opensearch.alerting.action.ExecuteWorkflowRequest
import org.opensearch.alerting.action.ExecuteWorkflowResponse
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.workflow.WorkflowRunnerService
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.authuser.User
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.time.Instant

private val log = LogManager.getLogger(TransportExecuteWorkflowAction::class.java)

class TransportExecuteWorkflowAction @Inject constructor(
    transportService: TransportService,
    private val client: Client,
    private val runner: MonitorRunnerService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<ExecuteWorkflowRequest, ExecuteWorkflowResponse>(
    ExecuteWorkflowAction.NAME, transportService, actionFilters, ::ExecuteWorkflowRequest
) {
    override fun doExecute(
        task: Task,
        execWorkflowRequest: ExecuteWorkflowRequest,
        actionListener: ActionListener<ExecuteWorkflowResponse>,
    ) {
        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
        log.debug("User and roles string from thread context: $userStr")
        val user: User? = User.parse(userStr)

        client.threadPool().threadContext.stashContext().use {
            val executeWorkflow = fun(workflow: Workflow) {
                runner.launch {
                    val (periodStart, periodEnd) =
                        workflow.schedule.getPeriodEndingAt(Instant.ofEpochMilli(execWorkflowRequest.requestEnd.millis))
                    try {
                        val workflowRunResult =
                            WorkflowRunnerService.runJob(workflow, periodStart, periodEnd, execWorkflowRequest.dryrun)
                        withContext(Dispatchers.IO) {
                            actionListener.onResponse(
                                ExecuteWorkflowResponse(
                                    workflowRunResult
                                )
                            )
                        }
                    } catch (e: Exception) {
                        log.error("Unexpected error running workflow", e)
                        withContext(Dispatchers.IO) {
                            actionListener.onFailure(AlertingException.wrap(e))
                        }
                    }
                }
            }

            if (execWorkflowRequest.workflowId != null) {
                val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX).id(execWorkflowRequest.workflowId)
                client.get(
                    getRequest,
                    object : ActionListener<GetResponse> {
                        override fun onResponse(response: GetResponse) {
                            if (!response.isExists) {
                                log.error("Can't find workflow with id: ${response.id}")
                                actionListener.onFailure(
                                    AlertingException.wrap(
                                        OpenSearchStatusException(
                                            "Can't find workflow with id: ${response.id}",
                                            RestStatus.NOT_FOUND
                                        )
                                    )
                                )
                                return
                            }
                            if (!response.isSourceEmpty) {
                                XContentHelper.createParser(
                                    xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                                    response.sourceAsBytesRef, XContentType.JSON
                                ).use { xcp ->
                                    val workflow = ScheduledJob.parse(xcp, response.id, response.version) as Workflow
                                    executeWorkflow(workflow)
                                }
                            }
                        }

                        override fun onFailure(t: Exception) {
                            log.error("Error getting workflow ${execWorkflowRequest.workflowId}", t)
                            actionListener.onFailure(AlertingException.wrap(t))
                        }
                    }
                )
            } else {
                val workflow = when (user?.name.isNullOrEmpty()) {
                    true -> execWorkflowRequest.workflow as Workflow
                    false -> (execWorkflowRequest.workflow as Workflow).copy(user = user)
                }

                executeWorkflow(workflow)
            }
        }
    }
}
