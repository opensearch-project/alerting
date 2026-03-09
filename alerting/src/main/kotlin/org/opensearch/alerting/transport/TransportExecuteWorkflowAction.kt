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
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.AlertingV2Utils.validateMonitorV1
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.action.ExecuteWorkflowAction
import org.opensearch.alerting.action.ExecuteWorkflowRequest
import org.opensearch.alerting.action.ExecuteWorkflowResponse
import org.opensearch.alerting.util.use
import org.opensearch.common.inject.Inject
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.authuser.User
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.time.Instant

private val log = LogManager.getLogger(TransportExecuteWorkflowAction::class.java)

class TransportExecuteWorkflowAction @Inject constructor(
    private val transportService: TransportService,
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
                    val (periodStart, periodEnd) = if (execWorkflowRequest.requestStart != null) {
                        Pair(
                            Instant.ofEpochMilli(execWorkflowRequest.requestStart.millis),
                            Instant.ofEpochMilli(execWorkflowRequest.requestEnd.millis)
                        )
                    } else {
                        workflow.schedule.getPeriodEndingAt(Instant.ofEpochMilli(execWorkflowRequest.requestEnd.millis))
                    }
                    try {
                        log.info(
                            "Executing workflow from API - id: ${workflow.id}, periodStart: $periodStart, periodEnd: $periodEnd, " +
                                "dryrun: ${execWorkflowRequest.dryrun}"
                        )
                        val workflowRunResult =
                            MonitorRunnerService.runJob(
                                workflow,
                                periodStart,
                                periodEnd,
                                execWorkflowRequest.dryrun,
                                transportService = transportService
                            )
                        withContext(Dispatchers.IO, {
                            actionListener.onResponse(
                                ExecuteWorkflowResponse(
                                    workflowRunResult
                                )
                            )
                        })
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
                                    val scheduledJob = ScheduledJob.parse(xcp, response.id, response.version)
                                    validateMonitorV1(scheduledJob)?.let {
                                        actionListener.onFailure(AlertingException.wrap(it))
                                        return
                                    }
                                    val workflow = scheduledJob as Workflow
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
