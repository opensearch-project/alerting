/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
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
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.action.ExecuteMonitorAction
import org.opensearch.alerting.action.ExecuteMonitorRequest
import org.opensearch.alerting.action.ExecuteMonitorResponse
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.time.Instant

private val log = LogManager.getLogger(TransportExecuteMonitorAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportExecuteMonitorAction @Inject constructor(
    transportService: TransportService,
    private val client: Client,
    private val clusterService: ClusterService,
    private val runner: MonitorRunnerService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    private val docLevelMonitorQueries: DocLevelMonitorQueries,
    private val settings: Settings
) : HandledTransportAction<ExecuteMonitorRequest, ExecuteMonitorResponse> (
    ExecuteMonitorAction.NAME, transportService, actionFilters, ::ExecuteMonitorRequest
) {
    @Volatile private var indexTimeout = AlertingSettings.INDEX_TIMEOUT.get(settings)

    override fun doExecute(task: Task, execMonitorRequest: ExecuteMonitorRequest, actionListener: ActionListener<ExecuteMonitorResponse>) {

        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
        log.debug("User and roles string from thread context: $userStr")
        val user: User? = User.parse(userStr)

        client.threadPool().threadContext.stashContext().use {
            val executeMonitor = fun(monitor: Monitor) {
                // Launch the coroutine with the clients threadContext. This is needed to preserve authentication information
                // stored on the threadContext set by the security plugin when using the Alerting plugin with the Security plugin.
                // runner.launch(ElasticThreadContextElement(client.threadPool().threadContext)) {
                runner.launch {
                    val (periodStart, periodEnd) =
                        monitor.schedule.getPeriodEndingAt(Instant.ofEpochMilli(execMonitorRequest.requestEnd.millis))
                    try {
                        val monitorRunResult = runner.runJob(monitor, periodStart, periodEnd, execMonitorRequest.dryrun)
                        withContext(Dispatchers.IO) {
                            actionListener.onResponse(ExecuteMonitorResponse(monitorRunResult))
                        }
                    } catch (e: Exception) {
                        log.error("Unexpected error running monitor", e)
                        withContext(Dispatchers.IO) {
                            actionListener.onFailure(AlertingException.wrap(e))
                        }
                    }
                }
            }

            if (execMonitorRequest.monitorId != null) {
                val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX).id(execMonitorRequest.monitorId)
                client.get(
                    getRequest,
                    object : ActionListener<GetResponse> {
                        override fun onResponse(response: GetResponse) {
                            if (!response.isExists) {
                                actionListener.onFailure(
                                    AlertingException.wrap(
                                        OpenSearchStatusException("Can't find monitor with id: ${response.id}", RestStatus.NOT_FOUND)
                                    )
                                )
                                return
                            }
                            if (!response.isSourceEmpty) {
                                XContentHelper.createParser(
                                    xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                                    response.sourceAsBytesRef, XContentType.JSON
                                ).use { xcp ->
                                    val monitor = ScheduledJob.parse(xcp, response.id, response.version) as Monitor
                                    executeMonitor(monitor)
                                }
                            }
                        }

                        override fun onFailure(t: Exception) {
                            actionListener.onFailure(AlertingException.wrap(t))
                        }
                    }
                )
            } else {
                val monitor = when (user?.name.isNullOrEmpty()) {
                    true -> execMonitorRequest.monitor as Monitor
                    false -> (execMonitorRequest.monitor as Monitor).copy(user = user)
                }

                if (monitor.monitorType == Monitor.MonitorType.DOC_LEVEL_MONITOR) {
                    try {
                        scope.launch {
                            if (!docLevelMonitorQueries.docLevelQueryIndexExists()) {
                                docLevelMonitorQueries.initDocLevelQueryIndex(monitor.dataSources)
                                log.info("Central Percolation index ${ScheduledJob.DOC_LEVEL_QUERIES_INDEX} created")
                            }
                            docLevelMonitorQueries.indexDocLevelQueries(
                                monitor,
                                monitor.id,
                                WriteRequest.RefreshPolicy.IMMEDIATE,
                                indexTimeout
                            )
                            log.info("Queries inserted into Percolate index ${ScheduledJob.DOC_LEVEL_QUERIES_INDEX}")
                            executeMonitor(monitor)
                        }
                    } catch (t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                } else {
                    executeMonitor(monitor)
                }
            }
        }
    }
}
