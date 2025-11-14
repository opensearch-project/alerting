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
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.AlertingV2Utils.validateMonitorV1
import org.opensearch.alerting.MonitorMetadataService
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.action.ExecuteMonitorAction
import org.opensearch.alerting.action.ExecuteMonitorRequest
import org.opensearch.alerting.action.ExecuteMonitorResponse
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.alerting.util.use
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.alerting.util.isMonitorOfStandardType
import org.opensearch.commons.authuser.User
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.time.Instant
import java.util.Locale

private val log = LogManager.getLogger(TransportExecuteMonitorAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportExecuteMonitorAction @Inject constructor(
    private val transportService: TransportService,
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
                    val (periodStart, periodEnd) = if (execMonitorRequest.requestStart != null) {
                        Pair(
                            Instant.ofEpochMilli(execMonitorRequest.requestStart.millis),
                            Instant.ofEpochMilli(execMonitorRequest.requestEnd.millis)
                        )
                    } else {
                        monitor.schedule.getPeriodEndingAt(Instant.ofEpochMilli(execMonitorRequest.requestEnd.millis))
                    }
                    try {
                        log.info(
                            "Executing monitor from API - id: ${monitor.id}, type: ${monitor.monitorType}, " +
                                "periodStart: $periodStart, periodEnd: $periodEnd, dryrun: ${execMonitorRequest.dryrun}"
                        )
                        val monitorRunResult = runner.runJob(monitor, periodStart, periodEnd, execMonitorRequest.dryrun, transportService)
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
                                    val scheduledJob = ScheduledJob.parse(xcp, response.id, response.version)
                                    validateMonitorV1(scheduledJob)?.let {
                                        actionListener.onFailure(AlertingException.wrap(it))
                                        return
                                    }
                                    val monitor = scheduledJob as Monitor
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

                if (
                    monitor.isMonitorOfStandardType() &&
                    Monitor.MonitorType.valueOf(monitor.monitorType.uppercase(Locale.ROOT)) == Monitor.MonitorType.DOC_LEVEL_MONITOR
                ) {
                    try {
                        scope.launch {
                            if (!docLevelMonitorQueries.docLevelQueryIndexExists(monitor.dataSources)) {
                                docLevelMonitorQueries.initDocLevelQueryIndex(monitor.dataSources)
                                log.info("Central Percolation index ${ScheduledJob.DOC_LEVEL_QUERIES_INDEX} created")
                            }
                            val (metadata, _) = MonitorMetadataService.getOrCreateMetadata(monitor, skipIndex = true)
                            docLevelMonitorQueries.indexDocLevelQueries(
                                monitor,
                                monitor.id,
                                metadata,
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
