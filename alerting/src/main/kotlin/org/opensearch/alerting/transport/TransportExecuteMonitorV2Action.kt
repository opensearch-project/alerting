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
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.action.ExecuteMonitorV2Action
import org.opensearch.alerting.action.ExecuteMonitorV2Request
import org.opensearch.alerting.action.ExecuteMonitorV2Response
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.MonitorV2
import org.opensearch.commons.alerting.model.PPLMonitor
import org.opensearch.commons.alerting.model.PPLMonitor.Companion.PPL_MONITOR_TYPE
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.time.Instant

private val log = LogManager.getLogger(TransportExecuteMonitorV2Action::class.java)

class TransportExecuteMonitorV2Action @Inject constructor(
    private val transportService: TransportService,
    private val client: Client,
    private val clusterService: ClusterService,
    private val runner: MonitorRunnerService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    private val settings: Settings
) : HandledTransportAction<ExecuteMonitorV2Request, ExecuteMonitorV2Response>(
    ExecuteMonitorV2Action.NAME, transportService, actionFilters, ::ExecuteMonitorV2Request
) {
    @Volatile private var indexTimeout = AlertingSettings.INDEX_TIMEOUT.get(settings)

    override fun doExecute(
        task: Task,
        execMonitorV2Request: ExecuteMonitorV2Request,
        actionListener: ActionListener<ExecuteMonitorV2Response>
    ) {
//        client.threadPool().threadContext.stashContext().use { // TODO: include this when security plugin enabled
        // first define a function that will be used later to run MonitorV2s
        val executeMonitorV2 = fun (monitorV2: MonitorV2) {
            runner.launch {
                // get execution time interval
                val (periodStart, periodEnd) = if (execMonitorV2Request.requestStart != null) {
                    Pair(
                        Instant.ofEpochMilli(execMonitorV2Request.requestStart.millis),
                        Instant.ofEpochMilli(execMonitorV2Request.requestEnd.millis)
                    )
                } else {
                    monitorV2.schedule.getPeriodEndingAt(Instant.ofEpochMilli(execMonitorV2Request.requestEnd.millis))
                }

                // call the MonitorRunnerService to execute the MonitorV2
                try {
                    val monitorV2Type = when (monitorV2) {
                        is PPLMonitor -> PPL_MONITOR_TYPE
                        else -> throw IllegalStateException("Unexpected MonitorV2 type: ${monitorV2.javaClass.name}")
                    }
                    log.info(
                        "Executing MonitorV2 from API - id: ${monitorV2.id}, type: $monitorV2Type, " +
                            "periodStart: $periodStart, periodEnd: $periodEnd, dryrun: ${execMonitorV2Request.dryrun}"
                    )
                    val monitorV2RunResult = runner.runJobV2(
                        monitorV2,
                        periodStart,
                        periodEnd,
                        execMonitorV2Request.dryrun,
                        transportService
                    )
                    withContext(Dispatchers.IO) {
                        actionListener.onResponse(ExecuteMonitorV2Response(monitorV2RunResult))
                    }
                } catch (e: Exception) {
                    log.error("Unexpected error running monitor", e)
                    withContext(Dispatchers.IO) {
                        actionListener.onFailure(AlertingException.wrap(e))
                    }
                }
            }
        }

        // now execute the MonitorV2
        if (execMonitorV2Request.monitorId != null) { // execute with monitor ID case
            // search the alerting-config index for the MonitorV2 with this ID
            val getMonitorV2Request = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX).id(execMonitorV2Request.monitorId)
            client.get(
                getMonitorV2Request,
                object : ActionListener<GetResponse> {
                    override fun onResponse(getMonitorV2Response: GetResponse) {
                        if (!getMonitorV2Response.isExists) {
                            actionListener.onFailure(
                                AlertingException.wrap(
                                    OpenSearchStatusException(
                                        "Can't find monitorV2 with id: ${getMonitorV2Response.id}",
                                        RestStatus.NOT_FOUND
                                    )
                                )
                            )
                            return
                        }
                        if (!getMonitorV2Response.isSourceEmpty) {
                            log.info("found monitor")
                            XContentHelper.createParser(
                                xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                                getMonitorV2Response.sourceAsBytesRef, XContentType.JSON
                            ).use { xcp ->
                                val monitorV2 = ScheduledJob.parse(
                                    xcp,
                                    getMonitorV2Response.id,
                                    getMonitorV2Response.version
                                ) as MonitorV2
                                // TODO: validate that this is a MonitorV2 and not a Monitor
                                executeMonitorV2(monitorV2)
                            }
                        }
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        } else { // execute with monitor object case
            val monitorV2 = execMonitorV2Request.monitorV2 as MonitorV2
            executeMonitorV2(monitorV2)
        }
//        }
    }
}
