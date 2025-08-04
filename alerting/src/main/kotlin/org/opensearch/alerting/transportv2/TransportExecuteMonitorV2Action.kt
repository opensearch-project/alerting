/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transportv2

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.AlertingV2Utils.validateMonitorV2
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.actionv2.ExecuteMonitorV2Action
import org.opensearch.alerting.actionv2.ExecuteMonitorV2Request
import org.opensearch.alerting.actionv2.ExecuteMonitorV2Response
import org.opensearch.alerting.core.settings.AlertingV2Settings.Companion.ALERTING_V2_ENABLED
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.modelv2.PPLSQLMonitor
import org.opensearch.alerting.modelv2.PPLSQLMonitor.Companion.PPL_SQL_MONITOR_TYPE
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.transport.SecureTransportAction
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.authuser.User
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
),
    SecureTransportAction {

    @Volatile private var alertingV2Enabled = ALERTING_V2_ENABLED.get(settings)

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_V2_ENABLED) { alertingV2Enabled = it }
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(
        task: Task,
        execMonitorV2Request: ExecuteMonitorV2Request,
        actionListener: ActionListener<ExecuteMonitorV2Response>
    ) {
        if (!alertingV2Enabled) {
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException(
                        "Alerting V2 is currently disabled, please enable it with the " +
                            "cluster setting: ${ALERTING_V2_ENABLED.key}",
                        RestStatus.FORBIDDEN
                    ),
                )
            )
            return
        }

        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
        log.debug("User and roles string from thread context: $userStr")
        val user: User? = User.parse(userStr)

        client.threadPool().threadContext.stashContext().use {
            /* first define a function that will be used later to run MonitorV2s */
            val executeMonitorV2 = fun (monitorV2: MonitorV2) {
                runner.launch {
                    // get execution end, this will be used to compute the execution interval
                    // via look back window (if one is supplied)
                    val periodEnd = Instant.ofEpochMilli(execMonitorV2Request.requestEnd.millis)

                    // call the MonitorRunnerService to execute the MonitorV2
                    try {
                        val monitorV2Type = when (monitorV2) {
                            is PPLSQLMonitor -> PPL_SQL_MONITOR_TYPE
                            else -> throw IllegalStateException("Unexpected MonitorV2 type: ${monitorV2.javaClass.name}")
                        }
                        log.info(
                            "Executing MonitorV2 from API - id: ${monitorV2.id}, type: $monitorV2Type, " +
                                "periodEnd: $periodEnd, manual: ${execMonitorV2Request.manual}"
                        )
                        val monitorV2RunResult = runner.runJobV2(
                            monitorV2,
                            periodEnd,
                            execMonitorV2Request.dryrun,
                            execMonitorV2Request.manual,
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

            /* now execute the MonitorV2 */

            // if both monitor_v2 id and object were passed in, ignore object and proceed with id
            if (execMonitorV2Request.monitorV2Id != null && execMonitorV2Request.monitorV2 != null) {
                log.info(
                    "Both a monitor_v2 id and monitor_v2 object were passed in to ExecuteMonitorV2" +
                        "request. Proceeding to execute by monitor_v2 ID and ignoring monitor_v2 object."
                )
            }

            if (execMonitorV2Request.monitorV2Id != null) { // execute with monitor ID case
                // search the alerting-config index for the MonitorV2 with this ID
                val getMonitorV2Request = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX).id(execMonitorV2Request.monitorV2Id)
                client.get(
                    getMonitorV2Request,
                    object : ActionListener<GetResponse> {
                        override fun onResponse(getMonitorV2Response: GetResponse) {
                            if (!getMonitorV2Response.isExists) {
                                actionListener.onFailure(
                                    AlertingException.wrap(
                                        OpenSearchStatusException(
                                            "Can't find monitorV2 with id: ${getMonitorV2Response.id} to execute",
                                            RestStatus.NOT_FOUND
                                        )
                                    )
                                )
                                return
                            }

                            if (getMonitorV2Response.isSourceEmpty) {
                                actionListener.onFailure(
                                    AlertingException.wrap(
                                        OpenSearchStatusException(
                                            "Found monitorV2 with id: ${getMonitorV2Response.id} but it was empty",
                                            RestStatus.NO_CONTENT
                                        )
                                    )
                                )
                                return
                            }

                            val xcp = XContentHelper.createParser(
                                xContentRegistry,
                                LoggingDeprecationHandler.INSTANCE,
                                getMonitorV2Response.sourceAsBytesRef,
                                XContentType.JSON
                            )

                            val scheduledJob = ScheduledJob.parse(xcp, getMonitorV2Response.id, getMonitorV2Response.version)

                            validateMonitorV2(scheduledJob)?.let {
                                actionListener.onFailure(AlertingException.wrap(it))
                                return
                            }

                            val monitorV2 = scheduledJob as MonitorV2

                            // security is enabled and filterby is enabled
                            // only run this check on manual executions,
                            // automatic scheduled job executions should
                            // bypass this check and proceed to execution
                            if (execMonitorV2Request.manual &&
                                !checkUserPermissionsWithResource(
                                        user,
                                        monitorV2.user,
                                        actionListener,
                                        "monitor",
                                        execMonitorV2Request.monitorV2Id
                                    )
                            ) {
                                return
                            }

                            try {
                                executeMonitorV2(monitorV2)
                            } catch (e: Exception) {
                                actionListener.onFailure(AlertingException.wrap(e))
                            }
                        }

                        override fun onFailure(t: Exception) {
                            actionListener.onFailure(AlertingException.wrap(t))
                        }
                    }
                )
            } else { // execute with monitor object case
                try {
                    val monitorV2 = execMonitorV2Request.monitorV2!!.makeCopy(user = user)
                    executeMonitorV2(monitorV2)
                } catch (e: Exception) {
                    actionListener.onFailure(AlertingException.wrap(e))
                }
            }
        }
    }
}
