/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transportv2

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.AlertingV2Utils.isIndexNotFoundException
import org.opensearch.alerting.AlertingV2Utils.validateMonitorV2
import org.opensearch.alerting.actionv2.GetMonitorV2Action
import org.opensearch.alerting.actionv2.GetMonitorV2Request
import org.opensearch.alerting.actionv2.GetMonitorV2Response
import org.opensearch.alerting.core.settings.AlertingV2Settings.Companion.ALERTING_V2_ENABLED
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.transport.SecureTransportAction
import org.opensearch.alerting.transport.TransportGetMonitorAction
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

private val log = LogManager.getLogger(TransportGetMonitorAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportGetMonitorV2Action @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    val clusterService: ClusterService,
    settings: Settings,
) : HandledTransportAction<GetMonitorV2Request, GetMonitorV2Response>(
    GetMonitorV2Action.NAME,
    transportService,
    actionFilters,
    ::GetMonitorV2Request
),
    SecureTransportAction {

    @Volatile private var alertingV2Enabled = ALERTING_V2_ENABLED.get(settings)

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_V2_ENABLED) { alertingV2Enabled = it }
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: GetMonitorV2Request, actionListener: ActionListener<GetMonitorV2Response>) {
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

        val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, request.monitorV2Id)
            .version(request.version)
            .fetchSourceContext(request.srcContext)

        val user = readUserFromThreadContext(client)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }

        client.threadPool().threadContext.stashContext().use {
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            actionListener.onFailure(
                                AlertingException.wrap(OpenSearchStatusException("MonitorV2 not found.", RestStatus.NOT_FOUND))
                            )
                            return
                        }

                        if (response.isSourceEmpty) {
                            actionListener.onFailure(
                                AlertingException.wrap(OpenSearchStatusException("MonitorV2 found but was empty.", RestStatus.NO_CONTENT))
                            )
                            return
                        }

                        val xcp = XContentHelper.createParser(
                            xContentRegistry,
                            LoggingDeprecationHandler.INSTANCE,
                            response.sourceAsBytesRef,
                            XContentType.JSON
                        )

                        val scheduledJob = ScheduledJob.parse(xcp, response.id, response.version)

                        validateMonitorV2(scheduledJob)?.let {
                            actionListener.onFailure(AlertingException.wrap(it))
                            return
                        }

                        val monitorV2 = scheduledJob as MonitorV2

                        // security is enabled and filterby is enabled
                        if (!checkUserPermissionsWithResource(
                                user,
                                monitorV2.user,
                                actionListener,
                                "monitor",
                                request.monitorV2Id
                            )
                        ) {
                            return
                        }

                        actionListener.onResponse(
                            GetMonitorV2Response(
                                response.id,
                                response.version,
                                response.seqNo,
                                response.primaryTerm,
                                monitorV2
                            )
                        )
                    }

                    override fun onFailure(e: Exception) {
                        if (isIndexNotFoundException(e)) {
                            log.error("Index not found while getting monitor V2", e)
                            actionListener.onFailure(
                                AlertingException.wrap(
                                    OpenSearchStatusException("Monitor V2 not found. Backing index is missing.", RestStatus.NOT_FOUND, e)
                                )
                            )
                        } else {
                            log.error("Unexpected error while getting monitor", e)
                            actionListener.onFailure(AlertingException.wrap(e))
                        }
                    }
                }
            )
        }
    }
}
