/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transportv2

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.AlertingV2Utils
import org.opensearch.alerting.actionv2.DeleteMonitorV2Action
import org.opensearch.alerting.actionv2.DeleteMonitorV2Request
import org.opensearch.alerting.actionv2.DeleteMonitorV2Response
import org.opensearch.alerting.core.settings.AlertingV2Settings.Companion.ALERTING_V2_ENABLED
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.service.DeleteMonitorService
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.transport.SecureTransportAction
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

private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
private val log = LogManager.getLogger(TransportDeleteMonitorV2Action::class.java)

/**
 * Transport action that contains the core logic for deleting monitor V2s.
 *
 * @opensearch.experimental
 */
class TransportDeleteMonitorV2Action @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<DeleteMonitorV2Request, DeleteMonitorV2Response>(
    DeleteMonitorV2Action.NAME, transportService, actionFilters, ::DeleteMonitorV2Request
),
    SecureTransportAction {

    @Volatile private var alertingV2Enabled = ALERTING_V2_ENABLED.get(settings)

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_V2_ENABLED) { alertingV2Enabled = it }
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: DeleteMonitorV2Request, actionListener: ActionListener<DeleteMonitorV2Response>) {
        if (!alertingV2Enabled) {
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException(
                        "Alerting V2 is currently disabled, please enable it with the " +
                            "cluster setting: ${ALERTING_V2_ENABLED.key}.",
                        RestStatus.FORBIDDEN
                    ),
                )
            )
            return
        }

        val user = readUserFromThreadContext(client)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }

        scope.launch {
            try {
                val monitorV2 = getMonitorV2(request.monitorV2Id, actionListener) ?: return@launch

                val canDelete = user == null || !doFilterForUser(user) ||
                    checkUserPermissionsWithResource(user, monitorV2!!.user, actionListener, "monitor_v2", request.monitorV2Id)

                if (canDelete) {
                    val deleteResponse =
                        DeleteMonitorService.deleteMonitorV2(request.monitorV2Id, request.refreshPolicy)
                    actionListener.onResponse(deleteResponse)
                } else {
                    actionListener.onFailure(
                        AlertingException(
                            "Not allowed to delete this Monitor V2",
                            RestStatus.FORBIDDEN,
                            IllegalStateException()
                        )
                    )
                }
            } catch (e: Exception) {
                actionListener.onFailure(e)
            }

            // scheduled AlertV2Mover will sweep the alerts and find that this monitor no longer exists,
            // and expire this monitor's alerts accordingly
        }
    }

    private suspend fun getMonitorV2(monitorV2Id: String, actionListener: ActionListener<DeleteMonitorV2Response>): MonitorV2? {
        val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, monitorV2Id)

        val getResponse: GetResponse = client.suspendUntil { get(getRequest, it) }
        if (!getResponse.isExists) {
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException("Monitor V2 with $monitorV2Id is not found", RestStatus.NOT_FOUND)
                )
            )
            return null
        }

        val xcp = XContentHelper.createParser(
            xContentRegistry, LoggingDeprecationHandler.INSTANCE,
            getResponse.sourceAsBytesRef, XContentType.JSON
        )
        val scheduledJob = ScheduledJob.parse(xcp, getResponse.id, getResponse.version)

        AlertingV2Utils.validateMonitorV2(scheduledJob)?.let {
            actionListener.onFailure(AlertingException.wrap(it))
            return null
        }

        val monitorV2 = scheduledJob as MonitorV2

        return monitorV2
    }
}
