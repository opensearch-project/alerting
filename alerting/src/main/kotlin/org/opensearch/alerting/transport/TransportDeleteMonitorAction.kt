/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.service.DeleteMonitorService
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.DeleteMonitorRequest
import org.opensearch.commons.alerting.action.DeleteMonitorResponse
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
private val log = LogManager.getLogger(TransportDeleteMonitorAction::class.java)

class TransportDeleteMonitorAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<ActionRequest, DeleteMonitorResponse>(
    AlertingActions.DELETE_MONITOR_ACTION_NAME,
    transportService,
    actionFilters,
    ::DeleteMonitorRequest
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: ActionRequest, actionListener: ActionListener<DeleteMonitorResponse>) {
        val transformedRequest = request as? DeleteMonitorRequest
            ?: recreateObject(request) { DeleteMonitorRequest(it) }
        val user = readUserFromThreadContext(client)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }
        scope.launch {
            DeleteMonitorHandler(
                client,
                actionListener,
                user,
                transformedRequest.monitorId
            ).resolveUserAndStart(transformedRequest.refreshPolicy)
        }
    }

    inner class DeleteMonitorHandler(
        private val client: Client,
        private val actionListener: ActionListener<DeleteMonitorResponse>,
        private val user: User?,
        private val monitorId: String
    ) {
        suspend fun resolveUserAndStart(refreshPolicy: RefreshPolicy) {
            try {
                val monitor = getMonitor()

                val canDelete = user == null || !doFilterForUser(user) ||
                    checkUserPermissionsWithResource(user, monitor.user, actionListener, "monitor", monitorId)

                if (DeleteMonitorService.monitorIsWorkflowDelegate(monitor.id)) {
                    actionListener.onFailure(
                        AlertingException(
                            "Monitor can't be deleted because it is a part of workflow(s)",
                            RestStatus.FORBIDDEN,
                            IllegalStateException()
                        )
                    )
                } else if (canDelete) {
                    actionListener.onResponse(
                        DeleteMonitorService.deleteMonitor(monitor, refreshPolicy)
                    )
                } else {
                    actionListener.onFailure(
                        AlertingException("Not allowed to delete this monitor!", RestStatus.FORBIDDEN, IllegalStateException())
                    )
                }
            } catch (t: Exception) {
                log.error("Failed to delete monitor $monitorId", t)
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun getMonitor(): Monitor {
            val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, monitorId)

            val getResponse: GetResponse = client.suspendUntil { get(getRequest, it) }
            if (getResponse.isExists == false) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException("Monitor with $monitorId is not found", RestStatus.NOT_FOUND)
                    )
                )
            }
            val xcp = XContentHelper.createParser(
                xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                getResponse.sourceAsBytesRef,
                XContentType.JSON
            )
            return ScheduledJob.parse(xcp, getResponse.id, getResponse.version) as Monitor
        }
    }
}
