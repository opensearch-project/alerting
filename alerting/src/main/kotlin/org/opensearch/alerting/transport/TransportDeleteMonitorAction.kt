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
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.service.DeleteMonitorService
import org.opensearch.alerting.settings.AlertingSettings
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
import org.opensearch.remote.metadata.client.GetDataObjectRequest
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
private val log = LogManager.getLogger(TransportDeleteMonitorAction::class.java)

class TransportDeleteMonitorAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
    val sdkClient: SdkClient
) : HandledTransportAction<ActionRequest, DeleteMonitorResponse>(
    AlertingActions.DELETE_MONITOR_ACTION_NAME, transportService, actionFilters, ::DeleteMonitorRequest
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
                    return
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
            val tenantId = client.threadPool().threadContext.getHeader(AlertingPlugin.TENANT_ID_HEADER)
            val getRequest = GetDataObjectRequest.builder()
                .index(ScheduledJob.SCHEDULED_JOBS_INDEX)
                .id(monitorId)
                .tenantId(tenantId)
                .build()

            try {
                val response = sdkClient.getDataObject(getRequest)
                val getResponse = response.getResponse()
                if (getResponse == null || !getResponse.isExists) {
                    throw OpenSearchStatusException("Monitor with $monitorId is not found", RestStatus.NOT_FOUND)
                }
                val xcp = XContentHelper.createParser(
                    xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    getResponse.sourceAsBytesRef, XContentType.JSON
                )
                return ScheduledJob.parse(xcp, getResponse.id, getResponse.version) as Monitor
            } catch (e: Exception) {
                if (e is OpenSearchStatusException) throw e
                log.error("GetMonitor operation failed for $monitorId", e)
                throw OpenSearchStatusException("Monitor with $monitorId is not found", RestStatus.NOT_FOUND, e)
            }
        }
    }
}
