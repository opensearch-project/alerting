/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.DeleteMonitorAction
import org.opensearch.alerting.action.DeleteMonitorRequest
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.DeleteByQueryAction
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.io.IOException

private val log = LogManager.getLogger(TransportDeleteMonitorAction::class.java)

class TransportDeleteMonitorAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<DeleteMonitorRequest, DeleteResponse>(
    DeleteMonitorAction.NAME, transportService, actionFilters, ::DeleteMonitorRequest
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: DeleteMonitorRequest, actionListener: ActionListener<DeleteResponse>) {
        val user = readUserFromThreadContext(client)
        val deleteRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, request.monitorId)
            .setRefreshPolicy(request.refreshPolicy)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }
        client.threadPool().threadContext.stashContext().use {
            DeleteMonitorHandler(client, actionListener, deleteRequest, user, request.monitorId).resolveUserAndStart()
        }
    }

    inner class DeleteMonitorHandler(
        private val client: Client,
        private val actionListener: ActionListener<DeleteResponse>,
        private val deleteRequest: DeleteRequest,
        private val user: User?,
        private val monitorId: String
    ) {
        fun resolveUserAndStart() {
            val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, monitorId)
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            actionListener.onFailure(
                                AlertingException.wrap(
                                    OpenSearchStatusException("Monitor with $monitorId is not found", RestStatus.NOT_FOUND)
                                )
                            )
                            return
                        }
                        val xcp = XContentHelper.createParser(
                            xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                            response.sourceAsBytesRef, XContentType.JSON
                        )
                        val monitor = ScheduledJob.parse(xcp, response.id, response.version) as Monitor
                        onGetResponse(monitor)
                    }
                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        }

        private fun onGetResponse(monitor: Monitor) {
            if (user == null) {
                // Security is disabled, so we can delete the destination without issues
                deleteMonitor(monitor)
            } else if (!doFilterForUser(user)) {
                // security is enabled and filterby is disabled.
                deleteMonitor(monitor)
            } else {
                try {
                    if (!checkUserPermissionsWithResource(user, monitor.user, actionListener, "monitor", monitorId)) {
                        return
                    } else {
                        deleteMonitor(monitor)
                    }
                } catch (ex: IOException) {
                    actionListener.onFailure(AlertingException.wrap(ex))
                }
            }
        }

        private fun deleteMonitor(monitor: Monitor) {
            client.delete(
                deleteRequest,
                object : ActionListener<DeleteResponse> {
                    override fun onResponse(response: DeleteResponse) {
                        val clusterState = clusterService.state()
                        if (clusterState.routingTable.hasIndex(monitor.dataSources.queryIndex)) {
                            deleteDocLevelMonitorQueries(monitor)
                        }
                        deleteMetadata()

                        actionListener.onResponse(response)
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        }

        private fun deleteMetadata() {
            val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, monitorId)
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (response.isExists) {
                            val deleteMetadataRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, "$monitorId")
                                .setRefreshPolicy(deleteRequest.refreshPolicy)
                            client.delete(
                                deleteMetadataRequest,
                                object : ActionListener<DeleteResponse> {
                                    override fun onResponse(response: DeleteResponse) {
                                    }

                                    override fun onFailure(t: Exception) {
                                    }
                                }
                            )
                        }
                    }
                    override fun onFailure(t: Exception) {
                    }
                }
            )
        }

        private fun deleteDocLevelMonitorQueries(monitor: Monitor) {
            DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                .source(monitor.dataSources.queryIndex)
                .filter(QueryBuilders.matchQuery("monitor_id", monitorId))
                .execute(
                    object : ActionListener<BulkByScrollResponse> {
                        override fun onResponse(response: BulkByScrollResponse) {
                        }

                        override fun onFailure(t: Exception) {
                        }
                    }
                )
        }
    }
}
