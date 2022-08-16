/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.GetMonitorAction
import org.opensearch.alerting.action.GetMonitorRequest
import org.opensearch.alerting.action.GetMonitorResponse
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
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportGetMonitorAction::class.java)

class TransportGetMonitorAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    val clusterService: ClusterService,
    settings: Settings
) : HandledTransportAction<GetMonitorRequest, GetMonitorResponse> (
    GetMonitorAction.NAME, transportService, actionFilters, ::GetMonitorRequest
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, getMonitorRequest: GetMonitorRequest, actionListener: ActionListener<GetMonitorResponse>) {
        val user = readUserFromThreadContext(client)

        val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, getMonitorRequest.monitorId)
            .version(getMonitorRequest.version)
            .fetchSourceContext(getMonitorRequest.srcContext)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }

        /*
         * Remove security context before you call elasticsearch api's. By this time, permissions required
         * to call this api are validated.
         * Once system-indices [https://github.com/opendistro-for-elasticsearch/security/issues/666] is done, we
         * might further improve this logic. Also change try to kotlin-use for auto-closable.
         */
        client.threadPool().threadContext.stashContext().use {
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            actionListener.onFailure(
                                AlertingException.wrap(OpenSearchStatusException("Monitor not found.", RestStatus.NOT_FOUND))
                            )
                            return
                        }

                        var monitor: Monitor? = null
                        if (!response.isSourceEmpty) {
                            XContentHelper.createParser(
                                xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                                response.sourceAsBytesRef, XContentType.JSON
                            ).use { xcp ->
                                monitor = ScheduledJob.parse(xcp, response.id, response.version) as Monitor

                                // security is enabled and filterby is enabled
                                if (!checkUserPermissionsWithResource(
                                        user,
                                        monitor?.user,
                                        actionListener,
                                        "monitor",
                                        getMonitorRequest.monitorId
                                    )
                                ) {
                                    return
                                }
                            }
                        }

                        actionListener.onResponse(
                            GetMonitorResponse(response.id, response.version, response.seqNo, response.primaryTerm, RestStatus.OK, monitor)
                        )
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        }
    }
}
