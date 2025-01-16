/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.opensearch.OpenSearchStatusException
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.common.inject.Inject
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.AlertingActions.GET_MONITOR_ACTION_TYPE
import org.opensearch.commons.alerting.action.AlertingActions.INDEX_MONITOR_ACTION_TYPE
import org.opensearch.commons.alerting.action.GetMonitorRequest
import org.opensearch.commons.alerting.action.GetMonitorResponse
import org.opensearch.commons.alerting.action.IndexMonitorRequest
import org.opensearch.commons.alerting.action.IndexMonitorResponse
import org.opensearch.commons.alerting.action.ToggleMonitorRequest
import org.opensearch.commons.alerting.action.ToggleMonitorResponse
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.core.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.time.Instant

class TransportToggleMonitorAction @Inject constructor(
    transportService: TransportService,
    val client: NodeClient,
    val namedWriteableRegistry: NamedWriteableRegistry,
    actionFilters: ActionFilters
) : HandledTransportAction<ToggleMonitorRequest, ToggleMonitorResponse>(
    AlertingActions.TOGGLE_MONITOR_ACTION_NAME,
    transportService,
    actionFilters,
    ::ToggleMonitorRequest
) {

    override fun doExecute(
        task: Task,
        request: ToggleMonitorRequest,
        actionListener: ActionListener<ToggleMonitorResponse>
    ) {
        val monitorId = request.monitorId
        val enabled = request.enabled

        val getMonitorRequest = GetMonitorRequest(
            monitorId = monitorId,
            version = -3L,
            method = request.method,
            srcContext = null
        )

        client.execute(
            GET_MONITOR_ACTION_TYPE,
            getMonitorRequest,
            object : ActionListener<GetMonitorResponse> {
                override fun onResponse(getMonitorResponse: GetMonitorResponse) {
                    try {
                        if (getMonitorResponse.monitor == null) {
                            actionListener.onFailure(
                                OpenSearchStatusException("Monitor $monitorId not found", RestStatus.NOT_FOUND)
                            )
                            return
                        }

                        if (getMonitorResponse.monitor!!.enabled == enabled) {
                            actionListener.onFailure(
                                OpenSearchStatusException(
                                    "Monitor $monitorId is already ${if (enabled) "enabled" else "disabled"}",
                                    RestStatus.BAD_REQUEST
                                )
                            )
                            return
                        }

                        // Create a copy of the monitor with the updated state
                        val updatedMonitor = getMonitorResponse.monitor!!.copy(
                            enabled = enabled,
                            enabledTime = if (enabled) Instant.now() else null
                        )

                        // Call indexMonitor API to update the monitor
                        val indexMonitorRequest = IndexMonitorRequest(
                            monitorId = monitorId,
                            seqNo = getMonitorResponse.seqNo,
                            primaryTerm = getMonitorResponse.primaryTerm,
                            refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE,
                            method = request.method,
                            monitor = updatedMonitor,
                        )

                        client.execute(
                            INDEX_MONITOR_ACTION_TYPE,
                            indexMonitorRequest,
                            object : ActionListener<IndexMonitorResponse> {
                                override fun onResponse(indexMonitorResponse: IndexMonitorResponse) {
                                    actionListener.onResponse(
                                        ToggleMonitorResponse(
                                            id = monitorId,
                                            version = indexMonitorResponse.version,
                                            seqNo = indexMonitorResponse.seqNo,
                                            primaryTerm = indexMonitorResponse.primaryTerm,
                                            monitor = updatedMonitor
                                        )
                                    )
                                }

                                override fun onFailure(e: Exception) {
                                    actionListener.onFailure(e)
                                }
                            }
                        )
                    } catch (e: Exception) {
                        actionListener.onFailure(e)
                    }
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(e)
                }
            }
        )
    }
}
