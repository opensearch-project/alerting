/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.client.node.NodeClient
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions.TOGGLE_MONITOR_ACTION_TYPE
import org.opensearch.commons.alerting.action.ToggleMonitorRequest
import org.opensearch.commons.alerting.action.ToggleMonitorResponse
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.PUT
import java.io.IOException

private val log = LogManager.getLogger(RestToggleMonitorAction::class.java)

class RestToggleMonitorAction : BaseRestHandler() {
    override fun getName(): String {
        return "toggle_monitor_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<RestHandler.ReplacedRoute> {
        return mutableListOf(
            ReplacedRoute(
                PUT,
                "${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}/_enable",
                PUT,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/{monitorID}/_enable"
            ),
            ReplacedRoute(
                PUT,
                "${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}/_disable",
                PUT,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/{monitorID}/_disable"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val monitorId = request.param("monitorID", Monitor.NO_ID)
        if (request.method() == PUT && Monitor.NO_ID == monitorId) {
            throw AlertingException.wrap(IllegalArgumentException("Missing monitor ID"))
        }

        // Check if the request is being made to enable the monitor
        val enabled = request.path().endsWith("/_enable")

        log.debug("{} {}/{}/{}", request.method(), AlertingPlugin.MONITOR_BASE_URI, monitorId, if (enabled) "enable" else "disable")

        return RestChannelConsumer { channel ->
            val toggleMonitorRequest = ToggleMonitorRequest(
                monitorId = monitorId,
                enabled = enabled,
                seqNo = -1, // Updated in the transport layer
                primaryTerm = -1, // Updated in the transport layer
                method = request.method()
            )

            client.execute(
                TOGGLE_MONITOR_ACTION_TYPE,
                toggleMonitorRequest,
                object : ActionListener<ToggleMonitorResponse> {
                    override fun onResponse(response: ToggleMonitorResponse) {
                        channel.sendResponse(
                            BytesRestResponse(
                                RestStatus.OK,
                                response.toXContent(
                                    XContentBuilder.builder(
                                        XContentType.JSON.xContent()
                                    ),
                                    ToXContent.EMPTY_PARAMS
                                )
                            )
                        )
                    }
                    override fun onFailure(e: Exception) {
                        channel.sendResponse(BytesRestResponse(channel, e))
                    }
                }
            )
        }
    }
}
