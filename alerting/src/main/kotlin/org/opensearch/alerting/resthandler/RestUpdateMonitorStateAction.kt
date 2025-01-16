/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.client.node.NodeClient
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions.UPDATE_MONITOR_STATE_ACTION_TYPE
import org.opensearch.commons.alerting.action.UpdateMonitorStateRequest
import org.opensearch.commons.alerting.action.UpdateMonitorStateResponse
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

private val log = LogManager.getLogger(RestUpdateMonitorStateAction::class.java)

// Rest handler to enable and disable monitors.
class RestUpdateMonitorStateAction : BaseRestHandler() {
    override fun getName(): String {
        return "update_monitor_state_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<RestHandler.ReplacedRoute> {
        return mutableListOf(
            ReplacedRoute(
                PUT,
                "${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}/enable",
                PUT,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/{monitorID}/enable"
            ),
            ReplacedRoute(
                PUT,
                "${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}/disable",
                PUT,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/{monitorID}/disable"
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
        val enabled = request.path().endsWith("/enable")

        log.debug("{} {}/{}/{}", request.method(), AlertingPlugin.MONITOR_BASE_URI, monitorId, if (enabled) "enable" else "disable")

        return RestChannelConsumer { channel ->
            val updateMonitorStateRequest = UpdateMonitorStateRequest(
                monitorId = monitorId,
                enabled = enabled,
                seqNo = -1, // This will be handled in transport layer
                primaryTerm = -1, // This will be handled in transport layer
                method = request.method()
            )

            client.execute(
                UPDATE_MONITOR_STATE_ACTION_TYPE,
                updateMonitorStateRequest,
                object : ActionListener<UpdateMonitorStateResponse> {
                    override fun onResponse(response: UpdateMonitorStateResponse) {
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
