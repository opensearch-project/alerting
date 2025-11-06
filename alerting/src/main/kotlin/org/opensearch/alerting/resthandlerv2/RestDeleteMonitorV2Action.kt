/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandlerv2

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.actionv2.DeleteMonitorV2Action
import org.opensearch.alerting.actionv2.DeleteMonitorV2Request
import org.opensearch.alerting.util.REFRESH
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.DELETE
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient
import java.io.IOException

private val log: Logger = LogManager.getLogger(RestDeleteMonitorV2Action::class.java)

class RestDeleteMonitorV2Action : BaseRestHandler() {

    override fun getName(): String {
        return "delete_monitor_v2_action"
    }

    override fun routes(): List<Route> {
        return mutableListOf(
            Route(
                DELETE,
                "${AlertingPlugin.MONITOR_V2_BASE_URI}/{monitor_id}"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val monitorV2Id = request.param("monitor_id")
        log.info("${request.method()} ${AlertingPlugin.MONITOR_V2_BASE_URI}/$monitorV2Id")

        val refreshPolicy = RefreshPolicy.parse(request.param(REFRESH, RefreshPolicy.IMMEDIATE.value))
        val deleteMonitorV2Request = DeleteMonitorV2Request(monitorV2Id, refreshPolicy)

        return RestChannelConsumer { channel ->
            client.execute(DeleteMonitorV2Action.INSTANCE, deleteMonitorV2Request, RestToXContentListener(channel))
        }
    }
}
