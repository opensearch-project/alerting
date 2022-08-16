/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.DeleteMonitorAction
import org.opensearch.alerting.action.DeleteMonitorRequest
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.util.REFRESH
import org.opensearch.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.DELETE
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

private val log: Logger = LogManager.getLogger(RestDeleteMonitorAction::class.java)
/**
 * This class consists of the REST handler to delete monitors.
 * When a monitor is deleted, all alerts are moved to the [Alert.State.DELETED] state and moved to the alert history index.
 * If this process fails the monitor is not deleted.
 */
class RestDeleteMonitorAction : BaseRestHandler() {

    override fun getName(): String {
        return "delete_monitor_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            ReplacedRoute(
                DELETE,
                "${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}",
                DELETE,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/{monitorID}"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}")

        val monitorId = request.param("monitorID")
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/$monitorId")

        val refreshPolicy = RefreshPolicy.parse(request.param(REFRESH, RefreshPolicy.IMMEDIATE.value))
        val deleteMonitorRequest = DeleteMonitorRequest(monitorId, refreshPolicy)

        return RestChannelConsumer { channel ->
            client.execute(DeleteMonitorAction.INSTANCE, deleteMonitorRequest, RestToXContentListener(channel))
        }
    }
}
