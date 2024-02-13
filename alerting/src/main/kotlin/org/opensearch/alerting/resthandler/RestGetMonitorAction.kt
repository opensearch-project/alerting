/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.util.context
import org.opensearch.client.node.NodeClient
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.GetMonitorRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.RestRequest.Method.HEAD
import org.opensearch.rest.action.RestActions
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.search.fetch.subphase.FetchSourceContext

private val log = LogManager.getLogger(RestGetMonitorAction::class.java)

/**
 * This class consists of the REST handler to retrieve a monitor .
 */
class RestGetMonitorAction : BaseRestHandler() {

    override fun getName(): String {
        return "get_monitor_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            // Get a specific monitor
            ReplacedRoute(
                GET,
                "${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}",
                GET,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/{monitorID}"
            ),
            ReplacedRoute(
                HEAD,
                "${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}",
                HEAD,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/{monitorID}"
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}")

        val monitorId = request.param("monitorID")
        if (monitorId == null || monitorId.isEmpty()) {
            throw IllegalArgumentException("missing id")
        }

        var srcContext = context(request)
        if (request.method() == HEAD) {
            srcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        }
        val getMonitorRequest = GetMonitorRequest(monitorId, RestActions.parseVersion(request), request.method(), srcContext)
        return RestChannelConsumer {
                channel ->
            client.execute(AlertingActions.GET_MONITOR_ACTION_TYPE, getMonitorRequest, RestToXContentListener(channel))
        }
    }
}
