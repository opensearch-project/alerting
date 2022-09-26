/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.GetAlertsAction
import org.opensearch.alerting.action.GetAlertsRequest
import org.opensearch.alerting.model.Table
import org.opensearch.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.action.RestToXContentListener

/**
 * This class consists of the REST handler to retrieve alerts .
 */
class RestGetAlertsAction : BaseRestHandler() {

    private val log = LogManager.getLogger(RestGetAlertsAction::class.java)

    override fun getName(): String {
        return "get_alerts_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            ReplacedRoute(
                GET,
                "${AlertingPlugin.MONITOR_BASE_URI}/alerts",
                GET,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/alerts"
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/alerts")

        val sortString = request.param("sortString", "monitor_name.keyword")
        val sortOrder = request.param("sortOrder", "asc")
        val missing: String? = request.param("missing")
        val size = request.paramAsInt("size", 20)
        val startIndex = request.paramAsInt("startIndex", 0)
        val searchString = request.param("searchString", "")
        val severityLevel = request.param("severityLevel", "ALL")
        val alertState = request.param("alertState", "ALL")
        val monitorId: String? = request.param("monitorId")
        val table = Table(
            sortOrder,
            sortString,
            missing,
            size,
            startIndex,
            searchString
        )

        val getAlertsRequest = GetAlertsRequest(table, severityLevel, alertState, monitorId, null)
        return RestChannelConsumer {
                channel ->
            client.execute(GetAlertsAction.INSTANCE, getAlertsRequest, RestToXContentListener(channel))
        }
    }
}
