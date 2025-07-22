/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.GetAlertsRequest
import org.opensearch.commons.alerting.model.Table
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient

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
        val findingIdsParam: String? = request.param("findingIds")
        val findingIds: List<String> = findingIdsParam?.split(",")?.map { it.trim() } ?: listOf()
        val workflowId: String? = request.param("workflowIds")
        val workflowIds = mutableListOf<String>()
        if (workflowId.isNullOrEmpty() == false) {
            workflowIds.add(workflowId)
        } else {
            workflowIds.add("")
        }
        val table = Table(
            sortOrder,
            sortString,
            missing,
            size,
            startIndex,
            searchString
        )

        val getAlertsRequest = GetAlertsRequest(table, findingIds = findingIds, severityLevel, alertState, monitorId, null, workflowIds = workflowIds)
        return RestChannelConsumer {
                channel ->
            client.execute(AlertingActions.GET_ALERTS_ACTION_TYPE, getAlertsRequest, RestToXContentListener(channel))
        }
    }
}
