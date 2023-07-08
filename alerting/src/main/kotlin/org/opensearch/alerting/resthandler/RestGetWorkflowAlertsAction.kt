/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.client.node.NodeClient
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.GetWorkflowAlertsRequest
import org.opensearch.commons.alerting.model.Table
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
class RestGetWorkflowAlertsAction : BaseRestHandler() {

    private val log = LogManager.getLogger(RestGetWorkflowAlertsAction::class.java)

    override fun getName(): String {
        return "get_workflow_alerts_action"
    }

    override fun routes(): List<Route> {
        return mutableListOf(
            Route(
                GET,
                "${AlertingPlugin.WORKFLOW_BASE_URI}/alerts"
            )
        )
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf()
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.WORKFLOW_BASE_URI}/alerts")

        val sortString = request.param("sortString", "monitor_name.keyword")
        val sortOrder = request.param("sortOrder", "asc")
        val missing: String? = request.param("missing")
        val size = request.paramAsInt("size", 20)
        val startIndex = request.paramAsInt("startIndex", 0)
        val searchString = request.param("searchString", "")
        val severityLevel = request.param("severityLevel", "ALL")
        val alertState = request.param("alertState", "ALL")
        val workflowId: String? = request.param("workflowIds")
        val getAssociatedAlerts: Boolean = request.param("getAssociatedAlerts", "false").toBoolean()
        val workflowIds = mutableListOf<String>()
        if (workflowId.isNullOrEmpty() == false) {
            workflowIds.add(workflowId)
        }
        val table = Table(
            sortOrder,
            sortString,
            missing,
            size,
            startIndex,
            searchString
        )

        val getWorkflowAlertsRequest = GetWorkflowAlertsRequest(
            table,
            severityLevel,
            alertState,
            alertIndex = null,
            associatedAlertsIndex = null,
            workflowIds = workflowIds,
            monitorIds = emptyList(),
            getAssociatedAlerts = getAssociatedAlerts
        )
        return RestChannelConsumer { channel ->
            client.execute(AlertingActions.GET_WORKFLOW_ALERTS_ACTION_TYPE, getWorkflowAlertsRequest, RestToXContentListener(channel))
        }
    }
}
