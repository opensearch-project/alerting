/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandlerv2

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.actionv2.GetAlertsV2Action
import org.opensearch.alerting.actionv2.GetAlertsV2Request
import org.opensearch.commons.alerting.model.Table
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient

/**
 * This class consists of the REST handler to retrieve V2 alerts.
 *
 * @opensearch.experimental
 */
class RestGetAlertsV2Action : BaseRestHandler() {

    private val log = LogManager.getLogger(RestGetAlertsV2Action::class.java)

    override fun getName(): String {
        return "get_alerts_v2_action"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(
                GET,
                "${AlertingPlugin.MONITOR_V2_BASE_URI}/alerts"
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_V2_BASE_URI}/alerts")

        val sortString = request.param("sortString", "monitor_v2_name.keyword")
        val sortOrder = request.param("sortOrder", "asc")
        val missing: String? = request.param("missing")
        val size = request.paramAsInt("size", 20)
        val startIndex = request.paramAsInt("startIndex", 0)
        val searchString = request.param("searchString", "")
        val severityLevel = request.param("severityLevel", "ALL")
        val monitorId: String? = request.param("monitorId")
        val table = Table(
            sortOrder,
            sortString,
            missing,
            size,
            startIndex,
            searchString
        )

        val getAlertsV2Request = GetAlertsV2Request(
            table,
            severityLevel,
            monitorId?.let { listOf(monitorId) }
        )
        return RestChannelConsumer {
                channel ->
            client.execute(GetAlertsV2Action.INSTANCE, getAlertsV2Request, RestToXContentListener(channel))
        }
    }
}
