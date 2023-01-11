/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.MonitorExplainAction
import org.opensearch.alerting.action.MonitorExplainRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener

private val log: Logger = LogManager.getLogger(RestMonitorExplainAction::class.java)
class RestMonitorExplainAction : BaseRestHandler() {
    override fun getName(): String {
        return "monitor_explain_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<RestHandler.ReplacedRoute> {
        return mutableListOf(
            // Search for monitors
            RestHandler.ReplacedRoute(
                RestRequest.Method.GET,
                "${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}/_explain",
                RestRequest.Method.GET,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/{monitorID}/_explain"
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}/_explain")

        val monitorId = request.param("monitorID")

        val docDiffString = request.param("doc_diff")
        val docDiff = docDiffString?.toBoolean() ?: false

        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/$monitorId")

        val monitorExplainRequest = MonitorExplainRequest(monitorId, docDiff)

        return RestChannelConsumer { channel -> client.execute(MonitorExplainAction.INSTANCE, monitorExplainRequest, RestToXContentListener(channel)) }
    }
}
