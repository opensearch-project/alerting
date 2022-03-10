/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.AcknowledgeAlertAction
import org.opensearch.alerting.action.AcknowledgeAlertRequest
import org.opensearch.alerting.util.REFRESH
import org.opensearch.client.node.NodeClient
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

private val log: Logger = LogManager.getLogger(RestAcknowledgeAlertAction::class.java)

/**
 * This class consists of the REST handler to acknowledge alerts.
 * The user provides the monitorID to which these alerts pertain and in the content of the request provides
 * the ids to the alerts he would like to acknowledge.
 */
class RestAcknowledgeAlertAction : BaseRestHandler() {

    override fun getName(): String {
        return "acknowledge_alert_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        // Acknowledge alerts
        return mutableListOf(
            ReplacedRoute(
                POST,
                "${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}/_acknowledge/alerts",
                POST,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/{monitorID}/_acknowledge/alerts"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}/_acknowledge/alerts")

        val monitorId = request.param("monitorID")
        require(!monitorId.isNullOrEmpty()) { "Missing monitor id." }
        val alertIds = getAlertIds(request.contentParser())
        require(alertIds.isNotEmpty()) { "You must provide at least one alert id." }
        val refreshPolicy = RefreshPolicy.parse(request.param(REFRESH, RefreshPolicy.IMMEDIATE.value))

        val acknowledgeAlertRequest = AcknowledgeAlertRequest(monitorId, alertIds, refreshPolicy)
        return RestChannelConsumer { channel ->
            client.execute(AcknowledgeAlertAction.INSTANCE, acknowledgeAlertRequest, RestToXContentListener(channel))
        }
    }

    /**
     * Parse the request content and return a list of the alert ids to acknowledge
     */
    private fun getAlertIds(xcp: XContentParser): List<String> {
        val ids = mutableListOf<String>()
        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()
            when (fieldName) {
                "alerts" -> {
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                    while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                        ids.add(xcp.text())
                    }
                }
            }
        }
        return ids
    }
}
