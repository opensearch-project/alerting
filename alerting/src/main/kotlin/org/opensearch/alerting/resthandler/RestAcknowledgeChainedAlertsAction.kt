/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.commons.alerting.action.AcknowledgeChainedAlertRequest
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient
import java.io.IOException

private val log: Logger = LogManager.getLogger(RestAcknowledgeAlertAction::class.java)

/**
 * This class consists of the REST handler to acknowledge chained alerts.
 * The user provides the workflowID to which these alerts pertain and in the content of the request provides
 * the ids to the chained alerts user would like to acknowledge.
 */
class RestAcknowledgeChainedAlertsAction : BaseRestHandler() {
    override fun getName(): String = "acknowledge_chained_alert_action"

    override fun routes(): List<Route> {
        // Acknowledge alerts
        return mutableListOf(
            Route(
                POST,
                "${AlertingPlugin.WORKFLOW_BASE_URI}/{workflowID}/_acknowledge/alerts",
            ),
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(
        request: RestRequest,
        client: NodeClient,
    ): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.WORKFLOW_BASE_URI}/{workflowID}/_acknowledge/alerts")

        val workflowId = request.param("workflowID")
        require(!workflowId.isNullOrEmpty()) { "Missing workflow id." }
        val alertIds = getAlertIds(request.contentParser())
        require(alertIds.isNotEmpty()) { "You must provide at least one alert id." }

        val acknowledgeAlertRequest = AcknowledgeChainedAlertRequest(workflowId, alertIds)
        return RestChannelConsumer { channel ->
            client.execute(AlertingActions.ACKNOWLEDGE_CHAINED_ALERTS_ACTION_TYPE, acknowledgeAlertRequest, RestToXContentListener(channel))
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
