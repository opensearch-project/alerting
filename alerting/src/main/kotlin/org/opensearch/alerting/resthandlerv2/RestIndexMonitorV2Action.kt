/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandlerv2

import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.actionv2.IndexMonitorV2Action
import org.opensearch.alerting.actionv2.IndexMonitorV2Request
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.util.IF_PRIMARY_TERM
import org.opensearch.alerting.util.IF_SEQ_NO
import org.opensearch.alerting.util.REFRESH
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.RestRequest.Method.PUT
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient
import java.io.IOException

private val log = LogManager.getLogger(RestIndexMonitorV2Action::class.java)

/**
 * Rest handlers to create and update V2 Monitors like PPL Monitors
 */
class RestIndexMonitorV2Action : BaseRestHandler() {
    override fun getName(): String {
        return "index_monitor_v2_action"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(
                POST,
                AlertingPlugin.MONITOR_V2_BASE_URI
            ),
            Route(
                PUT,
                "${AlertingPlugin.MONITOR_V2_BASE_URI}/{monitor_id}"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${request.path()}")

        val xcp = request.contentParser()
        ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)

        val monitorV2: MonitorV2
        val rbacRoles: List<String>?
        try {
            monitorV2 = MonitorV2.parse(xcp)
            rbacRoles = request.contentParser().map()["rbac_roles"] as List<String>?
        } catch (e: Exception) {
            throw AlertingException.wrap(IllegalArgumentException(e.localizedMessage))
        }

        val id = request.param("monitor_id", MonitorV2.NO_ID)
        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }

        val indexMonitorV2Request = IndexMonitorV2Request(id, seqNo, primaryTerm, refreshPolicy, request.method(), monitorV2, rbacRoles)

        return RestChannelConsumer { channel ->
            client.execute(IndexMonitorV2Action.INSTANCE, indexMonitorV2Request, RestToXContentListener(channel))
        }
    }
}
