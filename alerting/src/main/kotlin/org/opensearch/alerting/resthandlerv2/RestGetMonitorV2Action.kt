/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandlerv2

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.actionv2.GetMonitorV2Action
import org.opensearch.alerting.actionv2.GetMonitorV2Request
import org.opensearch.alerting.modelv2.MonitorV2.Companion.UUID_LENGTH
import org.opensearch.alerting.util.context
import org.opensearch.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.RestRequest.Method.HEAD
import org.opensearch.rest.action.RestActions
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.search.fetch.subphase.FetchSourceContext

private val log = LogManager.getLogger(RestGetMonitorV2Action::class.java)

/**
 * This class consists of the REST handler to retrieve a V2 monitor by its ID.
 *
 * @opensearch.experimental
 */
class RestGetMonitorV2Action : BaseRestHandler() {

    override fun getName(): String {
        return "get_monitor_v2_action"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(
                GET,
                "${AlertingPlugin.MONITOR_V2_BASE_URI}/{monitor_id}"
            ),
            Route(
                HEAD,
                "${AlertingPlugin.MONITOR_V2_BASE_URI}/{monitor_id}"
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_V2_BASE_URI}/{monitor_id}")

        val monitorV2Id = request.param("monitor_id")
        if (monitorV2Id == null || monitorV2Id.isEmpty()) {
            throw IllegalArgumentException("No MonitorV2 ID provided")
        }

        if (monitorV2Id.length != UUID_LENGTH) {
            throw IllegalArgumentException("MonitorV2 ID provided does not have correct length")
        }

        var srcContext = context(request)
        if (request.method() == HEAD) {
            srcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        }

        val getMonitorV2Request = GetMonitorV2Request(monitorV2Id, RestActions.parseVersion(request), srcContext)
        return RestChannelConsumer {
                channel ->
            client.execute(GetMonitorV2Action.INSTANCE, getMonitorV2Request, RestToXContentListener(channel))
        }
    }
}
