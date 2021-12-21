/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.ExecuteMonitorAction
import org.opensearch.alerting.action.ExecuteMonitorRequest
import org.opensearch.alerting.model.Monitor
import org.opensearch.client.node.NodeClient
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentParser.Token.START_OBJECT
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener
import java.time.Instant

private val log = LogManager.getLogger(RestExecuteMonitorAction::class.java)

class RestExecuteMonitorAction : BaseRestHandler() {

    override fun getName(): String = "execute_monitor_action"

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            ReplacedRoute(
                POST,
                "${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}/_execute",
                POST,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/{monitorID}/_execute"
            ),
            ReplacedRoute(
                POST,
                "${AlertingPlugin.MONITOR_BASE_URI}/_execute",
                POST,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/_execute"
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/_execute")

        return RestChannelConsumer { channel ->
            val dryrun = request.paramAsBoolean("dryrun", false)
            val requestEnd = request.paramAsTime("period_end", TimeValue(Instant.now().toEpochMilli()))

            if (request.hasParam("monitorID")) {
                val monitorId = request.param("monitorID")
                val execMonitorRequest = ExecuteMonitorRequest(dryrun, requestEnd, monitorId, null)
                client.execute(ExecuteMonitorAction.INSTANCE, execMonitorRequest, RestToXContentListener(channel))
            } else {
                val xcp = request.contentParser()
                ensureExpectedToken(START_OBJECT, xcp.nextToken(), xcp)
                val monitor = Monitor.parse(xcp, Monitor.NO_ID, Monitor.NO_VERSION)
                val execMonitorRequest = ExecuteMonitorRequest(dryrun, requestEnd, null, monitor)
                client.execute(ExecuteMonitorAction.INSTANCE, execMonitorRequest, RestToXContentListener(channel))
            }
        }
    }

    override fun responseParams(): Set<String> {
        return setOf("dryrun", "period_end", "monitorID")
    }
}
