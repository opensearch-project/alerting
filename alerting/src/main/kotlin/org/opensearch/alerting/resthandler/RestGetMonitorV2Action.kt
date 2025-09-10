package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.actionv2.GetMonitorV2Action
import org.opensearch.alerting.actionv2.GetMonitorV2Request
import org.opensearch.alerting.util.context
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.RestRequest.Method.HEAD
import org.opensearch.rest.action.RestActions
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.transport.client.node.NodeClient

private val log = LogManager.getLogger(RestGetMonitorV2Action::class.java)

class RestGetMonitorV2Action : BaseRestHandler() {

    override fun getName(): String {
        return "get_monitor_v2_action"
    }

    override fun routes(): List<Route> {
        return mutableListOf(
            Route(
                GET,
                "${AlertingPlugin.MONITOR_V2_BASE_URI}/{monitorV2Id}"
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_V2_BASE_URI}/{monitorV2Id}")

        val monitorV2Id = request.param("monitorV2Id")
        if (monitorV2Id == null || monitorV2Id.isEmpty()) {
            throw IllegalArgumentException("No MonitorV2 ID provided")
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
