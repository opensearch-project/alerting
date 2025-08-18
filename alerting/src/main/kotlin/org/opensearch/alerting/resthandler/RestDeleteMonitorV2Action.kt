package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.util.REFRESH
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.DeleteMonitorV2Request
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.DELETE
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient
import java.io.IOException

private val log: Logger = LogManager.getLogger(RestDeleteMonitorAction::class.java)

class RestDeleteMonitorV2Action : BaseRestHandler() {

    override fun getName(): String {
        return "delete_monitor_v2_action"
    }

    override fun routes(): List<Route> {
        return mutableListOf(
            Route(
                DELETE,
                "${AlertingPlugin.MONITOR_V2_BASE_URI}/{monitorId}"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val monitorId = request.param("monitorId")
        log.info("${request.method()} ${AlertingPlugin.MONITOR_V2_BASE_URI}/$monitorId")

        val refreshPolicy = RefreshPolicy.parse(request.param(REFRESH, RefreshPolicy.IMMEDIATE.value))
        val deleteMonitorV2Request = DeleteMonitorV2Request(monitorId, refreshPolicy)

        return RestChannelConsumer { channel ->
            client.execute(AlertingActions.DELETE_MONITOR_V2_ACTION_TYPE, deleteMonitorV2Request, RestToXContentListener(channel))
        }
    }
}
