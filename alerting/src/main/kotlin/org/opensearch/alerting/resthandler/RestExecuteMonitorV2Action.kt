package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.ExecuteMonitorV2Action
import org.opensearch.alerting.action.ExecuteMonitorV2Request
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.MonitorV2
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.xcontent.XContentParser.Token.START_OBJECT
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient
import java.time.Instant

private val log = LogManager.getLogger(RestExecuteMonitorV2Action::class.java)

class RestExecuteMonitorV2Action : BaseRestHandler() {

    override fun getName(): String = "execute_monitor_v2_action"

    override fun routes(): List<Route> {
        return listOf(
            Route(
                POST,
                "${AlertingPlugin.MONITOR_V2_BASE_URI}/{monitorId}/_execute"
            ),
            Route(
                POST,
                "${AlertingPlugin.MONITOR_V2_BASE_URI}/_execute"
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_V2_BASE_URI}/_execute")

        return RestChannelConsumer { channel ->
            val dryrun = request.paramAsBoolean("dryrun", false)
            val requestEnd = request.paramAsTime("period_end", TimeValue(Instant.now().toEpochMilli()))

            if (request.hasParam("monitorId")) {
                val monitorId = request.param("monitorId")
                val execMonitorV2Request = ExecuteMonitorV2Request(dryrun, monitorId, null, null, requestEnd)
                client.execute(ExecuteMonitorV2Action.INSTANCE, execMonitorV2Request, RestToXContentListener(channel))
            } else {
                val xcp = request.contentParser()
                ensureExpectedToken(START_OBJECT, xcp.nextToken(), xcp)

                val monitorV2: MonitorV2
                try {
                    monitorV2 = MonitorV2.parse(xcp)
                } catch (e: Exception) {
                    throw AlertingException.wrap(e)
                }

                val execMonitorV2Request = ExecuteMonitorV2Request(dryrun, null, monitorV2, null, requestEnd)
                client.execute(ExecuteMonitorV2Action.INSTANCE, execMonitorV2Request, RestToXContentListener(channel))
            }
        }
    }

    override fun responseParams(): Set<String> {
        return setOf("dryrun", "period_end", "monitorId")
    }
}
