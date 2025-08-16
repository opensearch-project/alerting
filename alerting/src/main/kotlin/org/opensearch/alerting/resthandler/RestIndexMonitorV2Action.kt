package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.util.IF_PRIMARY_TERM
import org.opensearch.alerting.util.IF_SEQ_NO
import org.opensearch.alerting.util.REFRESH
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.IndexMonitorV2Request
import org.opensearch.commons.alerting.model.MonitorV2
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
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
            // TODO: support UpdateMonitor
//            Route(
//                PUT,
//                "${AlertingPlugin.PPL_MONITOR_BASE_URI}/{monitorID}"
//            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${request.path()}")
        log.info("preparing rest request")

        val xcp = request.contentParser()
        ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)

        log.info("initial ensure")

        val monitorV2: MonitorV2
        try {
            monitorV2 = MonitorV2.parse(xcp)
        } catch (e: Exception) {
            throw AlertingException.wrap(e)
        }

        log.info("monitorv2 retrieved")

        val id = request.param("monitorID", MonitorV2.NO_ID)
        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }

        val indexMonitorV2Request = IndexMonitorV2Request(seqNo, primaryTerm, refreshPolicy, monitorV2)

        return RestChannelConsumer { channel ->
            client.execute(AlertingActions.INDEX_MONITOR_V2_ACTION_TYPE, indexMonitorV2Request, RestToXContentListener(channel))
        }
    }
}
