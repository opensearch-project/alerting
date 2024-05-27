package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.client.node.NodeClient
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.DeleteNoteRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.DELETE
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

private val log: Logger = LogManager.getLogger(RestDeleteMonitorAction::class.java)

class RestDeleteNoteAction : BaseRestHandler() {

    override fun getName(): String {
        return "delete_note_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            ReplacedRoute(
                DELETE,
                "${AlertingPlugin.MONITOR_BASE_URI}/alerts/notes/{noteID}",
                DELETE,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/alerts/notes/{noteID}"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/alerts/notes/{noteID}")

        val noteId = request.param("noteID")
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/$noteId")

        val deleteMonitorRequest = DeleteNoteRequest(noteId)

        return RestChannelConsumer { channel ->
            client.execute(AlertingActions.DELETE_NOTES_ACTION_TYPE, deleteMonitorRequest, RestToXContentListener(channel))
        }
    }
}
