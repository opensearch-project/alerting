/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.IF_PRIMARY_TERM
import org.opensearch.alerting.util.IF_SEQ_NO
import org.opensearch.client.node.NodeClient
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.IndexNoteRequest
import org.opensearch.commons.alerting.action.IndexNoteResponse
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Note
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.RestRequest.Method.PUT
import org.opensearch.rest.RestResponse
import org.opensearch.rest.action.RestResponseListener
import java.io.IOException

private val log = LogManager.getLogger(RestIndexMonitorAction::class.java)

/**
 * Rest handlers to create and update notes.
 */
class RestIndexNoteAction : BaseRestHandler() {

    override fun getName(): String {
        return "index_note_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            ReplacedRoute(
                POST,
                "${AlertingPlugin.MONITOR_BASE_URI}/alerts/{alertID}/notes",
                POST,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/alerts/{alertID}/notes",
            ),
            ReplacedRoute(
                PUT,
                "${AlertingPlugin.MONITOR_BASE_URI}/alerts/notes/{noteID}",
                PUT,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/alerts/notes/{noteID}",
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/alerts/notes")

        val alertId = request.param("alertID", Alert.NO_ID)
        val noteId = request.param("noteID", Note.NO_ID)
        if (request.method() == POST && Alert.NO_ID == alertId) {
            throw AlertingException.wrap(IllegalArgumentException("Missing alert ID"))
        } else if (request.method() == PUT && Note.NO_ID == noteId) {
            throw AlertingException.wrap(IllegalArgumentException("Missing note ID"))
        }

        // TODO: validation for empty string?
        val content = request.contentParser().map()["content"] as String?
            ?: throw AlertingException.wrap(IllegalArgumentException("Missing note content"))
        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)

        val indexNoteRequest = IndexNoteRequest(alertId, noteId, seqNo, primaryTerm, request.method(), content)

        return RestChannelConsumer { channel ->
            client.execute(AlertingActions.INDEX_NOTE_ACTION_TYPE, indexNoteRequest, indexNoteResponse(channel, request.method()))
        }
    }
}
private fun indexNoteResponse(channel: RestChannel, restMethod: RestRequest.Method):
    RestResponseListener<IndexNoteResponse> {
    return object : RestResponseListener<IndexNoteResponse>(channel) {
        @Throws(Exception::class)
        override fun buildResponse(response: IndexNoteResponse): RestResponse {
            var returnStatus = RestStatus.CREATED
            if (restMethod == RestRequest.Method.PUT)
                returnStatus = RestStatus.OK

            val restResponse = BytesRestResponse(returnStatus, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
            if (returnStatus == RestStatus.CREATED) {
                val location = "${AlertingPlugin.MONITOR_BASE_URI}/alerts/notes/${response.id}"
                restResponse.addHeader("Location", location)
            }
            return restResponse
        }
    }
}
