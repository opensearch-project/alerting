/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.notes.NotesIndices.Companion.ALL_NOTES_INDEX_PATTERN
import org.opensearch.alerting.util.context
import org.opensearch.client.node.NodeClient
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.SearchNoteRequest
import org.opensearch.commons.alerting.model.Note
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestResponse
import org.opensearch.rest.action.RestResponseListener
import org.opensearch.search.builder.SearchSourceBuilder
import java.io.IOException

private val log = LogManager.getLogger(RestIndexMonitorAction::class.java)

/**
 * Rest handler to search notes.
 */
class RestSearchNoteAction() : BaseRestHandler() {

    override fun getName(): String {
        return "search_notes_action"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(
                RestRequest.Method.GET,
                "${AlertingPlugin.MONITOR_BASE_URI}/alerts/notes/_search"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/alerts/notes/_search")

        val searchSourceBuilder = SearchSourceBuilder()
        searchSourceBuilder.parseXContent(request.contentOrSourceParamParser())
        searchSourceBuilder.fetchSource(context(request))

        val searchRequest = SearchRequest()
            .source(searchSourceBuilder)
            .indices(ALL_NOTES_INDEX_PATTERN)

        val searchNoteRequest = SearchNoteRequest(searchRequest)
        return RestChannelConsumer { channel ->
            client.execute(AlertingActions.SEARCH_NOTES_ACTION_TYPE, searchNoteRequest, searchNoteResponse(channel))
        }
    }

    private fun searchNoteResponse(channel: RestChannel): RestResponseListener<SearchResponse> {
        return object : RestResponseListener<SearchResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: SearchResponse): RestResponse {
                if (response.isTimedOut) {
                    return BytesRestResponse(RestStatus.REQUEST_TIMEOUT, response.toString())
                }

                // Swallow exception and return response as is
                try {
                    for (hit in response.hits) {
                        XContentType.JSON.xContent().createParser(
                            channel.request().xContentRegistry,
                            LoggingDeprecationHandler.INSTANCE,
                            hit.sourceAsString
                        ).use { hitsParser ->
                            hitsParser.nextToken()
                            val note = Note.parse(hitsParser, hit.id)
                            val xcb = note.toXContent(jsonBuilder(), EMPTY_PARAMS)
                            hit.sourceRef(BytesReference.bytes(xcb))
                        }
                    }
                } catch (e: Exception) {
                    log.info("The note parsing failed. Will return response as is.")
                }
                return BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), EMPTY_PARAMS))
            }
        }
    }
}
