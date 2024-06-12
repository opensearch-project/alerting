/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.comments.CommentsIndices.Companion.ALL_COMMENTS_INDEX_PATTERN
import org.opensearch.alerting.util.context
import org.opensearch.client.node.NodeClient
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.SearchCommentRequest
import org.opensearch.commons.alerting.model.Comment
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
 * Rest handler to search alerting comments.
 */
class RestSearchAlertingCommentAction() : BaseRestHandler() {

    override fun getName(): String {
        return "search_alerting_comments_action"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(
                RestRequest.Method.GET,
                "${AlertingPlugin.COMMENTS_BASE_URI}/_search"
            ),
            Route(
                RestRequest.Method.POST,
                "${AlertingPlugin.COMMENTS_BASE_URI}/_search"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.info("${request.method()} ${AlertingPlugin.COMMENTS_BASE_URI}/_search")

        val searchSourceBuilder = SearchSourceBuilder()
        searchSourceBuilder.parseXContent(request.contentOrSourceParamParser())
        searchSourceBuilder.fetchSource(context(request))

        val searchRequest = SearchRequest()
            .source(searchSourceBuilder)
            .indices(ALL_COMMENTS_INDEX_PATTERN)

        val searchCommentRequest = SearchCommentRequest(searchRequest)
        return RestChannelConsumer { channel ->
            client.execute(AlertingActions.SEARCH_COMMENTS_ACTION_TYPE, searchCommentRequest, searchCommentResponse(channel))
        }
    }

    private fun searchCommentResponse(channel: RestChannel): RestResponseListener<SearchResponse> {
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
                            val comment = Comment.parse(hitsParser, hit.id)
                            val xcb = comment.toXContent(jsonBuilder(), EMPTY_PARAMS)
                            hit.sourceRef(BytesReference.bytes(xcb))
                        }
                    }
                } catch (e: Exception) {
                    log.error("The comment parsing failed. Will return response as is.")
                }
                return BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), EMPTY_PARAMS))
            }
        }
    }
}