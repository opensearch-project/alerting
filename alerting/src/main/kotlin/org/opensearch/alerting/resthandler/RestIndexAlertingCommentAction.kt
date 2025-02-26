/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.util.IF_PRIMARY_TERM
import org.opensearch.alerting.util.IF_SEQ_NO
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.IndexCommentRequest
import org.opensearch.commons.alerting.action.IndexCommentResponse
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Comment
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestResponse
import org.opensearch.rest.action.RestResponseListener
import org.opensearch.transport.client.node.NodeClient
import java.io.IOException

private val log = LogManager.getLogger(RestIndexMonitorAction::class.java)

/**
 * Rest handlers to create and update alerting comments.
 */
class RestIndexAlertingCommentAction : BaseRestHandler() {

    override fun getName(): String {
        return "index_alerting_comment_action"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(
                RestRequest.Method.POST,
                "${AlertingPlugin.COMMENTS_BASE_URI}/{id}"
            ),
            Route(
                RestRequest.Method.PUT,
                "${AlertingPlugin.COMMENTS_BASE_URI}/{id}"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.info("${request.method()} ${AlertingPlugin.COMMENTS_BASE_URI}")

        val id = request.param(
            "id",
            if (request.method() == RestRequest.Method.POST) Alert.NO_ID else Comment.NO_ID
        )
        if (request.method() == RestRequest.Method.POST && Alert.NO_ID == id) {
            throw AlertingException.wrap(IllegalArgumentException("Missing alert ID"))
        } else if (request.method() == RestRequest.Method.PUT && Comment.NO_ID == id) {
            throw AlertingException.wrap(IllegalArgumentException("Missing comment ID"))
        }

        val alertId = if (request.method() == RestRequest.Method.POST) id else Alert.NO_ID
        val commentId = if (request.method() == RestRequest.Method.PUT) id else Comment.NO_ID

        val content = request.contentParser().map()[Comment.COMMENT_CONTENT_FIELD] as String?
        if (content.isNullOrEmpty()) {
            throw AlertingException.wrap(IllegalArgumentException("Missing comment content"))
        }
        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)

        val indexCommentRequest = IndexCommentRequest(
            alertId,
            "alert",
            commentId,
            seqNo,
            primaryTerm,
            request.method(),
            content
        )

        return RestChannelConsumer { channel ->
            client.execute(AlertingActions.INDEX_COMMENT_ACTION_TYPE, indexCommentRequest, indexCommentResponse(channel, request.method()))
        }
    }

    private fun indexCommentResponse(channel: RestChannel, restMethod: RestRequest.Method):
        RestResponseListener<IndexCommentResponse> {
        return object : RestResponseListener<IndexCommentResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: IndexCommentResponse): RestResponse {
                var returnStatus = RestStatus.CREATED
                if (restMethod == RestRequest.Method.PUT)
                    returnStatus = RestStatus.OK

                val restResponse = BytesRestResponse(returnStatus, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                if (returnStatus == RestStatus.CREATED) {
                    val location = "${AlertingPlugin.COMMENTS_BASE_URI}/${response.id}"
                    restResponse.addHeader("Location", location)
                }
                return restResponse
            }
        }
    }
}
