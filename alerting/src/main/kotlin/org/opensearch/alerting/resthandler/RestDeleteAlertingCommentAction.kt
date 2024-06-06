/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.client.node.NodeClient
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.DeleteCommentRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

private val log: Logger = LogManager.getLogger(RestDeleteMonitorAction::class.java)

/**
 * Rest handlers to create and update comments.
 */
class RestDeleteAlertingCommentAction : BaseRestHandler() {

    override fun getName(): String {
        return "delete_alerting_comment_action"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(
                RestRequest.Method.DELETE,
                "${AlertingPlugin.COMMENTS_BASE_URI}/{commentID}"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.COMMENTS_BASE_URI}/{commentID}")
        val commentId = request.param("commentID")
        val deleteMonitorRequest = DeleteCommentRequest(commentId)
        return RestChannelConsumer { channel ->
            client.execute(AlertingActions.DELETE_COMMENT_ACTION_TYPE, deleteMonitorRequest, RestToXContentListener(channel))
        }
    }
}
