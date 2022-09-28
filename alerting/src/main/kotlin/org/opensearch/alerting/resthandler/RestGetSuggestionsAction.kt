/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.GetSuggestionsAction
import org.opensearch.alerting.action.GetSuggestionsRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener

private val log = LogManager.getLogger(RestGetSuggestionsAction::class.java)

class RestGetSuggestionsAction : BaseRestHandler() {

    override fun getName(): String = "get_suggestions_action"

    override fun routes(): List<Route> {
        return listOf(
            Route(POST, AlertingPlugin.SUGGESTIONS_BASE_URI) // inline object with specific format is required
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.SUGGESTIONS_BASE_URI}")

        return RestChannelConsumer { channel ->
            val getSuggestionsRequest = GetSuggestionsRequest.parse(request)
            client.execute(GetSuggestionsAction.INSTANCE, getSuggestionsRequest, RestToXContentListener(channel))
        }
    }
}
