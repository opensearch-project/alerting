/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.GetRemoteIndexesAction
import org.opensearch.alerting.action.GetRemoteIndexesRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.core.common.Strings
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener

private val log = LogManager.getLogger(RestGetRemoteIndexesAction::class.java)

class RestGetRemoteIndexesAction : BaseRestHandler() {
    val ROUTE = "${AlertingPlugin.REMOTE_BASE_URI}/indexes"

    override fun getName(): String {
        return "get_remote_indexes_action"
    }

    override fun routes(): List<RestHandler.Route> {
        return mutableListOf(
            RestHandler.Route(RestRequest.Method.GET, ROUTE)
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} $ROUTE")
        val indexes = Strings.splitStringByCommaToArray(request.param(GetRemoteIndexesRequest.INDEXES_FIELD, ""))
        val includeMappings = request.paramAsBoolean(GetRemoteIndexesRequest.INCLUDE_MAPPINGS_FIELD, false)
        return RestChannelConsumer {
                channel ->
            client.execute(
                GetRemoteIndexesAction.INSTANCE,
                GetRemoteIndexesRequest(
                    indexes = indexes.toList(),
                    includeMappings = includeMappings
                ),
                RestToXContentListener(channel)
            )
        }
    }
}
