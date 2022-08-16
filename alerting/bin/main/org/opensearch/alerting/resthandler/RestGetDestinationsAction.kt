/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.GetDestinationsAction
import org.opensearch.alerting.action.GetDestinationsRequest
import org.opensearch.alerting.model.Table
import org.opensearch.alerting.util.context
import org.opensearch.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestActions
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.search.fetch.subphase.FetchSourceContext

/**
 * This class consists of the REST handler to retrieve destinations .
 */
class RestGetDestinationsAction : BaseRestHandler() {

    private val log = LogManager.getLogger(RestGetDestinationsAction::class.java)

    override fun getName(): String {
        return "get_destinations_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            // Get a specific destination
            ReplacedRoute(
                RestRequest.Method.GET,
                "${AlertingPlugin.DESTINATION_BASE_URI}/{destinationID}",
                RestRequest.Method.GET,
                "${AlertingPlugin.LEGACY_OPENDISTRO_DESTINATION_BASE_URI}/{destinationID}"
            ),
            ReplacedRoute(
                RestRequest.Method.GET,
                AlertingPlugin.DESTINATION_BASE_URI,
                RestRequest.Method.GET,
                AlertingPlugin.LEGACY_OPENDISTRO_DESTINATION_BASE_URI
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${request.path()}")

        val destinationId: String? = request.param("destinationID")

        var srcContext = context(request)
        if (request.method() == RestRequest.Method.HEAD) {
            srcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        }

        val sortString = request.param("sortString", "destination.name.keyword")
        val sortOrder = request.param("sortOrder", "asc")
        val missing: String? = request.param("missing")
        val size = request.paramAsInt("size", 20)
        val startIndex = request.paramAsInt("startIndex", 0)
        val searchString = request.param("searchString", "")
        val destinationType = request.param("destinationType", "ALL")

        val table = Table(
            sortOrder,
            sortString,
            missing,
            size,
            startIndex,
            searchString
        )

        val getDestinationsRequest = GetDestinationsRequest(
            destinationId,
            RestActions.parseVersion(request),
            srcContext,
            table,
            destinationType
        )
        return RestChannelConsumer {
                channel ->
            client.execute(GetDestinationsAction.INSTANCE, getDestinationsRequest, RestToXContentListener(channel))
        }
    }
}
