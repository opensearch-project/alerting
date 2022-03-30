/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.GetFindingsSearchAction
import org.opensearch.alerting.action.GetFindingsSearchRequest
import org.opensearch.alerting.model.Table
import org.opensearch.alerting.util.context
import org.opensearch.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestActions
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.search.fetch.subphase.FetchSourceContext

/**
 * This class consists of the REST handler to search findings .
 */
class RestGetFindingsSearchAction : BaseRestHandler() {

    private val log = LogManager.getLogger(RestGetFindingsSearchAction::class.java)

    override fun getName(): String {
        return "get_findings_search_action"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(POST, "${AlertingPlugin.FINDING_BASE_URI}/_search"),
            Route(GET, "${AlertingPlugin.FINDING_BASE_URI}/_search")
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.info("Entering RestGetFindingsSearchAction.kt.")
        log.info("${request.method()} ${request.path()}")

        val findingID: String? = request.param("findingID")

        var srcContext = context(request)
        if (request.method() == RestRequest.Method.HEAD) {
            srcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        }

        val sortString = request.param("sortString", "id.keyword")
        val sortOrder = request.param("sortOrder", "asc")
        val missing: String? = request.param("missing")
        val size = request.paramAsInt("size", 20)
        val startIndex = request.paramAsInt("startIndex", 0)
        val searchString = request.param("searchString", "")

        val table = Table(
            sortOrder,
            sortString,
            missing,
            size,
            startIndex,
            searchString
        )

        val getFindingsSearchRequest = GetFindingsSearchRequest(
            findingID,
            RestActions.parseVersion(request),
            srcContext,
            table
        )
        return RestChannelConsumer {
            channel ->
            client.execute(GetFindingsSearchAction.INSTANCE, getFindingsSearchRequest, RestToXContentListener(channel))
        }
    }
}
