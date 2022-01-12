/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.SearchEmailGroupAction
import org.opensearch.alerting.core.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.alerting.model.destination.email.EmailGroup
import org.opensearch.alerting.util.context
import org.opensearch.client.node.NodeClient
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestResponse
import org.opensearch.rest.RestStatus
import org.opensearch.rest.action.RestResponseListener
import org.opensearch.search.builder.SearchSourceBuilder
import java.io.IOException

/**
 * Rest handlers to search for EmailGroup
 */
class RestSearchEmailGroupAction : BaseRestHandler() {

    override fun getName(): String {
        return "search_email_group_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            ReplacedRoute(
                RestRequest.Method.POST,
                "${AlertingPlugin.EMAIL_GROUP_BASE_URI}/_search",
                RestRequest.Method.POST,
                "${AlertingPlugin.LEGACY_OPENDISTRO_EMAIL_GROUP_BASE_URI}/_search"
            ),
            ReplacedRoute(
                RestRequest.Method.GET,
                "${AlertingPlugin.EMAIL_GROUP_BASE_URI}/_search",
                RestRequest.Method.GET,
                "${AlertingPlugin.LEGACY_OPENDISTRO_EMAIL_GROUP_BASE_URI}/_search"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val searchSourceBuilder = SearchSourceBuilder()
        searchSourceBuilder.parseXContent(request.contentOrSourceParamParser())
        searchSourceBuilder.fetchSource(context(request))

        // An exists query is added on top of the user's query to ensure that only documents of email_group type
        // are searched
        searchSourceBuilder.query(
            QueryBuilders.boolQuery().must(searchSourceBuilder.query())
                .filter(QueryBuilders.existsQuery(EmailGroup.EMAIL_GROUP_TYPE))
        )
            .seqNoAndPrimaryTerm(true)
        val searchRequest = SearchRequest()
            .source(searchSourceBuilder)
            .indices(SCHEDULED_JOBS_INDEX)
        return RestChannelConsumer { channel ->
            client.execute(SearchEmailGroupAction.INSTANCE, searchRequest, searchEmailGroupResponse(channel))
        }
    }

    private fun searchEmailGroupResponse(channel: RestChannel): RestResponseListener<SearchResponse> {
        return object : RestResponseListener<SearchResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: SearchResponse): RestResponse {
                if (response.isTimedOut) {
                    return BytesRestResponse(RestStatus.REQUEST_TIMEOUT, response.toString())
                }

                for (hit in response.hits) {
                    XContentType.JSON.xContent().createParser(
                        channel.request().xContentRegistry,
                        LoggingDeprecationHandler.INSTANCE, hit.sourceAsString
                    ).use { hitsParser ->
                        val emailGroup = EmailGroup.parseWithType(hitsParser, hit.id, hit.version)
                        val xcb = emailGroup.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
                        hit.sourceRef(BytesReference.bytes(xcb))
                    }
                }
                return BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
            }
        }
    }
}
