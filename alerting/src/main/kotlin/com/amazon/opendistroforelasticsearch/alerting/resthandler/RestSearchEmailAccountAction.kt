/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.alerting.resthandler

import com.amazon.opendistroforelasticsearch.alerting.AlertingPlugin
import com.amazon.opendistroforelasticsearch.alerting.action.SearchEmailAccountAction
import com.amazon.opendistroforelasticsearch.alerting.core.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import com.amazon.opendistroforelasticsearch.alerting.model.destination.email.EmailAccount
import com.amazon.opendistroforelasticsearch.alerting.util.context
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.client.node.NodeClient
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.ToXContent.EMPTY_PARAMS
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestResponse
import org.opensearch.rest.RestStatus
import org.opensearch.rest.action.RestResponseListener
import org.opensearch.search.builder.SearchSourceBuilder
import java.io.IOException

/**
 * Rest handlers to search for EmailAccount
 */
class RestSearchEmailAccountAction : BaseRestHandler() {

    override fun getName(): String {
        return "search_email_account_action"
    }

    override fun routes(): List<Route> {
        return listOf(
                Route(RestRequest.Method.POST, "${AlertingPlugin.EMAIL_ACCOUNT_BASE_URI}/_search"),
                Route(RestRequest.Method.GET, "${AlertingPlugin.EMAIL_ACCOUNT_BASE_URI}/_search")
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val searchSourceBuilder = SearchSourceBuilder()
        searchSourceBuilder.parseXContent(request.contentOrSourceParamParser())
        searchSourceBuilder.fetchSource(context(request))

        // An exists query is added on top of the user's query to ensure that only documents of email_account type
        // are searched
        searchSourceBuilder.query(QueryBuilders.boolQuery().must(searchSourceBuilder.query())
                .filter(QueryBuilders.existsQuery(EmailAccount.EMAIL_ACCOUNT_TYPE)))
                .seqNoAndPrimaryTerm(true)
        val searchRequest = SearchRequest()
                .source(searchSourceBuilder)
                .indices(SCHEDULED_JOBS_INDEX)
        return RestChannelConsumer { channel ->
            client.execute(SearchEmailAccountAction.INSTANCE, searchRequest, searchEmailAccountResponse(channel))
        }
    }

    private fun searchEmailAccountResponse(channel: RestChannel): RestResponseListener<SearchResponse> {
        return object : RestResponseListener<SearchResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: SearchResponse): RestResponse {
                if (response.isTimedOut) {
                    return BytesRestResponse(RestStatus.REQUEST_TIMEOUT, response.toString())
                }

                for (hit in response.hits) {
                    XContentType.JSON.xContent().createParser(channel.request().xContentRegistry,
                            LoggingDeprecationHandler.INSTANCE, hit.sourceAsString).use { hitsParser ->
                                val emailAccount = EmailAccount.parseWithType(hitsParser, hit.id, hit.version)
                                val xcb = emailAccount.toXContent(jsonBuilder(), EMPTY_PARAMS)
                                hit.sourceRef(BytesReference.bytes(xcb))
                            }
                }
                return BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), EMPTY_PARAMS))
            }
        }
    }
}
