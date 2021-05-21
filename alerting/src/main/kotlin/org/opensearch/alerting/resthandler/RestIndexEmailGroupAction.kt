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

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.IndexEmailGroupAction
import org.opensearch.alerting.action.IndexEmailGroupRequest
import org.opensearch.alerting.action.IndexEmailGroupResponse
import org.opensearch.alerting.model.destination.email.EmailGroup
import org.opensearch.alerting.util.IF_PRIMARY_TERM
import org.opensearch.alerting.util.IF_SEQ_NO
import org.opensearch.alerting.util.REFRESH
import org.opensearch.client.node.NodeClient
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestResponse
import org.opensearch.rest.RestStatus
import org.opensearch.rest.action.RestResponseListener
import java.io.IOException

private val log = LogManager.getLogger(RestIndexEmailGroupAction::class.java)

/**
 * Rest handlers to create and update EmailGroup.
 */
class RestIndexEmailGroupAction : BaseRestHandler() {

    override fun getName(): String {
        return "index_email_group_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            ReplacedRoute(
                RestRequest.Method.POST,
                AlertingPlugin.EMAIL_GROUP_BASE_URI,
                RestRequest.Method.POST,
                AlertingPlugin.LEGACY_OPENDISTRO_EMAIL_GROUP_BASE_URI
            ),
            ReplacedRoute(
                RestRequest.Method.PUT,
                "${AlertingPlugin.EMAIL_GROUP_BASE_URI}/{emailGroupID}",
                RestRequest.Method.PUT,
                "${AlertingPlugin.LEGACY_OPENDISTRO_EMAIL_GROUP_BASE_URI}/{emailGroupID}"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("emailGroupID", EmailGroup.NO_ID)
        if (request.method() == RestRequest.Method.PUT && EmailGroup.NO_ID == id) {
            throw IllegalArgumentException("Missing email group ID")
        }

        // Validate request by parsing JSON to EmailGroup
        val xcp = request.contentParser()
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
        val emailGroup = EmailGroup.parse(xcp, id)
        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }

        val indexEmailGroupRequest = IndexEmailGroupRequest(id, seqNo, primaryTerm, refreshPolicy, request.method(), emailGroup)

        return RestChannelConsumer { channel ->
            client.execute(IndexEmailGroupAction.INSTANCE, indexEmailGroupRequest, indexEmailGroupResponse(channel, request.method()))
        }
    }

    private fun indexEmailGroupResponse(channel: RestChannel, restMethod: RestRequest.Method):
        RestResponseListener<IndexEmailGroupResponse> {
        return object : RestResponseListener<IndexEmailGroupResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: IndexEmailGroupResponse): RestResponse {
                var returnStatus = RestStatus.CREATED
                if (restMethod == RestRequest.Method.PUT)
                    returnStatus = RestStatus.OK

                val restResponse = BytesRestResponse(returnStatus, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                if (returnStatus == RestStatus.CREATED) {
                    val location = "${AlertingPlugin.EMAIL_GROUP_BASE_URI}/${response.id}"
                    restResponse.addHeader("Location", location)
                }

                return restResponse
            }
        }
    }
}
