/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.IndexEmailAccountAction
import org.opensearch.alerting.action.IndexEmailAccountRequest
import org.opensearch.alerting.action.IndexEmailAccountResponse
import org.opensearch.alerting.model.destination.email.EmailAccount
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

private val log = LogManager.getLogger(RestIndexEmailAccountAction::class.java)

/**
 * Rest handlers to create and update EmailAccount
 */
class RestIndexEmailAccountAction : BaseRestHandler() {

    override fun getName(): String {
        return "index_email_account_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            ReplacedRoute(
                RestRequest.Method.POST,
                AlertingPlugin.EMAIL_ACCOUNT_BASE_URI,
                RestRequest.Method.POST,
                AlertingPlugin.LEGACY_OPENDISTRO_EMAIL_ACCOUNT_BASE_URI
            ),
            ReplacedRoute(
                RestRequest.Method.PUT,
                "${AlertingPlugin.EMAIL_ACCOUNT_BASE_URI}/{emailAccountID}",
                RestRequest.Method.PUT,
                "${AlertingPlugin.LEGACY_OPENDISTRO_EMAIL_ACCOUNT_BASE_URI}/{emailAccountID}"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("emailAccountID", EmailAccount.NO_ID)
        if (request.method() == RestRequest.Method.PUT && EmailAccount.NO_ID == id) {
            throw IllegalArgumentException("Missing email account ID")
        }

        // Validate request by parsing JSON to EmailAccount
        val xcp = request.contentParser()
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
        val emailAccount = EmailAccount.parse(xcp, id)
        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }

        val indexEmailAccountRequest = IndexEmailAccountRequest(id, seqNo, primaryTerm, refreshPolicy, request.method(), emailAccount)

        return RestChannelConsumer { channel ->
            client.execute(IndexEmailAccountAction.INSTANCE, indexEmailAccountRequest, indexEmailAccountResponse(channel, request.method()))
        }
    }

    private fun indexEmailAccountResponse(channel: RestChannel, restMethod: RestRequest.Method):
        RestResponseListener<IndexEmailAccountResponse> {
        return object : RestResponseListener<IndexEmailAccountResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: IndexEmailAccountResponse): RestResponse {
                var returnStatus = RestStatus.CREATED
                if (restMethod == RestRequest.Method.PUT)
                    returnStatus = RestStatus.OK

                val restResponse = BytesRestResponse(returnStatus, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                if (returnStatus == RestStatus.CREATED) {
                    val location = "${AlertingPlugin.EMAIL_ACCOUNT_BASE_URI}/${response.id}"
                    restResponse.addHeader("Location", location)
                }

                return restResponse
            }
        }
    }
}
