/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.resthandler

import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.IF_PRIMARY_TERM
import org.opensearch.alerting.util.IF_SEQ_NO
import org.opensearch.alerting.util.REFRESH
import org.opensearch.client.node.NodeClient
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.IndexWorkflowRequest
import org.opensearch.commons.alerting.action.IndexWorkflowResponse
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestResponse
import org.opensearch.rest.RestStatus
import org.opensearch.rest.action.RestResponseListener
import java.io.IOException
import java.time.Instant

/**
 * Rest handlers to create and update workflows.
 */
class RestIndexWorkflowAction : BaseRestHandler() {

    override fun getName(): String {
        return "index_workflow_action"
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(RestRequest.Method.POST, AlertingPlugin.WORKFLOW_BASE_URI),
            RestHandler.Route(
                RestRequest.Method.PUT,
                "${AlertingPlugin.WORKFLOW_BASE_URI}/{workflowID}"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("workflowID", Workflow.NO_ID)
        if (request.method() == RestRequest.Method.PUT && Workflow.NO_ID == id) {
            throw AlertingException.wrap(IllegalArgumentException("Missing workflow ID"))
        }

        // Validate request by parsing JSON to Monitor
        val xcp = request.contentParser()
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
        val workflow = Workflow.parse(xcp, id).copy(lastUpdateTime = Instant.now())
        val rbacRoles = request.contentParser().map()["rbac_roles"] as List<String>?

        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }
        val workflowRequest =
            IndexWorkflowRequest(id, seqNo, primaryTerm, refreshPolicy, request.method(), workflow, rbacRoles)

        return RestChannelConsumer { channel ->
            client.execute(AlertingActions.INDEX_WORKFLOW_ACTION_TYPE, workflowRequest, indexMonitorResponse(channel, request.method()))
        }
    }

    private fun indexMonitorResponse(channel: RestChannel, restMethod: RestRequest.Method): RestResponseListener<IndexWorkflowResponse> {
        return object : RestResponseListener<IndexWorkflowResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: IndexWorkflowResponse): RestResponse {
                var returnStatus = RestStatus.CREATED
                if (restMethod == RestRequest.Method.PUT)
                    returnStatus = RestStatus.OK

                val restResponse =
                    BytesRestResponse(returnStatus, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                if (returnStatus == RestStatus.CREATED) {
                    val location = "${AlertingPlugin.WORKFLOW_BASE_URI}/${response.id}"
                    restResponse.addHeader("Location", location)
                }
                return restResponse
            }
        }
    }
}
