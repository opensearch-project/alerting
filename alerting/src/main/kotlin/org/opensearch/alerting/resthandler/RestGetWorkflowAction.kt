/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.util.context
import org.opensearch.client.node.NodeClient
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.GetWorkflowRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.search.fetch.subphase.FetchSourceContext

/**
 * This class consists of the REST handler to retrieve a workflow .
 */
class RestGetWorkflowAction : BaseRestHandler() {

    private val log = LogManager.getLogger(javaClass)

    override fun getName(): String {
        return "get_workflow_action"
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(
                RestRequest.Method.GET,
                "${AlertingPlugin.WORKFLOW_BASE_URI}/{workflowID}"
            ),
            RestHandler.Route(
                RestRequest.Method.HEAD,
                "${AlertingPlugin.WORKFLOW_BASE_URI}/{workflowID}"
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.WORKFLOW_BASE_URI}/{workflowID}")

        val workflowId = request.param("workflowID")
        if (workflowId == null || workflowId.isEmpty()) {
            throw IllegalArgumentException("missing id")
        }

        var srcContext = context(request)
        if (request.method() == RestRequest.Method.HEAD) {
            srcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        }
        val getWorkflowRequest =
            GetWorkflowRequest(workflowId, request.method())
        return RestChannelConsumer {
                channel ->
            client.execute(AlertingActions.GET_WORKFLOW_ACTION_TYPE, getWorkflowRequest, RestToXContentListener(channel))
        }
    }
}
