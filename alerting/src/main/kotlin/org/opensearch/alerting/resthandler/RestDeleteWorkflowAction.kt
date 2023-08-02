/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.util.REFRESH
import org.opensearch.client.node.NodeClient
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.DeleteWorkflowRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

/**
 * This class consists of the REST handler to delete workflows.
 */
class RestDeleteWorkflowAction : BaseRestHandler() {

    private val log = LogManager.getLogger(javaClass)

    override fun getName(): String {
        return "delete_workflow_action"
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(
                RestRequest.Method.DELETE,
                "${AlertingPlugin.WORKFLOW_BASE_URI}/{workflowID}"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.WORKFLOW_BASE_URI}/{workflowID}")

        val workflowId = request.param("workflowID")
        val deleteDelegateMonitors = request.paramAsBoolean("deleteDelegateMonitors", false)
        log.debug("${request.method()} ${request.uri()}")

        val refreshPolicy =
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH, WriteRequest.RefreshPolicy.IMMEDIATE.value))
        val deleteWorkflowRequest = DeleteWorkflowRequest(workflowId, deleteDelegateMonitors)

        return RestChannelConsumer { channel ->
            client.execute(
                AlertingActions.DELETE_WORKFLOW_ACTION_TYPE, deleteWorkflowRequest,
                RestToXContentListener(channel)
            )
        }
    }
}
