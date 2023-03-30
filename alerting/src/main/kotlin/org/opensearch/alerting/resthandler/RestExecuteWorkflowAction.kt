/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.ExecuteMonitorAction
import org.opensearch.alerting.action.ExecuteMonitorRequest
import org.opensearch.alerting.action.ExecuteWorkflowAction
import org.opensearch.alerting.action.ExecuteWorkflowRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.time.Instant

private val log = LogManager.getLogger(RestExecuteWorkflowAction::class.java)

class RestExecuteWorkflowAction : BaseRestHandler() {

    override fun getName(): String = "execute_workflow_action"

    override fun routes(): List<RestHandler.Route> {
        return mutableListOf(
            RestHandler.Route(
                RestRequest.Method.POST,
                "${AlertingPlugin.WORKFLOW_BASE_URI}/{workflowID}/_execute",
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.WORKFLOW_BASE_URI}/_execute")

        return RestChannelConsumer { channel ->
            val dryrun = request.paramAsBoolean("dryrun", false)
            val requestEnd = request.paramAsTime("period_end", TimeValue(Instant.now().toEpochMilli()))

            if (request.hasParam("workflowID")) {
                val workflowId = request.param("workflowID")
                val execMonitorRequest = ExecuteWorkflowRequest(dryrun, requestEnd, workflowId, null)
                client.execute(ExecuteWorkflowAction.INSTANCE, execMonitorRequest, RestToXContentListener(channel))
            } else throw java.lang.IllegalArgumentException("worfklowID is mandatory")
        }
    }

    override fun responseParams(): Set<String> {
        return setOf("dryrun", "period_end", "workflowID")
    }
}
