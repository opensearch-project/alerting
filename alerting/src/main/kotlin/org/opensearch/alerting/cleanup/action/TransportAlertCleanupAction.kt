/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.cleanup.action

import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.FailedNodeException
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.nodes.TransportNodesAction
import org.opensearch.alerting.cleanup.AlertCleanupService
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportRequest
import org.opensearch.transport.TransportService
import java.io.IOException

private val log = LogManager.getLogger(TransportAlertCleanupAction::class.java)

/**
 * Hands the announcement to every addressed node so each can race for the cleanup task's lock.
 *
 * [nodeOperation] starts the drain and returns; it does not wait for it. The drain takes minutes for a large backlog and
 * proceeds under the task's own lock, so holding a transport thread for it would achieve nothing -- the durable task,
 * not this call, is what guarantees completion.
 */
class TransportAlertCleanupAction : TransportNodesAction<
        AlertCleanupRequest,
        AlertCleanupResponse,
        TransportAlertCleanupAction.NodeRequest,
        AlertCleanupNodeResponse
        > {

    @Inject
    constructor(
        threadPool: ThreadPool,
        clusterService: ClusterService,
        transportService: TransportService,
        actionFilters: ActionFilters,
    ) : super(
        AlertCleanupAction.NAME,
        threadPool,
        clusterService,
        transportService,
        actionFilters,
        { AlertCleanupRequest(it) },
        { NodeRequest(it) },
        ThreadPool.Names.MANAGEMENT,
        AlertCleanupNodeResponse::class.java
    )

    override fun newNodeRequest(request: AlertCleanupRequest): NodeRequest = NodeRequest(request.jobId)

    override fun newNodeResponse(si: StreamInput): AlertCleanupNodeResponse = AlertCleanupNodeResponse(si)

    override fun newResponse(
        request: AlertCleanupRequest,
        responses: MutableList<AlertCleanupNodeResponse>,
        failures: MutableList<FailedNodeException>,
    ): AlertCleanupResponse = AlertCleanupResponse(clusterService.clusterName, responses, failures)

    override fun nodeOperation(request: NodeRequest): AlertCleanupNodeResponse {
        val node = clusterService.localNode()
        if (AlertCleanupService.multiTenancyEnabled) {
            return AlertCleanupNodeResponse(node, false)
        }
        AlertCleanupService.launch {
            try {
                AlertCleanupService.runTasksFor(request.jobId)
            } catch (e: Exception) {
                log.error("Alert cleanup for job ${request.jobId} failed on node ${node.id}.", e)
            }
        }
        return AlertCleanupNodeResponse(node, true)
    }

    class NodeRequest : TransportRequest {

        lateinit var jobId: String

        constructor(si: StreamInput) : super(si) {
            jobId = si.readString()
        }

        constructor(jobId: String) : super() {
            this.jobId = jobId
        }

        @Throws(IOException::class)
        override fun writeTo(out: StreamOutput) {
            super.writeTo(out)
            out.writeString(jobId)
        }
    }
}
