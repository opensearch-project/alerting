/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.cleanup.action

import org.opensearch.action.FailedNodeException
import org.opensearch.action.support.nodes.BaseNodeResponse
import org.opensearch.action.support.nodes.BaseNodesResponse
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import java.io.IOException

/**
 * A node's acknowledgement that it received the announcement and started racing for the lock.
 *
 * [accepted] is false only when the node declined outright -- multi-tenancy is enabled, for instance. It does not
 * indicate whether the node won the lock or how many alerts it moved, because the drain outlives this response.
 */
class AlertCleanupNodeResponse : BaseNodeResponse {

    val accepted: Boolean

    constructor(si: StreamInput) : super(si) {
        accepted = si.readBoolean()
    }

    constructor(node: DiscoveryNode, accepted: Boolean) : super(node) {
        this.accepted = accepted
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeBoolean(accepted)
    }

    companion object {
        fun readResponse(si: StreamInput): AlertCleanupNodeResponse = AlertCleanupNodeResponse(si)
    }
}

class AlertCleanupResponse : BaseNodesResponse<AlertCleanupNodeResponse> {

    constructor(si: StreamInput) : super(si)

    constructor(
        clusterName: ClusterName,
        nodeResponses: List<AlertCleanupNodeResponse>,
        failures: List<FailedNodeException>,
    ) : super(clusterName, nodeResponses, failures)

    override fun writeNodesTo(out: StreamOutput, nodes: MutableList<AlertCleanupNodeResponse>) {
        out.writeList(nodes)
    }

    override fun readNodesFrom(si: StreamInput): MutableList<AlertCleanupNodeResponse> {
        return si.readList { AlertCleanupNodeResponse.readResponse(it) }
    }
}
