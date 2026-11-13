/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.cleanup.action

import org.opensearch.action.support.nodes.BaseNodesRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import java.io.IOException

/**
 * Tells the addressed nodes that [jobId] has cleanup outstanding.
 *
 * The node list is chosen by the sender rather than left empty (which would mean "all nodes"), because the drain is
 * data-plane work and does not belong on a dedicated cluster manager node.
 */
class AlertCleanupRequest : BaseNodesRequest<AlertCleanupRequest> {

    lateinit var jobId: String

    constructor(si: StreamInput) : super(si) {
        jobId = si.readString()
    }

    constructor(jobId: String, nodeIds: Array<String>) : super(*nodeIds) {
        this.jobId = jobId
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(jobId)
    }
}
