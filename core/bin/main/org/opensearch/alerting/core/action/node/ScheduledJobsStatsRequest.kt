/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.action.node

import org.opensearch.action.support.nodes.BaseNodesRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import java.io.IOException

/**
 * A request to get node (cluster) level ScheduledJobsStatus.
 * By default all the parameters will be true.
 */
class ScheduledJobsStatsRequest : BaseNodesRequest<ScheduledJobsStatsRequest> {
    var jobSchedulingMetrics: Boolean = true
    var jobsInfo: Boolean = true

    constructor(si: StreamInput) : super(si) {
        jobSchedulingMetrics = si.readBoolean()
        jobsInfo = si.readBoolean()
    }
    constructor(nodeIds: Array<String>) : super(*nodeIds)

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeBoolean(jobSchedulingMetrics)
        out.writeBoolean(jobsInfo)
    }

    fun all(): ScheduledJobsStatsRequest {
        jobSchedulingMetrics = true
        jobsInfo = true
        return this
    }

    fun clear(): ScheduledJobsStatsRequest {
        jobSchedulingMetrics = false
        jobsInfo = false
        return this
    }
}
