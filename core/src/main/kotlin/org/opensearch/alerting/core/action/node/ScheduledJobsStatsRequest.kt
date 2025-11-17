/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.action.node

import org.opensearch.action.support.nodes.BaseNodesRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import java.io.IOException

/**
 * A request to get node (cluster) level ScheduledJobsStatus.
 * By default all the parameters will be true.
 */
class ScheduledJobsStatsRequest : BaseNodesRequest<ScheduledJobsStatsRequest> {
    var jobSchedulingMetrics: Boolean = true
    var jobsInfo: Boolean = true
    // show Alerting V2 scheduled jobs if true, Alerting V1 scheduled jobs if false, all scheduled jobs if null
    var showAlertingV2ScheduledJobs: Boolean? = null

    constructor(si: StreamInput) : super(si) {
        jobSchedulingMetrics = si.readBoolean()
        jobsInfo = si.readBoolean()
        showAlertingV2ScheduledJobs = si.readOptionalBoolean()
    }

    constructor(nodeIds: Array<String>, showAlertingV2ScheduledJobs: Boolean?) : super(*nodeIds) {
        this.showAlertingV2ScheduledJobs = showAlertingV2ScheduledJobs
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeBoolean(jobSchedulingMetrics)
        out.writeBoolean(jobsInfo)
        out.writeOptionalBoolean(showAlertingV2ScheduledJobs)
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
