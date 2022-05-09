/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.action.node

import org.opensearch.action.FailedNodeException
import org.opensearch.action.support.nodes.BaseNodesResponse
import org.opensearch.alerting.core.settings.LegacyOpenDistroScheduledJobSettings
import org.opensearch.alerting.core.settings.ScheduledJobSettings
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.health.ClusterIndexHealth
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentFragment
import org.opensearch.common.xcontent.XContentBuilder

/**
 * ScheduledJobsStatsResponse is a class that will contain all the response from each node.
 */
class ScheduledJobsStatsResponse : BaseNodesResponse<ScheduledJobStats>, ToXContentFragment {

    private var scheduledJobEnabled: Boolean = false
    private var indexExists: Boolean? = null
    private var indexHealth: ClusterIndexHealth? = null

    constructor(si: StreamInput) : super(si) {
        this.scheduledJobEnabled = si.readBoolean()
        this.indexExists = si.readBoolean()
        this.indexHealth = si.readOptionalWriteable { ClusterIndexHealth(si) }
    }

    constructor(
        clusterName: ClusterName,
        nodeResponses: List<ScheduledJobStats>,
        failures: List<FailedNodeException>,
        scheduledJobEnabled: Boolean,
        indexExists: Boolean,
        indexHealth: ClusterIndexHealth?
    ) : super(clusterName, nodeResponses, failures) {
        this.scheduledJobEnabled = scheduledJobEnabled
        this.indexExists = indexExists
        this.indexHealth = indexHealth
    }

    override fun writeNodesTo(
        out: StreamOutput,
        nodes: MutableList<ScheduledJobStats>
    ) {
        out.writeList(nodes)
    }

    override fun readNodesFrom(si: StreamInput): MutableList<ScheduledJobStats> {
        return si.readList { ScheduledJobStats.readScheduledJobStatus(it) }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.field(LegacyOpenDistroScheduledJobSettings.SWEEPER_ENABLED.key, scheduledJobEnabled)
        builder.field(ScheduledJobSettings.SWEEPER_ENABLED.key, scheduledJobEnabled)
        builder.field("scheduled_job_index_exists", indexExists)
        builder.field("scheduled_job_index_status", indexHealth?.status?.name?.lowercase())
        val nodesOnSchedule = nodes.count { it.status == ScheduledJobStats.ScheduleStatus.GREEN }
        val nodesNotOnSchedule = nodes.count { it.status == ScheduledJobStats.ScheduleStatus.RED }
        builder.field("nodes_on_schedule", nodesOnSchedule)
        builder.field("nodes_not_on_schedule", nodesNotOnSchedule)
        builder.startObject("nodes")
        for (scheduledJobStatus in nodes) {
            builder.startObject(scheduledJobStatus.node.id)
            scheduledJobStatus.toXContent(builder, params)
            builder.endObject()
        }
        builder.endObject()

        return builder
    }
}
