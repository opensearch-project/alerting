/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.alerting.core.action.node

import org.opensearch.alerting.core.settings.ScheduledJobSettings
import org.opensearch.action.FailedNodeException
import org.opensearch.action.support.nodes.BaseNodesResponse
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

    constructor(si: StreamInput): super(si) {
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
        builder.field(ScheduledJobSettings.SWEEPER_ENABLED.key, scheduledJobEnabled)
        builder.field("scheduled_job_index_exists", indexExists)
        builder.field("scheduled_job_index_status", indexHealth?.status?.name?.toLowerCase())
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
