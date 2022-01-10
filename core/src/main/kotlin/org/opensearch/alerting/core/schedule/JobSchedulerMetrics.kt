/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.schedule

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentFragment
import org.opensearch.common.xcontent.XContentBuilder
import java.time.Instant

class JobSchedulerMetrics : ToXContentFragment, Writeable {
    val scheduledJobId: String
    val lastExecutionTime: Long?
    val runningOnTime: Boolean

    constructor(scheduledJobId: String, lastExecutionTime: Long?, runningOnTime: Boolean) {
        this.scheduledJobId = scheduledJobId
        this.lastExecutionTime = lastExecutionTime
        this.runningOnTime = runningOnTime
    }

    constructor(si: StreamInput) {
        scheduledJobId = si.readString()
        lastExecutionTime = si.readOptionalLong()
        runningOnTime = si.readBoolean()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(scheduledJobId)
        out.writeOptionalLong(lastExecutionTime)
        out.writeBoolean(runningOnTime)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        if (lastExecutionTime != null)
            builder.timeField(
                "last_execution_time", "last_execution_time_in_millis",
                Instant.ofEpochMilli(lastExecutionTime).toEpochMilli()
            )
        builder.field("running_on_time", runningOnTime)
        return builder
    }
}
