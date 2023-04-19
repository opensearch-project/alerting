/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException
import java.lang.Exception
import java.time.Instant

data class WorkflowRunResult(
    val workflowRunResult: List<MonitorRunResult<*>> = mutableListOf(),
    val executionStartTime: Instant,
    val executionEndTime: Instant? = null,
    val executionId: String,
    val error: Exception? = null
) : Writeable, ToXContent {

    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    constructor(sin: StreamInput) : this(
        sin.readList<MonitorRunResult<*>> { s: StreamInput -> MonitorRunResult.readFrom(s) },
        sin.readInstant(),
        sin.readInstant(),
        sin.readString(),
        sin.readException()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeList(workflowRunResult)
        out.writeInstant(executionStartTime)
        out.writeInstant(executionEndTime)
        out.writeString(executionId)
        out.writeException(error)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject().startArray("workflow_run_result")
        for (monitorResult in workflowRunResult) {
            monitorResult.toXContent(builder, ToXContent.EMPTY_PARAMS)
        }
        builder.endArray().field("execution_start_time", executionStartTime)
            .field("execution_end_time", executionEndTime)
            .field("error", error?.message).endObject()
        return builder
    }
}
