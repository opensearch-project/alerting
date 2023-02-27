/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionResponse
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.WorkflowRunResult
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import java.io.IOException
import java.time.Instant

class ExecuteWorkflowResponse : ActionResponse, ToXContentObject {

    val workflowRunResult: List<MonitorRunResult<*>>
    val executionStartTime: Instant
    val executionEndTime: Instant
    val status: WorkflowRunResult.WorkflowExecutionStatus

    constructor(
        monitorRunResult: List<MonitorRunResult<*>>,
        executionStartTime: Instant,
        executionEndTime: Instant,
        status: WorkflowRunResult.WorkflowExecutionStatus
    ) : super() {
        this.workflowRunResult = monitorRunResult
        this.executionStartTime = executionStartTime
        this.executionEndTime = executionEndTime
        this.status = status
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readList<MonitorRunResult<*>> { s: StreamInput -> MonitorRunResult.readFrom(s) },
        sin.readInstant(),
        sin.readInstant(),
        sin.readEnum(WorkflowRunResult.WorkflowExecutionStatus::class.java)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeList(workflowRunResult)
        out.writeInstant(executionStartTime)
        out.writeInstant(executionEndTime)
        out.writeEnum(status)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject("workflow_run_result")
        builder.startArray()
        for (monitorResult in workflowRunResult) {
            monitorResult.toXContent(builder, ToXContent.EMPTY_PARAMS)
        }
        builder.endArray()
        builder.endObject()
        builder.field("execution_start_time", executionStartTime)
        builder.field("execution_end_time", executionEndTime)
        builder.field("status", status)
        return builder
    }
}
