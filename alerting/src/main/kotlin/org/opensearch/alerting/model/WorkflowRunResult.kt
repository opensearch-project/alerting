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
    val workflowId: String,
    val workflowName: String,
    val monitorRunResults: List<MonitorRunResult<*>> = mutableListOf(),
    val executionStartTime: Instant,
    var executionEndTime: Instant? = null,
    val executionId: String,
    val error: Exception? = null,
    val triggerResults: Map<String, ChainedAlertTriggerRunResult> = mapOf(),
) : Writeable, ToXContent {

    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    constructor(sin: StreamInput) : this(
        workflowId = sin.readString(),
        workflowName = sin.readString(),
        monitorRunResults = sin.readList<MonitorRunResult<*>> { s: StreamInput -> MonitorRunResult.readFrom(s) },
        executionStartTime = sin.readInstant(),
        executionEndTime = sin.readOptionalInstant(),
        executionId = sin.readString(),
        error = sin.readException(),
        triggerResults = suppressWarning(sin.readMap()) as Map<String, ChainedAlertTriggerRunResult>
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(workflowId)
        out.writeString(workflowName)
        out.writeList(monitorRunResults)
        out.writeInstant(executionStartTime)
        out.writeOptionalInstant(executionEndTime)
        out.writeString(executionId)
        out.writeException(error)
        out.writeMap(triggerResults)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("execution_id", executionId)
        builder.field("workflow_name", workflowName)
        builder.field("workflow_id", workflowId)
        builder.field("trigger_results", triggerResults)
        builder.startArray("monitor_run_results")
        for (monitorResult in monitorRunResults) {
            monitorResult.toXContent(builder, ToXContent.EMPTY_PARAMS)
        }
        builder.endArray()
            .field("execution_start_time", executionStartTime)
            .field("execution_end_time", executionEndTime)
            .field("error", error?.message)
            .endObject()
        return builder
    }

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): WorkflowRunResult {
            return WorkflowRunResult(sin)
        }

        @Suppress("UNCHECKED_CAST")
        fun suppressWarning(map: MutableMap<String?, Any?>?): Map<String, TriggerRunResult> {
            return map as Map<String, TriggerRunResult>
        }
    }
}
