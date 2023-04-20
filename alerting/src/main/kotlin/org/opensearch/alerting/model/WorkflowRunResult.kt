/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.commons.alerting.model.Alert
import java.io.IOException
import java.lang.Exception
import java.time.Instant

data class WorkflowRunResult(
    val workflowRunResult: List<MonitorRunResult<*>> = mutableListOf(),
    val executionStartTime: Instant,
    val executionEndTime: Instant,
    val executionId: String,
    val error: Exception? = null,
    val chainedAlert: Alert? = null
) : Writeable, ToXContent {

    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    constructor(sin: StreamInput) : this(
        sin.readList<MonitorRunResult<*>> { s: StreamInput -> MonitorRunResult.readFrom(s) },
        sin.readInstant(),
        sin.readInstant(),
        sin.readString(),
        sin.readException(),
        Alert.readFrom(sin)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeList(workflowRunResult)
        out.writeInstant(executionStartTime)
        out.writeInstant(executionEndTime)
        out.writeString(executionId)
        out.writeException(error)
        chainedAlert?.writeTo(out)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        try {
            builder.startObject().startArray("workflow_run_result")
            for (monitorResult in workflowRunResult) {
                monitorResult.toXContent(builder, ToXContent.EMPTY_PARAMS)
            }

            builder.endArray().field("execution_start_time", executionStartTime)
                .field("execution_end_time", executionEndTime)
                .field("error", error?.message)
            builder.startArray("chained_alert")
            chainedAlert?.toXContent(builder, ToXContent.EMPTY_PARAMS)
            builder.endArray()
            builder.endObject()
        } catch (e: Exception) {
            println(e)
        }
        return builder
    }
}
