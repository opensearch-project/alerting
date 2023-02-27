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
import java.io.IOException

data class WorkflowRunResult(
    val workflowRunResult: List<MonitorRunResult<*>>
) : Writeable, ToXContent {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readList<MonitorRunResult<*>> { s: StreamInput -> MonitorRunResult.readFrom(s) }
    )

    override fun writeTo(out: StreamOutput) {
        out.writeList(workflowRunResult)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startArray()
        for (monitorResult in workflowRunResult) {
            monitorResult.toXContent(builder, ToXContent.EMPTY_PARAMS)
        }
        builder.endArray()
        return builder
    }

    enum class WorkflowExecutionStatus(val value: String) {
        SUCCESSFUL("successful"),
        UNSUCCESSFUL("unsuccessful")
    }
}
