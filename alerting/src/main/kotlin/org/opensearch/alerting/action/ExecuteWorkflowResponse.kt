/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionResponse
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import java.io.IOException

class ExecuteWorkflowResponse : ActionResponse, ToXContentObject {

    val workflowRunResult: List<MonitorRunResult<*>>

    constructor(monitorRunResult: List<MonitorRunResult<*>>) : super() {
        this.workflowRunResult = monitorRunResult
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readList<MonitorRunResult<*>> { s: StreamInput -> MonitorRunResult.readFrom(s) }
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeList(workflowRunResult)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startArray()
        for (monitorResult in workflowRunResult) {
            monitorResult.toXContent(builder, ToXContent.EMPTY_PARAMS)
        }
        builder.endArray()
        return builder
    }
}
