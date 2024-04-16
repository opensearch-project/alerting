/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.alerting.model.InputRunResults
import org.opensearch.alerting.util.AlertingException
import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException

class DocLevelMonitorFanOutResponse : ActionResponse, ToXContentObject {
    val nodeId: String
    val executionId: String
    val monitorId: String
    val lastRunContexts: MutableMap<String, Any>
    val inputResults: InputRunResults
    val triggerResults: Map<String, DocumentLevelTriggerRunResult>
    val exception: AlertingException?

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        nodeId = sin.readString(),
        executionId = sin.readString(),
        monitorId = sin.readString(),
        lastRunContexts = sin.readMap()!! as MutableMap<String, Any>,
        inputResults = InputRunResults.readFrom(sin),
        triggerResults = suppressWarning(sin.readMap(StreamInput::readString, DocumentLevelTriggerRunResult::readFrom)),
        exception = sin.readException()
    )

    constructor(
        nodeId: String,
        executionId: String,
        monitorId: String,
        lastRunContexts: MutableMap<String, Any>,
        inputResults: InputRunResults = InputRunResults(), // partial,
        triggerResults: Map<String, DocumentLevelTriggerRunResult> = mapOf(),
        exception: AlertingException? = null
    ) : super() {
        this.nodeId = nodeId
        this.executionId = executionId
        this.monitorId = monitorId
        this.lastRunContexts = lastRunContexts
        this.inputResults = inputResults
        this.triggerResults = triggerResults
        this.exception = exception
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(nodeId)
        out.writeString(executionId)
        out.writeString(monitorId)
        out.writeMap(lastRunContexts)
        inputResults.writeTo(out)
        out.writeMap(
            triggerResults,
            StreamOutput::writeString,
            { stream, stats -> stats.writeTo(stream) }
        )
        out.writeException(exception)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field("node_id", nodeId)
            .field("execution_id", executionId)
            .field("monitor_id", monitorId)
            .field("last_run_contexts", lastRunContexts)
            .field("input_results", inputResults)
            .field("trigger_results", triggerResults)
            .field("exception", exception)
            .endObject()
        return builder
    }

    companion object {
        @Suppress("UNCHECKED_CAST")
        fun suppressWarning(map: MutableMap<String?, Any?>?): Map<String, DocumentLevelTriggerRunResult> {
            return map as Map<String, DocumentLevelTriggerRunResult>
        }
    }
}
