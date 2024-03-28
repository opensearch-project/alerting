/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.alerting.model.IndexExecutionContext
import org.opensearch.alerting.model.MonitorMetadata
import org.opensearch.alerting.workflow.WorkflowRunContext
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException

class DocLevelMonitorFanOutRequest : ActionRequest, ToXContentObject {
    val monitor: Monitor
    val dryRun: Boolean
    val monitorMetadata: MonitorMetadata
    val executionId: String
    val indexExecutionContext: IndexExecutionContext
    val shardIds: List<ShardId>
    val concreteIndicesSeenSoFar: List<String>
    val workflowRunContext: WorkflowRunContext?

    constructor(
        monitor: Monitor,
        dryRun: Boolean,
        monitorMetadata: MonitorMetadata,
        executionId: String,
        indexExecutionContext: IndexExecutionContext,
        shardIds: List<ShardId>,
        concreteIndicesSeenSoFar: List<String>,
        workflowRunContext: WorkflowRunContext?,
    ) : super() {
        this.monitor = monitor
        this.dryRun = dryRun
        this.monitorMetadata = monitorMetadata
        this.executionId = executionId
        this.indexExecutionContext = indexExecutionContext
        this.shardIds = shardIds
        this.concreteIndicesSeenSoFar = concreteIndicesSeenSoFar
        this.workflowRunContext = workflowRunContext
        require(false == shardIds.isEmpty()) { }
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        monitor = Monitor.readFrom(sin)!!,
        dryRun = sin.readBoolean(),
        monitorMetadata = MonitorMetadata.readFrom(sin),
        executionId = sin.readString(),
        indexExecutionContext = IndexExecutionContext(sin),
        shardIds = sin.readList(::ShardId),
        concreteIndicesSeenSoFar = sin.readStringList(),
        workflowRunContext = if (sin.readBoolean()) {
            WorkflowRunContext(sin)
        } else null,
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        monitor.writeTo(out)
        out.writeBoolean(dryRun)
        monitorMetadata.writeTo(out)
        out.writeString(executionId)
        indexExecutionContext.writeTo(out)
        out.writeCollection(shardIds)
        out.writeStringCollection(concreteIndicesSeenSoFar)
        out.writeBoolean(workflowRunContext != null)
        workflowRunContext?.writeTo(out)
    }

    override fun validate(): ActionRequestValidationException? {
        var actionValidationException: ActionRequestValidationException? = null
        if (shardIds.isEmpty()) {
            actionValidationException = ActionRequestValidationException()
            actionValidationException.addValidationError("shard_ids is null or empty")
        }
        return actionValidationException
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field("monitor", monitor)
            .field("dry_run", dryRun)
            .field("execution_id", executionId)
            .field("index_execution_context", indexExecutionContext)
            .field("shard_ids", shardIds)
            .field("concrete_indices", concreteIndicesSeenSoFar)
            .field("workflow_run_context", workflowRunContext)
        return builder.endObject()
    }
}
