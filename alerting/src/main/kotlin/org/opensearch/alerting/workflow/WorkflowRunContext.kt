/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.workflow

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder

data class WorkflowRunContext(
    // In case of dry run it's random generated id, while in other cases it's workflowId
    val workflowId: String,
    val workflowMetadataId: String,
    val chainedMonitorId: String?,
    val matchingDocIdsPerIndex: Map<String, List<String>>,
    val auditDelegateMonitorAlerts: Boolean,
) : Writeable, ToXContentObject {
    companion object {
        fun readFrom(sin: StreamInput): WorkflowRunContext {
            return WorkflowRunContext(sin)
        }
    }

    constructor(sin: StreamInput) : this(
        sin.readString(),
        sin.readString(),
        sin.readOptionalString(),
        sin.readMap() as Map<String, List<String>>,
        sin.readBoolean()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(workflowId)
        out.writeString(workflowMetadataId)
        out.writeOptionalString(chainedMonitorId)
        out.writeMap(matchingDocIdsPerIndex)
        out.writeBoolean(auditDelegateMonitorAlerts)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        builder.startObject()
            .field("workflow_id", workflowId)
            .field("workflow_metadata_id", workflowMetadataId)
            .field("chained_monitor_id", chainedMonitorId)
            .field("matching_doc_ids_per_index", matchingDocIdsPerIndex)
            .field("audit_delegate_monitor_alerts", auditDelegateMonitorAlerts)
            .endObject()
        return builder
    }
}
