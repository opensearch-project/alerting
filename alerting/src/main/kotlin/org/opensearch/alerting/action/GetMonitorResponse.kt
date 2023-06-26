/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.util.IndexUtils.Companion._ID
import org.opensearch.commons.alerting.util.IndexUtils.Companion._PRIMARY_TERM
import org.opensearch.commons.alerting.util.IndexUtils.Companion._SEQ_NO
import org.opensearch.commons.alerting.util.IndexUtils.Companion._VERSION
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.rest.RestStatus
import java.io.IOException

class GetMonitorResponse : ActionResponse, ToXContentObject {
    var id: String
    var version: Long
    var seqNo: Long
    var primaryTerm: Long
    var status: RestStatus
    var monitor: Monitor?
    var associatedWorkflows: List<String>?

    constructor(
        id: String,
        version: Long,
        seqNo: Long,
        primaryTerm: Long,
        status: RestStatus,
        monitor: Monitor?,
        associatedCompositeMonitors: List<String>?,
    ) : super() {
        this.id = id
        this.version = version
        this.seqNo = seqNo
        this.primaryTerm = primaryTerm
        this.status = status
        this.monitor = monitor
        this.associatedWorkflows = associatedCompositeMonitors ?: emptyList()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(), // id
        version = sin.readLong(), // version
        seqNo = sin.readLong(), // seqNo
        primaryTerm = sin.readLong(), // primaryTerm
        status = sin.readEnum(RestStatus::class.java), // RestStatus
        monitor = if (sin.readBoolean()) {
            Monitor.readFrom(sin) // monitor
        } else null,
        associatedCompositeMonitors = sin.readOptionalStringList(),
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeEnum(status)
        if (monitor != null) {
            out.writeBoolean(true)
            monitor?.writeTo(out)
        } else {
            out.writeBoolean(false)
        }
        out.writeOptionalStringCollection(associatedWorkflows)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(_ID, id)
            .field(_VERSION, version)
            .field(_SEQ_NO, seqNo)
            .field(_PRIMARY_TERM, primaryTerm)
        if (monitor != null) {
            builder.field("monitor", monitor)
        }
        if (associatedWorkflows != null) {
            builder.field("associated_workflows", associatedWorkflows)
        }

        return builder.endObject()
    }
}
