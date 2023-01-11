/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.commons.alerting.util.IndexUtils.Companion._ID
import java.io.IOException

class MonitorExplainResponse : ActionResponse, ToXContentObject {
    var monitorId: String
    var seqNoDiff: Long
    var docDiff: Long?

    constructor(
        monitorId: String,
        seqNoDiff: Long,
        docDiff: Long?
    ) : super() {
        this.monitorId = monitorId
        this.seqNoDiff = seqNoDiff
        this.docDiff = docDiff
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(), // id
        sin.readLong(), // seqNo
        sin.readOptionalLong() // docDiff
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(monitorId)
        out.writeLong(seqNoDiff)
        out.writeOptionalLong(docDiff)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(_ID, monitorId)
            .field("seq_no_diff", seqNoDiff)

        if (docDiff != null)
            builder.field("doc_diff", docDiff)

        return builder.endObject()
    }
}
