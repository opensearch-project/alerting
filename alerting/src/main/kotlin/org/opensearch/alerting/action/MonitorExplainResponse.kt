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
    var documentsBehind: Long

    constructor(
        monitorId: String,
        documentsBehind: Long,
    ) : super() {
        this.monitorId = monitorId
        this.documentsBehind = documentsBehind
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(), // id
        sin.readLong(), // documentsBehind
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(monitorId)
        out.writeLong(documentsBehind)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(_ID, monitorId)
            .field("documents_behind", documentsBehind)

        return builder.endObject()
    }
}
