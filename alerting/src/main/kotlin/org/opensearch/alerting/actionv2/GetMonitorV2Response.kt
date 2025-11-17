/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.commons.alerting.util.IndexUtils.Companion._ID
import org.opensearch.commons.alerting.util.IndexUtils.Companion._PRIMARY_TERM
import org.opensearch.commons.alerting.util.IndexUtils.Companion._SEQ_NO
import org.opensearch.commons.alerting.util.IndexUtils.Companion._VERSION
import org.opensearch.commons.notifications.action.BaseResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException

class GetMonitorV2Response : BaseResponse {
    var id: String
    var version: Long
    var seqNo: Long
    var primaryTerm: Long
    var monitorV2: MonitorV2?

    constructor(
        id: String,
        version: Long,
        seqNo: Long,
        primaryTerm: Long,
        monitorV2: MonitorV2?
    ) : super() {
        this.id = id
        this.version = version
        this.seqNo = seqNo
        this.primaryTerm = primaryTerm
        this.monitorV2 = monitorV2
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(), // id
        version = sin.readLong(), // version
        seqNo = sin.readLong(), // seqNo
        primaryTerm = sin.readLong(), // primaryTerm
        monitorV2 = if (sin.readBoolean()) {
            MonitorV2.readFrom(sin) // monitorV2
        } else {
            null
        }
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        if (monitorV2 != null) {
            out.writeBoolean(true)
            MonitorV2.writeTo(out, monitorV2!!)
        } else {
            out.writeBoolean(false)
        }
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(_ID, id)
            .field(_VERSION, version)
            .field(_SEQ_NO, seqNo)
            .field(_PRIMARY_TERM, primaryTerm)
        if (monitorV2 != null) {
            builder.field("monitorV2", monitorV2)
        }
        return builder.endObject()
    }
}
