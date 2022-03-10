/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionResponse
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.util._ID
import org.opensearch.alerting.util._PRIMARY_TERM
import org.opensearch.alerting.util._SEQ_NO
import org.opensearch.alerting.util._VERSION
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.rest.RestStatus
import java.io.IOException

class IndexDestinationResponse : ActionResponse, ToXContentObject {
    val id: String
    val version: Long
    val seqNo: Long
    val primaryTerm: Long
    val status: RestStatus
    val destination: Destination

    constructor(
        id: String,
        version: Long,
        seqNo: Long,
        primaryTerm: Long,
        status: RestStatus,
        destination: Destination
    ) : super() {
        this.id = id
        this.version = version
        this.seqNo = seqNo
        this.primaryTerm = primaryTerm
        this.status = status
        this.destination = destination
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super() {
        this.id = sin.readString()
        this.version = sin.readLong()
        this.seqNo = sin.readLong()
        this.primaryTerm = sin.readLong()
        this.status = sin.readEnum(RestStatus::class.java)
        this.destination = Destination.readFrom(sin)
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeEnum(status)
        destination.writeTo(out)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(_ID, id)
            .field(_VERSION, version)
            .field(_SEQ_NO, seqNo)
            .field(_PRIMARY_TERM, primaryTerm)
            .field("destination", destination)
            .endObject()
    }
}
