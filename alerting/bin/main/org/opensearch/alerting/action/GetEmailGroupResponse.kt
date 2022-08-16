/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionResponse
import org.opensearch.alerting.model.destination.email.EmailGroup
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

class GetEmailGroupResponse : ActionResponse, ToXContentObject {
    var id: String
    var version: Long
    var seqNo: Long
    var primaryTerm: Long
    var status: RestStatus
    var emailGroup: EmailGroup?

    constructor(
        id: String,
        version: Long,
        seqNo: Long,
        primaryTerm: Long,
        status: RestStatus,
        emailGroup: EmailGroup?
    ) : super() {
        this.id = id
        this.version = version
        this.seqNo = seqNo
        this.primaryTerm = primaryTerm
        this.status = status
        this.emailGroup = emailGroup
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(), // id
        sin.readLong(), // version
        sin.readLong(), // seqNo
        sin.readLong(), // primaryTerm
        sin.readEnum(RestStatus::class.java), // RestStatus
        if (sin.readBoolean()) {
            EmailGroup.readFrom(sin) // emailGroup
        } else null
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeEnum(status)
        if (emailGroup != null) {
            out.writeBoolean(true)
            emailGroup?.writeTo(out)
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
        if (emailGroup != null)
            builder.field("email_group", emailGroup)

        return builder.endObject()
    }
}
