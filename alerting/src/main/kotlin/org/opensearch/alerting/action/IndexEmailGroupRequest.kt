/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.model.destination.email.EmailGroup
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.rest.RestRequest
import java.io.IOException

class IndexEmailGroupRequest : ActionRequest {
    var emailGroupID: String
    var seqNo: Long
    var primaryTerm: Long
    var refreshPolicy: WriteRequest.RefreshPolicy
    var method: RestRequest.Method
    var emailGroup: EmailGroup

    constructor(
        emailGroupID: String,
        seqNo: Long,
        primaryTerm: Long,
        refreshPolicy: WriteRequest.RefreshPolicy,
        method: RestRequest.Method,
        emailGroup: EmailGroup
    ) : super() {
        this.emailGroupID = emailGroupID
        this.seqNo = seqNo
        this.primaryTerm = primaryTerm
        this.refreshPolicy = refreshPolicy
        this.method = method
        this.emailGroup = emailGroup
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(), // emailGroupID
        sin.readLong(), // seqNo
        sin.readLong(), // primaryTerm
        WriteRequest.RefreshPolicy.readFrom(sin), // refreshPolicy
        sin.readEnum(RestRequest.Method::class.java), // method
        EmailGroup.readFrom(sin) // emailGroup
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(emailGroupID)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        refreshPolicy.writeTo(out)
        out.writeEnum(method)
        emailGroup.writeTo(out)
    }
}
