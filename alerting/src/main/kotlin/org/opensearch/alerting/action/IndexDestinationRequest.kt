/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.rest.RestRequest
import java.io.IOException

class IndexDestinationRequest : ActionRequest {
    val destinationId: String
    val seqNo: Long
    val primaryTerm: Long
    val refreshPolicy: WriteRequest.RefreshPolicy
    val method: RestRequest.Method
    var destination: Destination

    constructor(
        destinationId: String,
        seqNo: Long,
        primaryTerm: Long,
        refreshPolicy: WriteRequest.RefreshPolicy,
        method: RestRequest.Method,
        destination: Destination
    ) : super() {
        this.destinationId = destinationId
        this.seqNo = seqNo
        this.primaryTerm = primaryTerm
        this.refreshPolicy = refreshPolicy
        this.method = method
        this.destination = destination
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super() {
        this.destinationId = sin.readString()
        this.seqNo = sin.readLong()
        this.primaryTerm = sin.readLong()
        this.refreshPolicy = WriteRequest.RefreshPolicy.readFrom(sin)
        this.method = sin.readEnum(RestRequest.Method::class.java)
        this.destination = Destination.readFrom(sin)
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(destinationId)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        refreshPolicy.writeTo(out)
        out.writeEnum(method)
        destination.writeTo(out)
    }
}
