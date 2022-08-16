/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.rest.RestRequest
import org.opensearch.search.fetch.subphase.FetchSourceContext
import java.io.IOException

class GetEmailAccountRequest : ActionRequest {
    val emailAccountID: String
    val version: Long
    val method: RestRequest.Method
    val srcContext: FetchSourceContext?

    constructor(
        emailAccountID: String,
        version: Long,
        method: RestRequest.Method,
        srcContext: FetchSourceContext?
    ) : super() {
        this.emailAccountID = emailAccountID
        this.version = version
        this.method = method
        this.srcContext = srcContext
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(), // emailAccountID
        sin.readLong(), // version
        sin.readEnum(RestRequest.Method::class.java), // method
        if (sin.readBoolean()) {
            FetchSourceContext(sin) // srcContext
        } else null
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(emailAccountID)
        out.writeLong(version)
        out.writeEnum(method)
        if (srcContext != null) {
            out.writeBoolean(true)
            srcContext.writeTo(out)
        } else {
            out.writeBoolean(false)
        }
    }
}
