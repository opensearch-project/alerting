/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.WriteRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import java.io.IOException

class DeleteEmailAccountRequest : ActionRequest {

    val emailAccountID: String
    val refreshPolicy: WriteRequest.RefreshPolicy

    constructor(emailAccountID: String, refreshPolicy: WriteRequest.RefreshPolicy) : super() {
        this.emailAccountID = emailAccountID
        this.refreshPolicy = refreshPolicy
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super() {
        emailAccountID = sin.readString()
        refreshPolicy = WriteRequest.RefreshPolicy.readFrom(sin)
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(emailAccountID)
        refreshPolicy.writeTo(out)
    }
}
