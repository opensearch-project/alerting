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
import java.util.Collections

class AcknowledgeAlertRequest : ActionRequest {
    val monitorId: String
    val alertIds: List<String>
    val refreshPolicy: WriteRequest.RefreshPolicy

    constructor(
        monitorId: String,
        alertIds: List<String>,
        refreshPolicy: WriteRequest.RefreshPolicy
    ) : super() {
        this.monitorId = monitorId
        this.alertIds = alertIds
        this.refreshPolicy = refreshPolicy
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(), // monitorId
        Collections.unmodifiableList(sin.readStringList()), // alertIds
        WriteRequest.RefreshPolicy.readFrom(sin) // refreshPolicy
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(monitorId)
        out.writeStringCollection(alertIds)
        refreshPolicy.writeTo(out)
    }
}
