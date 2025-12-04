/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.WriteRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import java.io.IOException

class DeleteMonitorV2Request : ActionRequest {
    val monitorV2Id: String
    val refreshPolicy: WriteRequest.RefreshPolicy

    constructor(monitorV2Id: String, refreshPolicy: WriteRequest.RefreshPolicy) : super() {
        this.monitorV2Id = monitorV2Id
        this.refreshPolicy = refreshPolicy
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        monitorV2Id = sin.readString(),
        refreshPolicy = WriteRequest.RefreshPolicy.readFrom(sin),
    )

    override fun validate(): ActionRequestValidationException? = null

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(monitorV2Id)
        refreshPolicy.writeTo(out)
    }
}
