/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.alerting.model.Table
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import java.io.IOException

class GetAlertsRequest : ActionRequest {
    val table: Table
    val severityLevel: String
    val alertState: String
    val monitorId: String?
    val alertIndex: String?

    constructor(
        table: Table,
        severityLevel: String,
        alertState: String,
        monitorId: String?,
        alertIndex: String?,
    ) : super() {
        this.table = table
        this.severityLevel = severityLevel
        this.alertState = alertState
        this.monitorId = monitorId
        this.alertIndex = alertIndex
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        table = Table.readFrom(sin),
        severityLevel = sin.readString(),
        alertState = sin.readString(),
        monitorId = sin.readOptionalString(),
        alertIndex = sin.readOptionalString()
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        table.writeTo(out)
        out.writeString(severityLevel)
        out.writeString(alertState)
        out.writeOptionalString(monitorId)
        out.writeOptionalString(alertIndex)
    }
}
