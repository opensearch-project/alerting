/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.commons.alerting.model.Table
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import java.io.IOException

class GetAlertsV2Request : ActionRequest {
    val table: Table
    val severityLevel: String
    val monitorV2Ids: List<String>?

    constructor(
        table: Table,
        severityLevel: String,
        monitorV2Ids: List<String>? = null,
    ) : super() {
        this.table = table
        this.severityLevel = severityLevel
        this.monitorV2Ids = monitorV2Ids
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        table = Table.readFrom(sin),
        severityLevel = sin.readString(),
        monitorV2Ids = sin.readOptionalStringList(),
    )

    override fun validate(): ActionRequestValidationException? = null

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        table.writeTo(out)
        out.writeString(severityLevel)
        out.writeOptionalStringCollection(monitorV2Ids)
    }
}
