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
import org.opensearch.index.query.BoolQueryBuilder
import java.io.IOException

class GetAlertsV2Request : ActionRequest {
    val table: Table
    val severityLevel: String
    val monitorV2Id: String?
    val monitorV2Ids: List<String>?
    val alertV2Ids: List<String>?
    val boolQueryBuilder: BoolQueryBuilder?

    constructor(
        table: Table,
        severityLevel: String,
        monitorV2Id: String?,
        monitorV2Ids: List<String>? = null,
        alertV2Ids: List<String>? = null,
        boolQueryBuilder: BoolQueryBuilder? = null
    ) : super() {
        this.table = table
        this.severityLevel = severityLevel
        this.monitorV2Id = monitorV2Id
        this.monitorV2Ids = monitorV2Ids
        this.alertV2Ids = alertV2Ids
        this.boolQueryBuilder = boolQueryBuilder
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        table = Table.readFrom(sin),
        severityLevel = sin.readString(),
        monitorV2Id = sin.readOptionalString(),
        monitorV2Ids = sin.readOptionalStringList(),
        alertV2Ids = sin.readOptionalStringList(),
        boolQueryBuilder = if (sin.readOptionalBoolean() == true) BoolQueryBuilder(sin) else null
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        table.writeTo(out)
        out.writeString(severityLevel)
        out.writeOptionalString(monitorV2Id)
        out.writeOptionalStringCollection(monitorV2Ids)
        out.writeOptionalStringCollection(alertV2Ids)
        if (boolQueryBuilder != null) {
            out.writeOptionalBoolean(true)
            boolQueryBuilder.writeTo(out)
        } else {
            out.writeOptionalBoolean(false)
        }
    }
}
