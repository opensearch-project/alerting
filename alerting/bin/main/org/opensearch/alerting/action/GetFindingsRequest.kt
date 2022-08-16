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

class GetFindingsRequest : ActionRequest {
    val findingId: String?
    val table: Table

    constructor(
        findingId: String?,
        table: Table
    ) : super() {
        this.findingId = findingId
        this.table = table
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        findingId = sin.readOptionalString(),
        table = Table.readFrom(sin)
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(findingId)
        table.writeTo(out)
    }
}
