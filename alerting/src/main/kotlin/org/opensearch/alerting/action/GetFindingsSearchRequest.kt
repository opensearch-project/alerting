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
import org.opensearch.search.fetch.subphase.FetchSourceContext
import java.io.IOException

class GetFindingsSearchRequest : ActionRequest {
    val findingId: String?
    val version: Long
    val srcContext: FetchSourceContext?
    val table: Table

    constructor(
        findingId: String?,
        version: Long,
        srcContext: FetchSourceContext?,
        table: Table
    ) : super() {
        this.findingId = findingId
        this.version = version
        this.srcContext = srcContext
        this.table = table
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        findingId = sin.readOptionalString(),
        version = sin.readLong(),
        srcContext = if (sin.readBoolean()) {
            FetchSourceContext(sin)
        } else null,
        table = Table.readFrom(sin)
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(findingId)
        out.writeLong(version)
        out.writeBoolean(srcContext != null)
        srcContext?.writeTo(out)
        table.writeTo(out)
    }
}
