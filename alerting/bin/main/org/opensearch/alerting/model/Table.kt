/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import java.io.IOException

data class Table(
    val sortOrder: String,
    val sortString: String,
    val missing: String?,
    val size: Int,
    val startIndex: Int,
    val searchString: String?
) : Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sortOrder = sin.readString(),
        sortString = sin.readString(),
        missing = sin.readOptionalString(),
        size = sin.readInt(),
        startIndex = sin.readInt(),
        searchString = sin.readOptionalString()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(sortOrder)
        out.writeString(sortString)
        out.writeOptionalString(missing)
        out.writeInt(size)
        out.writeInt(startIndex)
        out.writeOptionalString(searchString)
    }

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): Table {
            return Table(sin)
        }
    }
}
