/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import java.io.IOException

data class DataSources(
    val queryIndex: String? = null,
    val queryIndexMapping: String? = null,
    val findingsIndex: String? = null,
    val alertsIndex: String? = null,
) : Writeable, ToXContentObject {
    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readOptionalString(), // queryIndex
        sin.readOptionalString(), // queryIndexMapping
        sin.readOptionalString(), // findingsIndex
        sin.readOptionalString(), // alertIndex
    )

    fun asTemplateArg(): Map<String, Any> {
        return mapOf(
            QUERY_INDEX_FIELD to queryIndex,
            QUERY_INDEX_MAPPING_FIELD to queryIndexMapping,
            FINDINGS_INDEX_FIELD to findingsIndex,
            ALERTS_INDEX_FIELD to alertsIndex
        ) as Map<String, Any>
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (queryIndex != null) {
            builder.field(QUERY_INDEX_FIELD, queryIndex)
        }
        if (queryIndexMapping != null) {
            builder.field(QUERY_INDEX_MAPPING_FIELD, queryIndexMapping)
        }
        if (findingsIndex != null) {
            builder.field(FINDINGS_INDEX_FIELD, findingsIndex)
        }
        if (alertsIndex != null) {
            builder.field(ALERTS_INDEX_FIELD, alertsIndex)
        }
        builder.endObject()
        return builder
    }

    companion object {
        const val QUERY_INDEX_FIELD = "query_index"
        const val QUERY_INDEX_MAPPING_FIELD = "query_index_mapping"
        const val FINDINGS_INDEX_FIELD = "findings_index"
        const val ALERTS_INDEX_FIELD = "alerts_index"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): DataSources {
            var queryIndex: String? = null
            var queryIndexMapping: String? = null
            var findingsIndex: String? = null
            var alertsIndex: String? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    QUERY_INDEX_FIELD -> queryIndex = xcp.text()
                    QUERY_INDEX_MAPPING_FIELD -> queryIndexMapping = xcp.text()
                    FINDINGS_INDEX_FIELD -> findingsIndex = xcp.text()
                    ALERTS_INDEX_FIELD -> alertsIndex = xcp.text()
                }
            }
            return DataSources(
                queryIndex = queryIndex,
                queryIndexMapping = queryIndexMapping,
                findingsIndex = findingsIndex,
                alertsIndex = alertsIndex
            )
        }
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(queryIndex)
        out.writeOptionalString(queryIndexMapping)
        out.writeOptionalString(findingsIndex)
        out.writeOptionalString(alertsIndex)
    }
}
