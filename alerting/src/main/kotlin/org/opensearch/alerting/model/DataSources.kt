/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.core.model.ScheduledJob
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
    /** Configures a custom query index name for the monitor. Creates a new index if index with given name not present.*/
    val queryIndex: String = ScheduledJob.DOC_LEVEL_QUERIES_INDEX,

    /** Configures a custom index to store findings for a monitor. Creates a new index if index with given name not present.
     *  If index is pre-existing, mapping is updated*/
    val findingsIndex: String = AlertIndices.FINDING_HISTORY_WRITE_INDEX,

    /** Configures a custom index to store alerts for a monitor. Creates a new index if index with given name not present.
     *  If index is pre-existing, mapping is updated. */
    val alertsIndex: String = AlertIndices.ALERT_INDEX,

    /** Configures custom mappings by field type for query index.
     * Custom query index mappings are configurable, only if a custom query index is configured too. */
    val queryIndexMappingsByType: Map<String, Map<String, String>> = mapOf()

) : Writeable, ToXContentObject {

    init {
        require(queryIndex.isNotEmpty()) {
            "Query index cannot be empty"
        }
        require(findingsIndex.isNotEmpty()) {
            "Findings index cannot be empty"
        }
        require(alertsIndex.isNotEmpty()) {
            "Alerts index cannot be empty"
        }
        if (queryIndexMappingsByType.isNotEmpty()) {
            require(queryIndex != ScheduledJob.DOC_LEVEL_QUERIES_INDEX) {
                "Custom query index mappings are configurable only if a custom query index is configured too."
            }
            require(
                queryIndexMappingsByType.size == 1 &&
                    queryIndexMappingsByType.containsKey("text") &&
                    queryIndexMappingsByType.get("text")?.size == 1 &&
                    queryIndexMappingsByType.get("text")!!.containsKey("analyzer")
            ) {
                "Custom query index mappings are currently configurable only for 'text' fields and mapping parameter can only be 'analyzer'"
            }
        }
    }

    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    constructor(sin: StreamInput) : this(
        queryIndex = sin.readString(),
        findingsIndex = sin.readString(),
        alertsIndex = sin.readString(),
        queryIndexMappingsByType = sin.readMap() as Map<String, Map<String, String>>
    )

    @Suppress("UNCHECKED_CAST")
    fun asTemplateArg(): Map<String, Any> {
        return mapOf(
            QUERY_INDEX_FIELD to queryIndex,
            FINDINGS_INDEX_FIELD to findingsIndex,
            ALERTS_INDEX_FIELD to alertsIndex,
            QUERY_INDEX_MAPPINGS_BY_TYPE to queryIndexMappingsByType
        ) as Map<String, Any>
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field(QUERY_INDEX_FIELD, queryIndex)
        builder.field(FINDINGS_INDEX_FIELD, findingsIndex)
        builder.field(ALERTS_INDEX_FIELD, alertsIndex)
        builder.field(QUERY_INDEX_MAPPINGS_BY_TYPE, queryIndexMappingsByType as Map<String, Any>)
        builder.endObject()
        return builder
    }

    companion object {
        const val QUERY_INDEX_FIELD = "query_index"
        const val FINDINGS_INDEX_FIELD = "findings_index"
        const val ALERTS_INDEX_FIELD = "alerts_index"
        const val QUERY_INDEX_MAPPINGS_BY_TYPE = "query_index_mappings_by_type"

        @JvmStatic
        @Throws(IOException::class)
        @Suppress("UNCHECKED_CAST")
        fun parse(xcp: XContentParser): DataSources {
            var queryIndex = ""
            var findingsIndex = ""
            var alertsIndex = ""
            var queryIndexMappingsByType: Map<String, Map<String, String>> = mapOf()

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    QUERY_INDEX_FIELD -> queryIndex = xcp.text()
                    FINDINGS_INDEX_FIELD -> findingsIndex = xcp.text()
                    ALERTS_INDEX_FIELD -> alertsIndex = xcp.text()
                    QUERY_INDEX_MAPPINGS_BY_TYPE -> queryIndexMappingsByType = xcp.map() as Map<String, Map<String, String>>
                }
            }
            return DataSources(
                queryIndex = queryIndex,
                findingsIndex = findingsIndex,
                alertsIndex = alertsIndex,
                queryIndexMappingsByType = queryIndexMappingsByType
            )
        }
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(queryIndex)
        out.writeString(findingsIndex)
        out.writeString(alertsIndex)
        out.writeMap(queryIndexMappingsByType as Map<String, Any>)
    }
}
