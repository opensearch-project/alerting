/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.alerting.elasticapi.instant
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException
import java.time.Instant

/**
 * A wrapper of the log event that enriches the event by also including information about the monitor it triggered.
 */
class Finding(
    val id: String = NO_ID,
    val relatedDocId: String,
    val monitorId: String,
    val monitorName: String,
    val index: String,
    val docLevelQueries: List<DocLevelQuery>,
    val timestamp: Instant,
    val triggerId: String?,
    val triggerName: String?
) : Writeable, ToXContent {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        relatedDocId = sin.readString(),
        monitorId = sin.readString(),
        monitorName = sin.readString(),
        index = sin.readString(),
        docLevelQueries = sin.readList((DocLevelQuery)::readFrom),
        timestamp = sin.readInstant(),
        triggerId = sin.readOptionalString(),
        triggerName = sin.readOptionalString()
    )

    fun asTemplateArg(): Map<String, Any?> {
        return mapOf(
            FINDING_ID_FIELD to id,
            RELATED_DOC_ID_FIELD to relatedDocId,
            MONITOR_ID_FIELD to monitorId,
            MONITOR_NAME_FIELD to monitorName,
            INDEX_FIELD to index,
            QUERIES_FIELD to docLevelQueries,
            TIMESTAMP_FIELD to timestamp.toEpochMilli(),
            TRIGGER_ID_FIELD to triggerId,
            TRIGGER_NAME_FIELD to triggerName
        )
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(FINDING_ID_FIELD, id)
            .field(RELATED_DOC_ID_FIELD, relatedDocId)
            .field(MONITOR_ID_FIELD, monitorId)
            .field(MONITOR_NAME_FIELD, monitorName)
            .field(INDEX_FIELD, index)
            .field(QUERIES_FIELD, docLevelQueries.toTypedArray())
            .field(TIMESTAMP_FIELD, timestamp.toEpochMilli())
            .field(TRIGGER_ID_FIELD, triggerId)
            .field(TRIGGER_NAME_FIELD, triggerName)
        builder.endObject()
        return builder
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeString(relatedDocId)
        out.writeString(monitorId)
        out.writeString(monitorName)
        out.writeString(index)
        out.writeCollection(docLevelQueries)
        out.writeInstant(timestamp)
        out.writeOptionalString(triggerId)
        out.writeOptionalString(triggerName)
    }

    companion object {
        const val FINDING_ID_FIELD = "id"
        const val RELATED_DOC_ID_FIELD = "related_doc_id"
        const val MONITOR_ID_FIELD = "monitor_id"
        const val MONITOR_NAME_FIELD = "monitor_name"
        const val INDEX_FIELD = "index"
        const val QUERIES_FIELD = "queries"
        const val TIMESTAMP_FIELD = "timestamp"
        const val TRIGGER_ID_FIELD = "trigger_id"
        const val TRIGGER_NAME_FIELD = "trigger_name"
        const val NO_ID = ""

        @JvmStatic @JvmOverloads
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Finding {
            var id: String = NO_ID
            lateinit var relatedDocId: String
            lateinit var monitorId: String
            lateinit var monitorName: String
            lateinit var index: String
            val queries: MutableList<DocLevelQuery> = mutableListOf()
            lateinit var timestamp: Instant
            var triggerId: String? = null
            var triggerName: String? = null

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    FINDING_ID_FIELD -> id = xcp.text()
                    RELATED_DOC_ID_FIELD -> relatedDocId = xcp.text()
                    MONITOR_ID_FIELD -> monitorId = xcp.text()
                    MONITOR_NAME_FIELD -> monitorName = xcp.text()
                    INDEX_FIELD -> index = xcp.text()
                    QUERIES_FIELD -> {
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            queries.add(DocLevelQuery.parse(xcp))
                        }
                    }
                    TIMESTAMP_FIELD -> {
                        timestamp = requireNotNull(xcp.instant())
                    }
                    TRIGGER_ID_FIELD -> triggerId = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else xcp.text()
                    TRIGGER_NAME_FIELD -> triggerName = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else xcp.text()
                }
            }

            return Finding(
                id = id,
                relatedDocId = relatedDocId,
                monitorId = monitorId,
                monitorName = monitorName,
                index = index,
                docLevelQueries = queries,
                timestamp = timestamp,
                triggerId = triggerId,
                triggerName = triggerName
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): Finding {
            return Finding(sin)
        }
    }
}
