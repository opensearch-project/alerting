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
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class DocLevelQuery(
    val id: String = NO_ID,
    val query: String,
    val severity: String,
    val tags: List<String> = mutableListOf()
) : Writeable, ToXContentObject {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(), // id
        sin.readString(), // query
        sin.readString(), // severity
        sin.readStringList() // tags
    )

    fun asTemplateArg(): Map<String, Any> {
        return mapOf(
            QUERY_ID_FIELD to id,
            QUERY_FIELD to query,
            SEVERITY_FIELD to severity,
            TAGS_FIELD to tags
        )
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeString(query)
        out.writeString(severity)
        out.writeStringCollection(tags)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(QUERY_ID_FIELD, id)
            .field(QUERY_FIELD, query)
            .field(SEVERITY_FIELD, severity)
            .field(TAGS_FIELD, tags.toTypedArray())
            .endObject()
        return builder
    }

    companion object {
        const val QUERY_ID_FIELD = "id"
        const val QUERY_FIELD = "query"
        const val SEVERITY_FIELD = "severity"
        const val TAGS_FIELD = "tags"

        const val NO_ID = ""

        @JvmStatic @Throws(IOException::class)
        fun parse(xcp: XContentParser): DocLevelQuery {
            var id: String = NO_ID
            lateinit var query: String
            lateinit var severity: String
            val tags: MutableList<String> = mutableListOf()

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    QUERY_ID_FIELD -> id = xcp.text()
                    QUERY_FIELD -> query = xcp.text()
                    SEVERITY_FIELD -> severity = xcp.text()
                    TAGS_FIELD -> {
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            tags.add(xcp.text())
                        }
                    }
                }
            }

            return DocLevelQuery(
                id = id,
                query = query,
                severity = severity,
                tags = tags
            )
        }

        @JvmStatic @Throws(IOException::class)
        fun readFrom(sin: StreamInput): DocLevelQuery {
            return DocLevelQuery(sin)
        }
    }
}
