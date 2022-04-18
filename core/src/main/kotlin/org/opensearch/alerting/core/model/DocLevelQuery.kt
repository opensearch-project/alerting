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
    val name: String,
    val query: String,
    val tags: List<String> = mutableListOf()
) : Writeable, ToXContentObject {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(), // id
        sin.readString(), // name
        sin.readString(), // query
        sin.readStringList() // tags
    )

    fun asTemplateArg(): Map<String, Any> {
        return mapOf(
            QUERY_ID_FIELD to id,
            NAME_FIELD to name,
            QUERY_FIELD to query,
            TAGS_FIELD to tags
        )
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeString(name)
        out.writeString(query)
        out.writeStringCollection(tags)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(QUERY_ID_FIELD, id)
            .field(NAME_FIELD, name)
            .field(QUERY_FIELD, query)
            .field(TAGS_FIELD, tags.toTypedArray())
            .endObject()
        return builder
    }

    companion object {
        const val QUERY_ID_FIELD = "id"
        const val NAME_FIELD = "name"
        const val QUERY_FIELD = "query"
        const val TAGS_FIELD = "tags"

        const val NO_ID = ""

        @JvmStatic @Throws(IOException::class)
        fun parse(xcp: XContentParser): DocLevelQuery {
            var id: String = NO_ID
            lateinit var query: String
            lateinit var name: String
            val tags: MutableList<String> = mutableListOf()

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    QUERY_ID_FIELD -> id = xcp.text()
                    NAME_FIELD -> name = xcp.text()
                    QUERY_FIELD -> query = xcp.text()
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
                name = name,
                query = query,
                tags = tags
            )
        }

        @JvmStatic @Throws(IOException::class)
        fun readFrom(sin: StreamInput): DocLevelQuery {
            return DocLevelQuery(sin)
        }
    }
}
