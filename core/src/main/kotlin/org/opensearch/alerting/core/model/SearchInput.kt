/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.model

import org.opensearch.common.CheckedFunction
import org.opensearch.common.ParseField
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.search.builder.SearchSourceBuilder
import java.io.IOException

data class SearchInput(val indices: List<String>, val query: SearchSourceBuilder) : Input {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readStringList(), // indices
        SearchSourceBuilder(sin) // query
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startObject(SEARCH_FIELD)
            .field(INDICES_FIELD, indices.toTypedArray())
            .field(QUERY_FIELD, query)
            .endObject()
            .endObject()
    }

    override fun name(): String {
        return SEARCH_FIELD
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indices)
        query.writeTo(out)
    }

    companion object {
        const val INDICES_FIELD = "indices"
        const val QUERY_FIELD = "query"
        const val SEARCH_FIELD = "search"

        val XCONTENT_REGISTRY = NamedXContentRegistry.Entry(Input::class.java, ParseField("search"), CheckedFunction { parseInner(it) })

        @JvmStatic @Throws(IOException::class)
        fun parseInner(xcp: XContentParser): SearchInput {
            val indices = mutableListOf<String>()
            lateinit var searchSourceBuilder: SearchSourceBuilder

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    INDICES_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            indices.add(xcp.text())
                        }
                    }
                    QUERY_FIELD -> {
                        searchSourceBuilder = SearchSourceBuilder.fromXContent(xcp, false)
                    }
                }
            }

            return SearchInput(
                indices,
                requireNotNull(searchSourceBuilder) { "SearchInput query is null" }
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): SearchInput {
            return SearchInput(sin)
        }
    }
}
