/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.model

import org.opensearch.alerting.core.model.DocLevelMonitorInput.Companion.DOC_LEVEL_INPUT_FIELD
import org.opensearch.alerting.core.model.SearchInput.Companion.SEARCH_FIELD
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

interface Input : Writeable, ToXContentObject {

    enum class Type(val value: String) {
        DOCUMENT_LEVEL_INPUT(DOC_LEVEL_INPUT_FIELD),
        SEARCH_INPUT(SEARCH_FIELD);

        override fun toString(): String {
            return value
        }
    }

    companion object {

        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Input {
            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp)
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)
            val input = if (xcp.currentName() == Type.SEARCH_INPUT.value) {
                SearchInput.parseInner(xcp)
            } else {
                DocLevelMonitorInput.parse(xcp)
            }
            ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp)
            return input
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): Input {
            return when (val type = sin.readEnum(Input.Type::class.java)) {
                Type.DOCUMENT_LEVEL_INPUT -> DocLevelMonitorInput(sin)
                Type.SEARCH_INPUT -> SearchInput(sin)
                // This shouldn't be reachable but ensuring exhaustiveness as Kotlin warns
                // enum can be null in Java
                else -> throw IllegalStateException("Unexpected input [$type] when reading Trigger")
            }
        }
    }

    fun name(): String
}
