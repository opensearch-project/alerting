/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.destination

import org.opensearch.common.Strings
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import java.io.IOException
import java.lang.IllegalStateException

/**
 * A value object that represents a Chime message. Chime message will be
 * submitted to the Chime destination
 */
data class Chime(val url: String) : ToXContent {

    init {
        require(!Strings.isNullOrEmpty(url)) { "URL is null or empty" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject(TYPE)
            .field(URL, url)
            .endObject()
    }

    @Throws(IOException::class)
    fun writeTo(out: StreamOutput) {
        out.writeString(url)
    }

    companion object {
        const val URL = "url"
        const val TYPE = "chime"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Chime {
            lateinit var url: String

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    URL -> url = xcp.text()
                    else -> {
                        throw IllegalStateException("Unexpected field: $fieldName, while parsing Chime destination")
                    }
                }
            }
            return Chime(url)
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): Chime? {
            return if (sin.readBoolean()) {
                Chime(sin.readString())
            } else null
        }
    }

    // Complete JSON structure is now constructed in the notification plugin
    fun constructMessageContent(subject: String?, message: String): String {
        return if (Strings.isNullOrEmpty(subject)) message else "$subject \n\n $message"
    }
}
