/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.destination

import org.opensearch.core.common.Strings
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

/**
 * A value object that represents a Slack message. Slack message will be
 * submitted to the Slack destination
 */
data class Slack(
    val url: String,
) : ToXContent {
    init {
        require(!Strings.isNullOrEmpty(url)) { "URL is null or empty" }
    }

    override fun toXContent(
        builder: XContentBuilder,
        params: ToXContent.Params,
    ): XContentBuilder =
        builder
            .startObject(TYPE)
            .field(URL, url)
            .endObject()

    @Throws(IOException::class)
    fun writeTo(out: StreamOutput) {
        out.writeString(url)
    }

    companion object {
        const val URL = "url"
        const val TYPE = "slack"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Slack {
            lateinit var url: String

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    URL -> {
                        url = xcp.text()
                    }

                    else -> {
                        throw IllegalStateException("Unexpected field: $fieldName, while parsing Slack destination")
                    }
                }
            }
            return Slack(url)
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): Slack? =
            if (sin.readBoolean()) {
                Slack(sin.readString())
            } else {
                null
            }
    }

    // Complete JSON structure is now constructed in the notification plugin
    fun constructMessageContent(
        subject: String?,
        message: String,
    ): String = if (Strings.isNullOrEmpty(subject)) message else "$subject \n\n $message"
}
