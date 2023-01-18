/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.destination

import org.opensearch.common.Strings
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException
import java.lang.IllegalStateException

/**
 * A value object that represents a Telegram message. Telegram message will be
 * submitted to the Telegram destination
 */
data class Telegram(val chatId: String, val botToken: String) : ToXContent {

    init {
        require(!Strings.isNullOrEmpty(chatId)) { "Chat ID is null or  empty" }
        require(!Strings.isNullOrEmpty(botToken)) { "Bot token is null or empty" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject(TYPE).field(CHAT_ID, chatId).field(BOT_TOKEN, botToken).endObject()
    }

    @Throws(IOException::class)
    fun writeTo(out: StreamOutput) {
        out.writeString(chatId)
        out.writeString(botToken)
    }

    companion object {
        const val CHAT_ID = "chat_id"
        const val BOT_TOKEN = "bot_token"
        const val TYPE = "telegram"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Telegram {
            lateinit var chatId: String
            lateinit var botToken: String

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    CHAT_ID -> chatId = xcp.text()
                    BOT_TOKEN -> botToken = xcp.text()
                    else -> {
                        throw IllegalStateException("Unexpected field: $fieldName, while parsing Telegram destination")
                    }
                }
            }
            return Telegram(chatId, botToken)
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): Telegram? {
            return if (sin.readBoolean()) {
                Telegram(sin.readString(), sin.readString())
            } else null
        }
    }
}
