/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.model

import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

interface Input : Writeable, ToXContentObject {
    companion object {

        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Input {
            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp)
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)
            val input = xcp.namedObject(Input::class.java, xcp.currentName(), null)
            ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp)
            return input
        }
    }

    fun name(): String
}
