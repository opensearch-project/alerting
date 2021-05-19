/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.alerting.alerts

import org.opensearch.alerting.elasticapi.instant
import org.opensearch.alerting.elasticapi.optionalTimeField
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException
import java.time.Instant

data class AlertError(val timestamp: Instant, val message: String) : Writeable, ToXContent {

    @Throws(IOException::class)
    constructor(sin: StreamInput): this(
        sin.readInstant(), // timestamp
        sin.readString() // message
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeInstant(timestamp)
        out.writeString(message)
    }
    companion object {

        const val TIMESTAMP_FIELD = "timestamp"
        const val MESSAGE_FIELD = "message"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): AlertError {

            lateinit var timestamp: Instant
            lateinit var message: String

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    TIMESTAMP_FIELD -> timestamp = requireNotNull(xcp.instant())
                    MESSAGE_FIELD -> message = xcp.text()
                }
            }
            return AlertError(timestamp = timestamp, message = message)
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): AlertError {
            return AlertError(sin)
        }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
                .optionalTimeField(TIMESTAMP_FIELD, timestamp)
                .field(MESSAGE_FIELD, message)
                .endObject()
    }
}
