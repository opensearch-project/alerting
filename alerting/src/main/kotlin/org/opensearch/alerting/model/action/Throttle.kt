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

package org.opensearch.alerting.model.action

import org.apache.commons.codec.binary.StringUtils
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import java.io.IOException
import java.time.temporal.ChronoUnit
import java.util.Locale

data class Throttle(
    val value: Int,
    val unit: ChronoUnit
) : Writeable, ToXContentObject {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this (
        sin.readInt(), // value
        sin.readEnum(ChronoUnit::class.java) // unit
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
                .field(VALUE_FIELD, value)
                .field(UNIT_FIELD, unit.name)
                .endObject()
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeInt(value)
        out.writeEnum(unit)
    }

    companion object {
        const val VALUE_FIELD = "value"
        const val UNIT_FIELD = "unit"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Throttle {
            var value: Int = 0
            var unit: ChronoUnit = ChronoUnit.MINUTES // only support MINUTES throttle unit currently

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    UNIT_FIELD -> {
                        val unitString = xcp.text().toUpperCase(Locale.ROOT)
                        require(StringUtils.equals(unitString, ChronoUnit.MINUTES.name), { "Only support MINUTES throttle unit currently" })
                        unit = ChronoUnit.valueOf(unitString)
                    }
                    VALUE_FIELD -> {
                        val currentToken = xcp.currentToken()
                        require(currentToken != XContentParser.Token.VALUE_NULL, { "Throttle value can't be null" })
                        when {
                            currentToken.isValue -> {
                                value = xcp.intValue()
                                require(value > 0, { "Can only set positive throttle period" })
                            }
                            else -> {
                                XContentParserUtils.throwUnknownToken(currentToken, xcp.tokenLocation)
                            }
                        }
                    }

                    else -> {
                        throw IllegalStateException("Unexpected field: $fieldName, while parsing action")
                    }
                }
            }
            return Throttle(value = value, unit = requireNotNull(unit))
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): Throttle {
            return Throttle(sin)
        }
    }
}
