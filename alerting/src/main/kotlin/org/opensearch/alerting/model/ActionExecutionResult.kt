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

package org.opensearch.alerting.model

import org.opensearch.alerting.elasticapi.instant
import org.opensearch.alerting.elasticapi.optionalTimeField
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import java.io.IOException
import java.time.Instant

/**
 * When an alert triggered, the trigger's actions will be executed.
 * Action execution result records action throttle result and is a part of Alert.
 */
data class ActionExecutionResult(
    val actionId: String,
    val lastExecutionTime: Instant?,
    val throttledCount: Int = 0
) : Writeable, ToXContentObject {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(), // actionId
        sin.readOptionalInstant(), // lastExecutionTime
        sin.readInt() // throttledCount
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(ACTION_ID_FIELD, actionId)
            .optionalTimeField(LAST_EXECUTION_TIME_FIELD, lastExecutionTime)
            .field(THROTTLED_COUNT_FIELD, throttledCount)
            .endObject()
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(actionId)
        out.writeOptionalInstant(lastExecutionTime)
        out.writeInt(throttledCount)
    }

    companion object {
        const val ACTION_ID_FIELD = "action_id"
        const val LAST_EXECUTION_TIME_FIELD = "last_execution_time"
        const val THROTTLED_COUNT_FIELD = "throttled_count"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ActionExecutionResult {
            lateinit var actionId: String
            var throttledCount: Int = 0
            var lastExecutionTime: Instant? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    ACTION_ID_FIELD -> actionId = xcp.text()
                    THROTTLED_COUNT_FIELD -> throttledCount = xcp.intValue()
                    LAST_EXECUTION_TIME_FIELD -> lastExecutionTime = xcp.instant()

                    else -> {
                        throw IllegalStateException("Unexpected field: $fieldName, while parsing action")
                    }
                }
            }

            requireNotNull(actionId) { "Must set action id" }
            return ActionExecutionResult(actionId, lastExecutionTime, throttledCount)
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): ActionExecutionResult {
            return ActionExecutionResult(sin)
        }
    }
}
