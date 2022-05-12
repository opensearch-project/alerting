/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.opensearchapi.instant
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import java.io.IOException
import java.time.Instant

data class MonitorMetadata(
    val id: String,
    val monitorId: String,
    val lastActionExecutionTimes: List<ActionExecutionTime>,
    val lastRunContext: Map<String, Any>
) : Writeable, ToXContent {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        monitorId = sin.readString(),
        lastActionExecutionTimes = sin.readList(ActionExecutionTime::readFrom),
        lastRunContext = Monitor.suppressWarning(sin.readMap())
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeString(monitorId)
        out.writeCollection(lastActionExecutionTimes)
        out.writeMap(lastRunContext)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (params.paramAsBoolean("with_type", false)) builder.startObject(METADATA)
        builder.field(MONITOR_ID_FIELD, monitorId)
            .field(LAST_ACTION_EXECUTION_FIELD, lastActionExecutionTimes.toTypedArray())
        if (lastRunContext.isNotEmpty()) builder.field(LAST_RUN_CONTEXT_FIELD, lastRunContext)
        if (params.paramAsBoolean("with_type", false)) builder.endObject()
        return builder.endObject()
    }

    companion object {
        const val METADATA = "metadata"
        const val MONITOR_ID_FIELD = "monitor_id"
        const val LAST_ACTION_EXECUTION_FIELD = "last_action_execution_times"
        const val LAST_RUN_CONTEXT_FIELD = "last_run_context"

        @JvmStatic @JvmOverloads
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): MonitorMetadata {
            lateinit var monitorId: String
            val lastActionExecutionTimes = mutableListOf<ActionExecutionTime>()
            var lastRunContext: Map<String, Any> = mapOf()

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    MONITOR_ID_FIELD -> monitorId = xcp.text()
                    LAST_ACTION_EXECUTION_FIELD -> {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            lastActionExecutionTimes.add(ActionExecutionTime.parse(xcp))
                        }
                    }
                    LAST_RUN_CONTEXT_FIELD -> lastRunContext = xcp.map()
                }
            }

            return MonitorMetadata(
                "$monitorId-metadata",
                monitorId = monitorId,
                lastActionExecutionTimes = lastActionExecutionTimes,
                lastRunContext = lastRunContext
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): MonitorMetadata {
            return MonitorMetadata(sin)
        }
    }
}

/**
 * A value object containing action execution time.
 */
data class ActionExecutionTime(
    val actionId: String,
    val executionTime: Instant
) : Writeable, ToXContent {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(), // actionId
        sin.readInstant(), // executionTime
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(ACTION_ID_FIELD, actionId)
            .field(EXECUTION_TIME_FIELD, executionTime)
            .endObject()
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(actionId)
        out.writeInstant(executionTime)
    }

    companion object {
        const val ACTION_ID_FIELD = "action_id"
        const val EXECUTION_TIME_FIELD = "execution_time"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ActionExecutionTime {
            lateinit var actionId: String
            lateinit var executionTime: Instant

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ACTION_ID_FIELD -> actionId = xcp.text()
                    EXECUTION_TIME_FIELD -> executionTime = xcp.instant()!!
                }
            }

            return ActionExecutionTime(
                actionId,
                executionTime
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): ActionExecutionTime {
            return ActionExecutionTime(sin)
        }
    }
}
