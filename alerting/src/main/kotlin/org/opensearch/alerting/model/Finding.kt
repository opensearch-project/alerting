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
 *   Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException
import java.time.Instant

/**
 * A wrapper of the log event that enriches the event by also including information about the monitor it triggered.
 */
class Finding(
    val id: String = NO_ID,
    val logEvent: Map<String, Any>,
    val monitorId: String,
    val monitorName: String,
    val queryId: String = NO_ID,
    val queryTags: List<String>,
    val severity: String,
    val timestamp: Instant,
    val triggerId: String,
    val triggerName: String
) : Writeable, ToXContent {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        logEvent = suppressWarning(sin.readMap()),
        monitorId = sin.readString(),
        monitorName = sin.readString(),
        queryId = sin.readString(),
        queryTags = sin.readStringList(),
        severity = sin.readString(),
        timestamp = sin.readInstant(),
        triggerId = sin.readString(),
        triggerName = sin.readString()
    )

    fun asTemplateArg(): Map<String, Any?> {
        return mapOf(
            FINDING_ID_FIELD to id,
            LOG_EVENT_FIELD to logEvent,
            MONITOR_ID_FIELD to monitorId,
            MONITOR_NAME_FIELD to monitorName,
            QUERY_ID_FIELD to queryId,
            QUERY_TAGS_FIELD to queryTags,
            SEVERITY_FIELD to severity,
            TIMESTAMP_FIELD to timestamp.toEpochMilli(),
            TRIGGER_ID_FIELD to triggerId,
            TRIGGER_NAME_FIELD to triggerName
        )
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(FINDING_ID_FIELD, id)
            .field(LOG_EVENT_FIELD, logEvent)
            .field(MONITOR_ID_FIELD, monitorId)
            .field(MONITOR_NAME_FIELD, monitorName)
            .field(QUERY_ID_FIELD, queryId)
            .field(QUERY_TAGS_FIELD, queryTags.toTypedArray())
            .field(SEVERITY_FIELD, severity)
            .field(TIMESTAMP_FIELD, timestamp)
            .field(TRIGGER_ID_FIELD, triggerId)
            .field(TRIGGER_NAME_FIELD, triggerName)
        builder.endObject()
        return builder
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeMap(logEvent)
        out.writeString(monitorId)
        out.writeString(monitorName)
        out.writeString(queryId)
        out.writeStringCollection(queryTags)
        out.writeString(severity)
        out.writeInstant(timestamp)
        out.writeString(triggerId)
        out.writeString(triggerName)
    }

    companion object {
        const val FINDING_ID_FIELD = "id"
        const val LOG_EVENT_FIELD = "log_event"
        const val MONITOR_ID_FIELD = "monitor_id"
        const val MONITOR_NAME_FIELD = "monitor_name"
        const val QUERY_ID_FIELD = "query_id"
        const val QUERY_TAGS_FIELD = "query_tags"
        const val SEVERITY_FIELD = "severity"
        const val TIMESTAMP_FIELD = "timestamp"
        const val TRIGGER_ID_FIELD = "trigger_id"
        const val TRIGGER_NAME_FIELD = "trigger_name"
        const val NO_ID = ""

        @JvmStatic @JvmOverloads
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, id: String = NO_ID): Finding {
            var logEvent: Map<String, Any> = mapOf()
            lateinit var monitorId: String
            lateinit var monitorName: String
            var queryId: String = NO_ID
            val queryTags: MutableList<String> = mutableListOf()
            lateinit var severity: String
            lateinit var timestamp: Instant
            lateinit var triggerId: String
            lateinit var triggerName: String

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    LOG_EVENT_FIELD -> logEvent = xcp.map()
                    MONITOR_ID_FIELD -> monitorId = xcp.text()
                    MONITOR_NAME_FIELD -> monitorName = xcp.text()
                    QUERY_ID_FIELD -> queryId = xcp.text()
                    QUERY_TAGS_FIELD -> {
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            queryTags.add(xcp.text())
                        }
                    }
                    SEVERITY_FIELD -> severity = xcp.text()
                    TIMESTAMP_FIELD -> timestamp = requireNotNull(xcp.instant())
                    TRIGGER_ID_FIELD -> triggerId = xcp.text()
                    TRIGGER_NAME_FIELD -> triggerName = xcp.text()
                }
            }

            return Finding(
                id = id,
                logEvent = logEvent,
                monitorId = monitorId,
                monitorName = monitorName,
                queryId = queryId,
                queryTags = queryTags,
                severity = severity,
                timestamp = timestamp,
                triggerId = triggerId,
                triggerName = triggerName
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): Finding {
            return Finding(sin)
        }

        @Suppress("UNCHECKED_CAST")
        fun suppressWarning(map: MutableMap<String?, Any?>?): MutableMap<String, Any> {
            return map as MutableMap<String, Any>
        }
    }
}
