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
import org.opensearch.common.lucene.uid.Versions
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.commons.authuser.User
import java.io.IOException
import java.time.Instant

class Finding(
    val id: String = NO_ID,
    val logEventId: String = NO_ID,
    val monitorId: String,
    val monitorName: String,
    val monitorUser: User?,
    val monitorVersion: Long = NO_VERSION,
    val ruleId: String = NO_ID,
    val ruleTags: List<String>,
    val severity: String,
    val timestamp: Instant,
    val triggerId: String,
    val triggerName: String
) : Writeable, ToXContent {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        logEventId = sin.readString(),
        monitorId = sin.readString(),
        monitorName = sin.readString(),
        monitorUser = if (sin.readBoolean()) User(sin) else null,
        monitorVersion = sin.readLong(),
        ruleId = sin.readString(),
        ruleTags = sin.readStringList(),
        severity = sin.readString(),
        timestamp = sin.readInstant(),
        triggerId = sin.readString(),
        triggerName = sin.readString()
    )

    fun asTemplateArg(): Map<String, Any?> {
        return mapOf(
            FINDING_ID_FIELD to id,
            LOG_EVENT_ID_FIELD to logEventId,
            MONITOR_ID_FIELD to monitorId,
            MONITOR_NAME_FIELD to monitorName,
            MONITOR_VERSION_FIELD to monitorVersion,
            RULE_ID_FIELD to ruleId,
            RULE_TAGS_FIELD to ruleTags,
            SEVERITY_FIELD to severity,
            TIMESTAMP_FIELD to timestamp.toEpochMilli(),
            TRIGGER_ID_FIELD to triggerId,
            TRIGGER_NAME_FIELD to triggerName
        )
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(FINDING_ID_FIELD, id)
            .field(LOG_EVENT_ID_FIELD, logEventId)
            .field(MONITOR_ID_FIELD, monitorId)
            .field(MONITOR_NAME_FIELD, monitorName)
            .field(MONITOR_USER_FIELD, monitorUser)
            .field(MONITOR_VERSION_FIELD, monitorVersion)
            .field(RULE_ID_FIELD, ruleId)
            .field(RULE_TAGS_FIELD, ruleTags.toTypedArray())
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
        out.writeString(logEventId)
        out.writeString(monitorId)
        out.writeString(monitorName)
        monitorUser?.writeTo(out)
        out.writeLong(monitorVersion)
        out.writeString(ruleId)
        out.writeStringCollection(ruleTags)
        out.writeString(severity)
        out.writeInstant(timestamp)
        out.writeString(triggerId)
        out.writeString(triggerName)
    }

    companion object {
        const val FINDING_ID_FIELD = "id"
        const val LOG_EVENT_ID_FIELD = "log_event_id"
        const val MONITOR_ID_FIELD = "monitor_id"
        const val MONITOR_NAME_FIELD = "monitor_name"
        const val MONITOR_USER_FIELD = "monitor_user"
        const val MONITOR_VERSION_FIELD = "monitor_version"
        const val RULE_ID_FIELD = "rule_id"
        const val RULE_TAGS_FIELD = "rule_tags"
        const val SEVERITY_FIELD = "severity"
        const val TIMESTAMP_FIELD = "timestamp"
        const val TRIGGER_ID_FIELD = "trigger_id"
        const val TRIGGER_NAME_FIELD = "trigger_name"
        const val NO_ID = ""
        const val NO_VERSION = Versions.NOT_FOUND

        @JvmStatic @JvmOverloads
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, id: String = NO_ID): Finding {
            var logEventId: String = NO_ID
            lateinit var monitorId: String
            lateinit var monitorName: String
            var monitorUser: User? = null
            var monitorVersion: Long = NO_VERSION
            var ruleId: String = NO_ID
            val ruleTags: MutableList<String> = mutableListOf()
            lateinit var severity: String
            lateinit var timestamp: Instant
            lateinit var triggerId: String
            lateinit var triggerName: String

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    LOG_EVENT_ID_FIELD -> logEventId = xcp.text()
                    MONITOR_ID_FIELD -> monitorId = xcp.text()
                    MONITOR_NAME_FIELD -> monitorName = xcp.text()
                    MONITOR_USER_FIELD -> monitorUser = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else User.parse(xcp)
                    MONITOR_VERSION_FIELD -> monitorVersion = xcp.longValue()
                    RULE_ID_FIELD -> ruleId = xcp.text()
                    RULE_TAGS_FIELD -> {
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                        // TODO dev code: investigate implementing error logging
//                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
//                            errorHistory.add(FindingError.parse(xcp))
//                        }
                    }
                    SEVERITY_FIELD -> severity = xcp.text()
                    TIMESTAMP_FIELD -> timestamp = requireNotNull(xcp.instant())
                    TRIGGER_ID_FIELD -> triggerId = xcp.text()
                    TRIGGER_NAME_FIELD -> triggerName = xcp.text()
                }
            }

            return Finding(
                id = id,
                logEventId = logEventId,
                monitorId = monitorId,
                monitorName = monitorName,
                monitorUser = monitorUser,
                monitorVersion = monitorVersion,
                ruleId = ruleId,
                ruleTags = ruleTags,
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
    }
}
