/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.core.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.alerting.model.action.Action
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

interface Trigger : Writeable, ToXContentObject {

    enum class Type(val value: String) {
        DOCUMENT_LEVEL_TRIGGER(DocumentLevelTrigger.DOCUMENT_LEVEL_TRIGGER_FIELD),
        QUERY_LEVEL_TRIGGER(QueryLevelTrigger.QUERY_LEVEL_TRIGGER_FIELD),
        BUCKET_LEVEL_TRIGGER(BucketLevelTrigger.BUCKET_LEVEL_TRIGGER_FIELD);

        override fun toString(): String {
            return value
        }
    }

    companion object {
        const val ID_FIELD = "id"
        const val NAME_FIELD = "name"
        const val SEVERITY_FIELD = "severity"
        const val ACTIONS_FIELD = "actions"

        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Trigger {
            val trigger: Trigger

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp)
            val triggerTypeNames = Type.values().map { it.toString() }
            if (triggerTypeNames.contains(xcp.currentName())) {
                ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)
                trigger = xcp.namedObject(Trigger::class.java, xcp.currentName(), null)
                ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp)
            } else {
                // Infer the old Trigger (now called QueryLevelTrigger) when it is not defined as a named
                // object to remain backwards compatible when parsing the old format
                trigger = QueryLevelTrigger.parseInner(xcp)
                ensureExpectedToken(Token.END_OBJECT, xcp.currentToken(), xcp)
            }
            return trigger
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): Trigger {
            return when (val type = sin.readEnum(Trigger.Type::class.java)) {
                Type.QUERY_LEVEL_TRIGGER -> QueryLevelTrigger(sin)
                Type.BUCKET_LEVEL_TRIGGER -> BucketLevelTrigger(sin)
                Type.DOCUMENT_LEVEL_TRIGGER -> DocumentLevelTrigger(sin)
                // This shouldn't be reachable but ensuring exhaustiveness as Kotlin warns
                // enum can be null in Java
                else -> throw IllegalStateException("Unexpected input [$type] when reading Trigger")
            }
        }
    }

    /** The id of the Trigger in the [SCHEDULED_JOBS_INDEX] */
    val id: String

    /** The name of the Trigger */
    val name: String

    /** The severity of the Trigger, used to classify the subsequent Alert */
    val severity: String

    /** The actions executed if the Trigger condition evaluates to true */
    val actions: List<Action>

    fun name(): String

    fun asTemplateArg(): Map<String, Any?>
}
