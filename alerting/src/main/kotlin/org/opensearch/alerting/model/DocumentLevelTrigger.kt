/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.model.Trigger.Companion.ACTIONS_FIELD
import org.opensearch.alerting.model.Trigger.Companion.ID_FIELD
import org.opensearch.alerting.model.Trigger.Companion.NAME_FIELD
import org.opensearch.alerting.model.Trigger.Companion.SEVERITY_FIELD
import org.opensearch.alerting.model.action.Action
import org.opensearch.common.CheckedFunction
import org.opensearch.common.ParseField
import org.opensearch.common.UUIDs
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.script.Script
import java.io.IOException

/**
 * A single-alert Trigger that uses Painless scripts which execute on the response of the Monitor input query to define
 * alerting conditions.
 */
data class DocumentLevelTrigger(
    override val id: String = UUIDs.base64UUID(),
    override val name: String,
    override val severity: String,
    override val actions: List<Action>,
    val condition: Script
) : Trigger {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(), // id
        sin.readString(), // name
        sin.readString(), // severity
        sin.readList(::Action), // actions
        Script(sin)
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .startObject(DOCUMENT_LEVEL_TRIGGER_FIELD)
            .field(ID_FIELD, id)
            .field(NAME_FIELD, name)
            .field(SEVERITY_FIELD, severity)
            .startObject(CONDITION_FIELD)
            .field(SCRIPT_FIELD, condition)
            .endObject()
            .field(ACTIONS_FIELD, actions.toTypedArray())
            .endObject()
            .endObject()
        return builder
    }

    override fun name(): String {
        return DOCUMENT_LEVEL_TRIGGER_FIELD
    }

    /** Returns a representation of the trigger suitable for passing into painless and mustache scripts. */
    fun asTemplateArg(): Map<String, Any> {
        return mapOf(
            ID_FIELD to id,
            NAME_FIELD to name,
            SEVERITY_FIELD to severity,
            ACTIONS_FIELD to actions.map { it.asTemplateArg() }
        )
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeString(name)
        out.writeString(severity)
        out.writeCollection(actions)
        condition.writeTo(out)
    }

    companion object {
        const val DOCUMENT_LEVEL_TRIGGER_FIELD = "document_level_trigger"
        const val CONDITION_FIELD = "condition"
        const val SCRIPT_FIELD = "script"
        const val QUERY_IDS_FIELD = "query_ids"

        val XCONTENT_REGISTRY = NamedXContentRegistry.Entry(
            Trigger::class.java, ParseField(DOCUMENT_LEVEL_TRIGGER_FIELD),
            CheckedFunction { parseInner(it) }
        )

        @JvmStatic @Throws(IOException::class)
        fun parseInner(xcp: XContentParser): DocumentLevelTrigger {
            var id = UUIDs.base64UUID() // assign a default triggerId if one is not specified
            lateinit var name: String
            lateinit var severity: String
            lateinit var condition: Script
            val queryIds: MutableList<String> = mutableListOf()
            val actions: MutableList<Action> = mutableListOf()

            if (xcp.currentToken() != Token.START_OBJECT && xcp.currentToken() != Token.FIELD_NAME) {
                XContentParserUtils.throwUnknownToken(xcp.currentToken(), xcp.tokenLocation)
            }

            // If the parser began on START_OBJECT, move to the next token so that the while loop enters on
            // the fieldName (or END_OBJECT if it's empty).
            if (xcp.currentToken() == Token.START_OBJECT) xcp.nextToken()

            while (xcp.currentToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()

                xcp.nextToken()
                when (fieldName) {
                    ID_FIELD -> id = xcp.text()
                    NAME_FIELD -> name = xcp.text()
                    SEVERITY_FIELD -> severity = xcp.text()
                    CONDITION_FIELD -> {
                        xcp.nextToken()
                        condition = Script.parse(xcp)
                        require(condition.lang == Script.DEFAULT_SCRIPT_LANG) {
                            "Invalid script language. Allowed languages are [${Script.DEFAULT_SCRIPT_LANG}]"
                        }
                        xcp.nextToken()
                    }
                    QUERY_IDS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            queryIds.add(xcp.text())
                        }
                    }
                    ACTIONS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            actions.add(Action.parse(xcp))
                        }
                    }
                }
                xcp.nextToken()
            }

            return DocumentLevelTrigger(
                name = requireNotNull(name) { "Trigger name is null" },
                severity = requireNotNull(severity) { "Trigger severity is null" },
                condition = requireNotNull(condition) { "Trigger condition is null" },
                actions = requireNotNull(actions) { "Trigger actions are null" },
                id = requireNotNull(id) { "Trigger id is null." }
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): DocumentLevelTrigger {
            return DocumentLevelTrigger(sin)
        }
    }
}
