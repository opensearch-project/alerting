/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.suggestions.suggestioninputs

import org.opensearch.action.ActionListener
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.suggestions.suggestioninputs.util.SuggestionInput
import org.opensearch.alerting.model.suggestions.suggestioninputs.util.SuggestionInputCompanion
import org.opensearch.alerting.model.suggestions.suggestioninputs.util.SuggestionsObjectListener
import org.opensearch.alerting.transport.TransportGetSuggestionsAction
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken

class MonitorObjInput() : SuggestionInput<Monitor, Monitor> {

    override lateinit var rawInput: Monitor
    override var async = false

    constructor(sin: StreamInput) : this() {
        rawInput = Monitor.readFrom(sin)
        async = sin.readBoolean()
    }

    override fun parseInput(xcp: XContentParser) {
        // parse input for monitor id
        ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp) // start of input {} block
        ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp) // name field, should be "monitorObj"
        if (xcp.currentName() != MONITOR_OBJ_FIELD) {
            throw IllegalArgumentException("for inputType = monitorObj, input must contain exactly one field named \"monitorObj\" that stores a valid monitor object")
        }
        ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp) // the value stored in the monitorObj field, the monitor obj itself
        val monitor: Monitor = Monitor.parse(xcp)

        this.rawInput = monitor

        ensureExpectedToken(Token.END_OBJECT, xcp.currentToken(), xcp) // that should be the only field in the object
    }

    override fun <S: Any> getObject(callback: SuggestionsObjectListener, transport: TransportGetSuggestionsAction, actionListener: ActionListener<S>): Monitor {
        return rawInput
    }

    override fun writeTo(out: StreamOutput) {
        rawInput.writeTo(out)
        out.writeBoolean(async)
    }

    companion object : SuggestionInputCompanion<Monitor, Monitor> {
        const val MONITOR_OBJ_FIELD = "monitorObj"

        @JvmStatic
        override fun readFrom(sin: StreamInput): MonitorObjInput {
            return MonitorObjInput(sin)
        }
    }
}
