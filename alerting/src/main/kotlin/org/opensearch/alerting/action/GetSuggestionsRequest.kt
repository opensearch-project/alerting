/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.alerting.model.suggestions.suggestioninputs.util.SuggestionInput
import org.opensearch.alerting.model.suggestions.suggestioninputs.util.SuggestionInputFactory
import org.opensearch.alerting.model.suggestions.suggestioninputs.util.SuggestionInputType
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.XContentParser
import java.io.IOException

class GetSuggestionsRequest : ActionRequest {
    private val inputType: SuggestionInputType
    val component: String
    val input: SuggestionInput<*, Any>

    constructor(
        inputType: SuggestionInputType,
        component: String,
        xcp: XContentParser
    ) : super() {
        this.inputType = inputType
        this.component = component
        this.input = SuggestionInputFactory.getInput(this.inputType, xcp)
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super() {
        this.inputType = sin.readEnum(SuggestionInputType::class.java) // inputType
        this.component = sin.readString() // component
        this.input = SuggestionInputFactory.getInput(this.inputType, sin)
    }

    override fun writeTo(out: StreamOutput) {
        out.writeEnum(inputType)
        out.writeString(component)
        input.writeTo(out)
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }
}
