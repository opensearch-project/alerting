/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.alerting.model.suggestions.suggestioninputs.util.SuggestionInput
import org.opensearch.alerting.model.suggestions.suggestioninputs.util.SuggestionInputFactory
import org.opensearch.alerting.model.suggestions.suggestioninputs.util.SuggestionInputType
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.rest.RestRequest
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

    companion object {
        @JvmStatic
        fun parse(request: RestRequest): GetSuggestionsRequest {
            var inputType: SuggestionInputType? = null
            var component: String? = null
            var hasInput = false

            val xcp = request.contentParser()

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    SuggestionInput.INPUT_TYPE_FIELD -> {
                        val rawInputType = xcp.text()
                        val allowedInputTypes = SuggestionInputType.values().map { it.value }
                        if (!allowedInputTypes.contains(rawInputType)) {
                            throw IllegalArgumentException("invalid inputType, must be one of $allowedInputTypes")
                        }
                        try {
                            inputType = SuggestionInputType.enumFromStr(rawInputType)
                        } catch (e: Exception) {
                            throw IllegalArgumentException("invalid inputType, must be one of $allowedInputTypes")
                        }
                    }
                    SuggestionInput.COMPONENT_FIELD -> component = xcp.text()
                    SuggestionInput.INPUT_FIELD -> {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
                        hasInput = true
                        xcp.skipChildren()
                    }
                    else -> throw IllegalArgumentException("request body must contain only input, inputType, and component fields")
                }
            }

            if (inputType == null || component == null || !hasInput) {
                throw IllegalArgumentException("request body must contain input, inputType, and component fields")
            }

            val newXcp = request.contentParser()
            prepareXcp(newXcp)

            return GetSuggestionsRequest(inputType, component, newXcp) // newXcp already pointing to beginning of input{} object
        }

        // prepare by making it point to the start of the "input{}" object rather
        // than the start of the entire request body
        private fun prepareXcp(xcp: XContentParser) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                if (fieldName == SuggestionInput.INPUT_FIELD) {
                    break
                } else {
                    xcp.skipChildren()
                }
            }
        }
    }
}
