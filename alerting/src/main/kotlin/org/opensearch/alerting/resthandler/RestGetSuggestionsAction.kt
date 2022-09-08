/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.GetSuggestionsAction
import org.opensearch.alerting.action.GetSuggestionsRequest
import org.opensearch.alerting.model.suggestions.suggestioninputs.util.SuggestionInput
import org.opensearch.alerting.model.suggestions.suggestioninputs.util.SuggestionInputType
import org.opensearch.client.node.NodeClient
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener

private val log = LogManager.getLogger(RestGetSuggestionsAction::class.java)

class RestGetSuggestionsAction : BaseRestHandler() {

    override fun getName(): String = "get_suggestions_action"

    override fun routes(): List<Route> {
        return listOf(
            Route(POST, AlertingPlugin.SUGGESTIONS_BASE_URI) // inline object with specific format is required
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.SUGGESTIONS_BASE_URI}")

        return RestChannelConsumer { channel ->
            var inputType: SuggestionInputType? = null
            var component: String? = null
            var hasInput = false

            val xcp = request.contentParser()
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    SuggestionInput.INPUT_TYPE_FIELD -> {
                        val rawInputType = xcp.text()
                        val allowedInputTypes = SuggestionInputType.values().map { it.value }
                        if (!allowedInputTypes.contains(rawInputType)) {
                            throw IllegalArgumentException("invalid inputType, must be one of $allowedInputTypes")
                        }
                        inputType = SuggestionInputType.enumFromStr(rawInputType)
                    }
                    SuggestionInput.COMPONENT_FIELD -> component = xcp.text()
                    SuggestionInput.INPUT_FIELD -> {
                        ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
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

            val getSuggestionsRequest = GetSuggestionsRequest(inputType, component, newXcp) // newXcp already pointing to beginning of input{} object

            client.execute(GetSuggestionsAction.INSTANCE, getSuggestionsRequest, RestToXContentListener(channel))
        }
    }

    // prepare by making it point to the start of the "input{}" object rather
    // than the start of the entire request body
    private fun prepareXcp(xcp: XContentParser) {
        ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)
        while (xcp.nextToken() != Token.END_OBJECT) {
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
