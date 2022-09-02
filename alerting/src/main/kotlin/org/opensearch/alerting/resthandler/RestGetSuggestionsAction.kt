/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.GetSuggestionsAction
import org.opensearch.alerting.action.GetSuggestionsRequest
import org.opensearch.alerting.rules.inputs.util.SuggestionInput
import org.opensearch.alerting.rules.inputs.util.SuggestionInputType
import org.opensearch.client.node.NodeClient
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
                    SuggestionInput.INPUT_TYPE_FIELD -> inputType = SuggestionInputType.enumFromStr(xcp.text())
                    SuggestionInput.COMPONENT_FIELD -> component = xcp.text()
                    SuggestionInput.INPUT_FIELD -> {
                        hasInput = true
                        break
                    }
                    else -> throw IllegalArgumentException("request body must contain only input, inputType, and component fields")
                }
            }

            if (inputType == null || component == null || !hasInput) {
                throw IllegalArgumentException("request body must contain input, inputType, and component fields")
            }

            val getSuggestionsRequest = GetSuggestionsRequest(inputType, component, xcp) // xcp already pointing to beginning of input{} object

            client.execute(GetSuggestionsAction.INSTANCE, getSuggestionsRequest, RestToXContentListener(channel))
        }
    }

//    private fun validateInputs(request: RestRequest) {
//        if (!request.hasParam(SuggestionInput.INPUT_FIELD) || !request.hasParam(SuggestionInput.INPUT_TYPE_FIELD) || !request.hasParam(SuggestionInput.COMPONENT_FIELD)) {
//            throw IllegalArgumentException(
//                "request body must contain input, inputType, and component fields, ${request.hasParam(SuggestionInput.INPUT_FIELD)}," +
//                    "${request.hasParam(SuggestionInput.INPUT_TYPE_FIELD)}, ${request.hasParam(SuggestionInput.COMPONENT_FIELD)}"
//            )
//        }
//
//        val inputType = request.param(SuggestionInput.INPUT_TYPE_FIELD)
//        val allowedInputTypes = SuggestionInputType.values().map { it.value }
//        if (!allowedInputTypes.contains(inputType)) {
//            throw IllegalArgumentException("invalid input type, must be one of $allowedInputTypes")
//        }
//    }
//
//    // prepare by making it point to the start of the "input{}" object rather
//    // than the start of the entire request body
//    private fun prepareXcp(xcp: XContentParser) {
//        ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)
//        while (xcp.nextToken() != Token.END_OBJECT) {
//            val fieldName = xcp.currentName()
//            xcp.nextToken()
//            if (fieldName == SuggestionInput.INPUT_FIELD) {
//                break
//            } else {
//                xcp.skipChildren()
//            }
//        }
//    }
}
