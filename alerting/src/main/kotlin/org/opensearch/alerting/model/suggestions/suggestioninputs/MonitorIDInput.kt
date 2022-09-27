/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.suggestions.suggestioninputs

import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.suggestions.suggestioninputs.util.SuggestionInput
import org.opensearch.alerting.model.suggestions.suggestioninputs.util.SuggestionInputCompanion
import org.opensearch.alerting.model.suggestions.suggestioninputs.util.SuggestionsObjectListener
import org.opensearch.client.Client
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.common.xcontent.XContentType
import org.opensearch.rest.RestStatus

class MonitorIDInput() : SuggestionInput<String, Monitor> {

    override var rawInput: String? = null
    override var async = true

    constructor(sin: StreamInput) : this() {
        rawInput = sin.readOptionalString()
        async = sin.readBoolean()
    }

    /**
     * User input requirements that will be checked for:
     * input{} must contain exactly one field named "monitorId"
     *
     * whether or not it stores a valid monitor id is deferred until
     * the id is used to query the Scheduled Job Index for the Monitor
     *
     * Error is thrown if any of the above is violated
     */
    override fun parseInput(xcp: XContentParser) {
        // parse input for monitor id
        ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp) // start of input {} block
        ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp) // name field, should be "monitorId"
        if (xcp.currentName() != MONITOR_ID_FIELD) {
            throw IllegalArgumentException("for inputType = monitorId, input must contain exactly one field named \"monitorId\" that stores a valid monitor id")
        }
        ensureExpectedToken(Token.VALUE_STRING, xcp.nextToken(), xcp) // the value stored in the monitorId field, the monitor id itself
        val monitorId: String = xcp.text()

        this.rawInput = monitorId

        ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp) // that should be the only field in the object
    }

    override fun getObject(callback: SuggestionsObjectListener, client: Client?, xContentRegistry: NamedXContentRegistry?): Monitor? {
        // check to ensure that parseInput was called first and rawInput is not null
        if (rawInput == null) {
            throw IllegalStateException("input was not parsed to get monitorId, parseInput() must be called first")
        } else if (client == null || xContentRegistry == null) {
            throw IllegalStateException("if the input requires async object retrieval, callback can't be null)")
        }

        val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX).id(this.rawInput)
        client.get(
            getRequest,
            object : ActionListener<GetResponse> {
                override fun onResponse(response: GetResponse) {
                    if (!response.isExists) {
                        callback.onFailure(OpenSearchStatusException("Monitor with ID $rawInput not found, please ensure the monitor id is valid", RestStatus.NOT_FOUND))
                    }

                    if (!response.isSourceEmpty) {
                        XContentHelper.createParser(
                            xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                            response.sourceAsBytesRef, XContentType.JSON
                        ).use { xcp ->
                            val monitor = ScheduledJob.parse(xcp, response.id, response.version) as Monitor
                            callback.onGetResponse(monitor)
                        }
                    }
                }

                override fun onFailure(e: Exception) {
                    callback.onFailure(e)
                }
            }
        )

        return null
    }

    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(rawInput)
        out.writeBoolean(async)
    }

    companion object : SuggestionInputCompanion<String, Monitor> {
        const val MONITOR_ID_FIELD = "monitorId"

        @JvmStatic
        override fun readFrom(sin: StreamInput): MonitorIDInput {
            return MonitorIDInput(sin)
        }
    }
}
