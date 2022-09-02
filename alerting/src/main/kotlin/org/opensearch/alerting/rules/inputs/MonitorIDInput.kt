/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.rules.inputs

import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.rules.inputs.util.SuggestionInput
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
    var obj: Monitor? = null

    constructor(sin: StreamInput) : this() {
        rawInput = sin.readOptionalString() // TODO: readString() or readOptionalString()?
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
        if (xcp.currentName() != "monitorId") { // TODO: put String vals in constants in companion object
            throw IllegalArgumentException("input must contain exactly one field named \"monitorId\" that stores a valid monitor id")
        }
        xcp.nextToken() // the value stored in the monitorId field, the monitor id itself
        val monitorId: String = xcp.text() // TODO: setting to Monitor.NO_ID is redundant? consider doing it anyway, ie initialize at top and set later

        this.rawInput = monitorId
    }

    override fun getObject(client: Client, xContentRegistry: NamedXContentRegistry): Monitor {
        // check to ensure that parseInput was called first and rawInput is not null
        if (rawInput == null) {
            throw IllegalStateException("input was not parsed to get monitorId, parseInput() must be called first")
        }

        // use monitor id to retrieve Monitor object from Scheduled Jobs Index
        var monitor: Monitor? = null

        val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX).id(this.rawInput)
        client.get(
            getRequest,
            object : ActionListener<GetResponse> {
                override fun onResponse(response: GetResponse) {
                    if (!response.isExists) {
                        throw OpenSearchStatusException("Monitor with ID $rawInput not found", RestStatus.NOT_FOUND)
                    }

                    if (!response.isSourceEmpty) {
                        XContentHelper.createParser(
                            xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                            response.sourceAsBytesRef, XContentType.JSON
                        ).use { xcp ->
                            val receivedMonitor = ScheduledJob.parse(xcp, response.id, response.version) as Monitor
                            monitor = receivedMonitor.copy()
                        }
                    }
                }

                override fun onFailure(e: Exception) {
                    throw IllegalStateException("onFailure: $e, $rawInput, $monitor")
                }
            }
        )

        if (monitor == null) {
            throw IllegalStateException("if monitor not found, should have already failed and never gotten here")
        }

        return monitor as Monitor
    }

    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(rawInput)
    }

    companion object {
        @JvmStatic
        fun readFrom(sin: StreamInput): MonitorIDInput {
            return MonitorIDInput(sin)
        }
    }
}
