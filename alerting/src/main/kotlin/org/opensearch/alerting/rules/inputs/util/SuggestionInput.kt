/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.rules.inputs.util

import org.opensearch.client.Client
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentParser

/**
 * I: the data type of the single value passed in as input in the req body (for example, if the user passes in a monitorId, I is String)
 * T: the data type of the object the user is ultimately seeking suggestions for (for example, Monitor)
 */
interface SuggestionInput<I, out T> : Writeable {

    var rawInput: I?

    /**
     * Parameters:
     * xcp: an XContentParser pointing to the beginning of the "input" field
     * object (ie the "input" field's Token.START_OBJECT)
     *
     * This function parses the given xcp for a specific field inside the
     * input.
     *
     * Requirements:
     * This function must either populate the rawInput field above with the value
     * it found
     * -- OR --
     * Throw an error if the input was invalid in some way
     *
     * Must only be called in REST handler
     */
    fun parseInput(xcp: XContentParser)

    /**
     * Parameters:
     * client: a Client that can be used to retrieve an object from
     * an index if necessary
     *
     * xContentRegistry: a NamedXContentRegistry that can be used to create
     * parsers (for example, a parser for a GetResponse from an index)
     *
     * Throws:
     * IllegalStateException if rawInput is null. parseInput() must
     * be called first to make rawInput non-null before getObject()
     * is called
     *
     * Uses the non-null value of rawInput to retrieve the object
     * of type T, making use of the given client and XContentRegistry
     * if necessary
     *
     * Returns:
     * the object of type T that the value of rawInput describes
     * for example:
     * if rawInput is a String monitorId, return the Monitor object that has that id
     * if rawInput is itself a Monitor, return that very Monitor
     */
    fun getObject(client: Client, xContentRegistry: NamedXContentRegistry): T

    /**
     * Implementations of this interface must also include a companion object
     * that implements:
     *
     * fun readFrom(sin: StreamInput): T
     */

    companion object {
        const val INPUT_TYPE_FIELD = "inputType"
        const val COMPONENT_FIELD = "component"
        const val INPUT_FIELD = "input"
    }
}
