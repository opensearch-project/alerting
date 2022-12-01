/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.suggestions.suggestioninputs.util

import org.opensearch.action.ActionListener
import org.opensearch.alerting.transport.TransportGetSuggestionsAction
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.XContentParser

/**
 * I: the data type of the single value passed in as input in the req body (for example, if the user passes in a monitorId, I is String)
 * T: the data type of the object the user is ultimately seeking suggestions for (for example, Monitor)
 */
interface SuggestionInput<I, out T> : Writeable {

    var rawInput: I
    var async: Boolean

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
     * Throw an error if the input format was invalid in some way
     *
     * Ways of input being invalid:
     * - input object contains no fields
     * - input object contains more than 1 field (input is expected to contain exactly 1 field with the same name
     *   as the value of inputType)
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
     * callback: use to relay information back to the caller of your SuggestionInput
     * you have access to the following functions:
     *
     * onGetResponse(obj: Any): Only use if your implementation involves async object retrieval
     * (for example, retrieving a Monitor from an index). Provide your result object
     * by calling this onGetResponse() rather than returning it.
     *
     * onFailure(e: Exception): Use to relay/throw errors cleanly
     *
     * What It Does:
     * Uses the non-null value of rawInput to retrieve the object
     * of type T
     *
     * If this involves async object retrieval (this.async == true), make use of the given client
     * and XContentRegistry, and provide the object by using the given callback's onGetResposne() rather
     * than returning synchronously out of the function
     *
     * If this does not involve async object retrieval (this.async == false), do not use any of the given
     * params (as they will all be null), and provide the object by returning it out of the function
     *
     * Must Throw:
     * Use the given ActionListener.onFailure() to throw:
     *
     * IllegalStateException if rawInput is null. parseInput() must
     * be called first to make rawInput non-null before getObject()
     * is called
     *
     * IllegalStateException if the input requires async object
     * retrieval (this.async == true), and at least one of the params
     * with default values are null
     *
     * Returns:
     * null: if async object retrieval was involved
     * - OR -
     * T: if async object retrieval was not involved
     *
     * for example:
     *
     * if rawInput is a String monitorId, async Monitor retrieval from an index is involved, so null is returned,
     * and the object is instead provided in the given callback
     *
     * if rawInput is itself a Monitor, async Monitor retrieval is not required, so return rawInput: Monitor itself
     */
    fun <S : Any> getObject(callback: SuggestionsObjectListener, transport: TransportGetSuggestionsAction, actionListener: ActionListener<S>): T?

    /**
     * Implementations of this interface must also include a companion object
     * that implements the SuggestionInputCompanion interface (also in this package)
     */

    companion object {
        const val INPUT_TYPE_FIELD = "inputType"
        const val COMPONENT_FIELD = "component"
        const val INPUT_FIELD = "input"
    }
}
