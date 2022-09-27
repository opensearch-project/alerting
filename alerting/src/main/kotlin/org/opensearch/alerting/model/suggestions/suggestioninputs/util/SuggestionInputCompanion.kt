/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.suggestions.suggestioninputs.util

import org.opensearch.common.io.stream.StreamInput

/**
 * All implementations of SuggestionInput must include a companion object that implements
 * the following interface, with type parameters I and T MATCHING the type parameters
 * of the SuggestionInput implementation it's a companion of
 */
interface SuggestionInputCompanion<I, out T> {
    fun readFrom(sin: StreamInput): SuggestionInput<I, T>
}
