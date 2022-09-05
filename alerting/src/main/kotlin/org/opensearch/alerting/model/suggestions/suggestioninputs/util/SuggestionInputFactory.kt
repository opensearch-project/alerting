/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.suggestions.suggestioninputs.util

import org.opensearch.alerting.model.suggestions.suggestioninputs.MonitorIDInput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.xcontent.XContentParser

object SuggestionInputFactory {
    fun getInput(inputType: SuggestionInputType, xcp: XContentParser): SuggestionInput<*, Any> {
        return when (inputType) {
            SuggestionInputType.MONITOR_ID -> {
                val input = MonitorIDInput()
                input.parseInput(xcp)
                input
            }
        }
    }

    fun getInput(inputType: SuggestionInputType, sin: StreamInput): SuggestionInput<*, Any> {
        return when (inputType) {
            SuggestionInputType.MONITOR_ID -> MonitorIDInput.readFrom(sin)
        }
    }
}
