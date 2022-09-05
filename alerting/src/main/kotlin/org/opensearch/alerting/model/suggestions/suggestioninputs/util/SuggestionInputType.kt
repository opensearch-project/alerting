/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.suggestions.suggestioninputs.util

enum class SuggestionInputType(val value: String) {
    MONITOR_ID("monitorId");
    // MONITOR_OBJ("monitorObj");

    override fun toString(): String {
        return value
    }

    companion object {
        fun enumFromStr(value: String) = SuggestionInputType.values().first { it.value == value }
    }
}
