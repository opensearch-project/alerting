/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.suggestions.rules

import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.suggestions.rules.util.ComponentType
import org.opensearch.alerting.model.suggestions.rules.util.Rule
import org.opensearch.alerting.model.suggestions.rules.util.SuggestionObjectType

object WildcardRule : Rule<Monitor> {

    override val objType = SuggestionObjectType.MONITOR
    override val component = ComponentType.MONITOR_QUERY

    // dummy example rule that checks for wildcard expressions in index declarations
    override fun evaluate(obj: Monitor): String? {
        val input = obj.inputs[0]

        if (input is SearchInput) {
            for (index in input.indices) {
                if (index.contains("*")) {
                    return "an index in your inputs uses a wildcard (*), consider explicitly declaring indices instead"
                }
            }
        }

        return null
    }
}
