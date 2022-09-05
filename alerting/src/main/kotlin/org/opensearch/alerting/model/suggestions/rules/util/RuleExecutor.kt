/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.suggestions.rules.util

object RuleExecutor {
    fun getSuggestions(obj: Any, component: String): List<String> {
        val relevantRules = RuleFactory.getRules(
            SuggestionObjectType.enumFromClass(obj::class),
            component
        )

        val suggestions = mutableListOf<String>()

        for (rule in relevantRules) {
            val suggestion = rule.evaluate(obj)
            if (suggestion != null) {
                suggestions.add(suggestion)
            }
        }

        return suggestions.toList()
    }
}
