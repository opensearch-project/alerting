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

        // TODO: theres a difference between supplying an invalid component,
        // TODO: and the framework not having Rules for that component, this
        // TODO: doesn't account for that difference very cleanly/explicitly
        if (relevantRules.isEmpty()) {
            return listOf("no suggestions found for given object and its given component, or the supplied object or component is invalid")
        }

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
