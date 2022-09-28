/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.suggestions.rules.util

object RuleExecutor {

    // TODO: theres a difference between supplying an invalid component,
    // TODO: and the framework not having Rules for that component, this
    // TODO: doesn't account for that difference very cleanly/explicitly
    const val NO_SUGGESTIONS_FOUND = "no suggestions found for given object and its given component, or the supplied object or component is invalid"

    fun getSuggestions(obj: Any, component: ComponentType): List<String> {
        val objType: SuggestionObjectType
        try {
            objType = SuggestionObjectType.enumFromClass(obj::class)
        } catch (e: Exception) {
            throw IllegalStateException("given object type is invalid")
        }

        if (component == ComponentType.NOT_SUPPORTED_COMPONENT) {
            return listOf(NO_SUGGESTIONS_FOUND)
        }

        val relevantRules = RuleFactory.getRules(
            objType,
            component
        )

        if (relevantRules.isEmpty()) {
            return listOf(NO_SUGGESTIONS_FOUND)
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
