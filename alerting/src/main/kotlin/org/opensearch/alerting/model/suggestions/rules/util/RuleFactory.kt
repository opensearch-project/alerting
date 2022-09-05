/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.suggestions.rules.util

import org.opensearch.alerting.model.suggestions.rules.WildcardRule

object RuleFactory {
    private val registeredRules = listOf<Rule<*>>(
        WildcardRule
    )

    @Suppress("UNCHECKED_CAST")
    fun getRules(objType: SuggestionObjectType, component: String): List<Rule<Any>> {
        val resultRules = mutableListOf<Rule<Any>>()
        for (rule in registeredRules) {
            if (rule.objType == objType && rule.component.startsWith(component)) {
                // had to make registeredRules of type List<Rule<*>> so that Rules could be put in at all.
                // However, since Rule interface is already of type <in Any>, casting the selected Rule
                // to <in Any> (or <Any> for short since the interface already has the "in" part), should
                // be safe.
                resultRules.add(rule as Rule<Any>)
            }
        }
        return resultRules.toList() // make list immutable
    }
}
