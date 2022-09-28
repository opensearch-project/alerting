/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.suggestions.rules.util

import org.opensearch.alerting.model.suggestions.rules.QueryWithAggsRule

object RuleFactory {
    private val registeredRules = listOf<Rule<*>>(
        QueryWithAggsRule
    )

    @Suppress("UNCHECKED_CAST")
    fun getRules(objType: SuggestionObjectType, component: ComponentType): List<Rule<Any>> { // component can't be NOT_SUPPORTED_COMPONENT
        val resultRules = mutableListOf<Rule<Any>>()
        for (rule in registeredRules) {
            if (rule.objType == objType && (component == rule.component || component == ComponentType.MONITOR_WHOLE)) {
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
