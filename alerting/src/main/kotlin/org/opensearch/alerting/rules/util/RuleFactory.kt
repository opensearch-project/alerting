/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.rules.util

import org.opensearch.alerting.rules.ExampleRule

class RuleFactory {
    enum class RegisteredRule {
        EXAMPLE_RULE
    }

    fun getRule(registeredRule: RegisteredRule): Rule {
        return when (registeredRule) {
            RegisteredRule.EXAMPLE_RULE -> ExampleRule
        }
    }
}
