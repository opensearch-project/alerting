/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.rules

import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.rules.util.Rule

object ExampleRule : Rule {
    // dummy example rule that checks for wildcard expressions in index declarations
    override fun evaluate(monitor: Monitor): Boolean {
        val input = monitor.inputs[0]

        if (input is SearchInput) {
            for (index in input.indices) {
                if (index.contains("*")) {
                    return false
                }
            }
        }

        return true
    }

    override fun suggestion(): String {
        return "an index in your inputs uses a wildcard (*), consider explicitly declaring indices instead"
    }
}
