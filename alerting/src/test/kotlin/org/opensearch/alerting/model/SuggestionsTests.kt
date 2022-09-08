/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.model.suggestions.rules.util.RuleExecutor
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.test.OpenSearchTestCase

private val log = LogManager.getLogger(SuggestionsTests::class.java)

class SuggestionsTests : OpenSearchTestCase() {
    fun `test no suggestions found on valid object and component`() {
        // ensure framework returns clean response when there are no Rules that match a valid input's profile
        val suggestions = RuleExecutor.getSuggestions(randomQueryLevelMonitor(), "monitor.name")
        assertEquals(1, suggestions.size)
        assertEquals(suggestions[0], "no suggestions found for given object and its given component, or the supplied object or component is invalid")
    }

    // ADD UNIT TESTS FOR NEW RULES HERE
}
