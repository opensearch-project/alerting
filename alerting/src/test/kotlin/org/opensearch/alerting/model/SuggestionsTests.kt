/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.model.suggestions.rules.QueryWithAggsRule
import org.opensearch.alerting.model.suggestions.rules.util.RuleExecutor
import org.opensearch.alerting.randomBucketLevelMonitor
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase

private val log = LogManager.getLogger(SuggestionsTests::class.java)

class SuggestionsTests : OpenSearchTestCase() {
    fun `test no suggestions found on valid object and component`() {
        // ensure framework returns clean response when there are no Rules that match a valid input's profile
        val suggestions = RuleExecutor.getSuggestions(randomQueryLevelMonitor(), "monitor.name")
        assertEquals(1, suggestions.size)
        assertEquals(suggestions[0], RuleExecutor.NO_SUGGESTIONS_FOUND)
    }

    // TODO: test for factory returning right Rules with right components

    // ADD UNIT TESTS FOR NEW RULES HERE

    // QueryWithAggsRule tests
    fun `test QueryWithAggsRule gives no suggestion if monitor isn't query-level`() {
        val monitor = randomBucketLevelMonitor()
        val suggestions = RuleExecutor.getSuggestions(monitor, "monitor")
        assert(!suggestions.contains(QueryWithAggsRule.QUERY_WITH_AGGS_SUGGESTION))
    }

    fun `test QueryWithAggsRule gives no suggestion if query level monitor's input doesn't use aggregations`() {
        val monitor = randomQueryLevelMonitor() // the random monitor returned has, by default, a single SearchInput that has null aggregations
        val suggestions = RuleExecutor.getSuggestions(monitor, "monitor")
        assert(!suggestions.contains(QueryWithAggsRule.QUERY_WITH_AGGS_SUGGESTION))
    }

    fun `test QueryWithAggsRule gives suggestion when query level monitor's input uses aggregations`() {
        val monitor = randomQueryLevelMonitor(
            inputs = listOf(
                SearchInput(emptyList(), SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).aggregation(AggregationBuilders.max("max").field("field1")))
            )
        )
        val suggestions = RuleExecutor.getSuggestions(monitor, "monitor")
        assert(suggestions.contains(QueryWithAggsRule.QUERY_WITH_AGGS_SUGGESTION))
    }
}
