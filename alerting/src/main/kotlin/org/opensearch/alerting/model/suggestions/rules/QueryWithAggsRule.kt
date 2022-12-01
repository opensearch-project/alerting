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

object QueryWithAggsRule : Rule<Monitor> {
    override val objType = SuggestionObjectType.MONITOR
    override val component = ComponentType.MONITOR_QUERY

    // the contents of this Rule's suggestion are the same every time an object
    // fails this Rule's check, so declare it as a constant
    const val QUERY_WITH_AGGS_SUGGESTION = "Each query in all of your search inputs performs an aggregation (a group by)." +
        "For performing queries on aggregates, please consider using a Bucket Level Monitor instead"

    @Suppress("UNCHECKED_CAST")
    override fun evaluate(obj: Monitor): String? {
        if (obj.monitorType != Monitor.MonitorType.QUERY_LEVEL_MONITOR) {
            return null // this Rule doesn't apply to non query-level monitors
        }

        val inputs = obj.inputs as List<SearchInput> // ok because all query level monitors use SearchInputs
        val input = inputs[0] // monitors can only have 1 search input

        if (input.query.aggregations() == null || input.query.aggregations().aggregatorFactories.isEmpty()) {
            return null // if their search input uses a query without an aggregation, don't recommend bucket level monitor
        }

        return QUERY_WITH_AGGS_SUGGESTION
    }
}
