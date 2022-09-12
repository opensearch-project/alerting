package org.opensearch.alerting.model.suggestions.rules

import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.suggestions.rules.util.Rule
import org.opensearch.alerting.model.suggestions.rules.util.SuggestionObjectType

object QueryWithAggsRule : Rule<Monitor> {
    override val objType = SuggestionObjectType.MONITOR
    override val component = "monitor.inputs.search.query"

    @Suppress("UNCHECKED_CAST")
    override fun evaluate(obj: Monitor): String? {
        if (obj.monitorType != Monitor.MonitorType.QUERY_LEVEL_MONITOR) {
            return null // this Rule doesn't apply to non query-level monitors
        }

        val inputs = obj.inputs as List<SearchInput> // ok because all query level monitors use SearchInputs

        for (input in inputs) {
            if (input.query.aggregations() == null) {
                return null // if even one of their search inputs use a query without an aggregation, don't recommend bucket level monitor
            }
        }

        return "Each query in all of your search inputs performs an aggregation (a group by)." +
            "For performing queries on aggregates, please consider using a Bucket Level Monitor instead"
    }
}
