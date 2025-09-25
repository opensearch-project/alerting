package org.opensearch.alerting.actionv2

import org.opensearch.action.ActionType
import org.opensearch.action.search.SearchResponse

class SearchMonitorV2Action private constructor() : ActionType<SearchResponse>(NAME, ::SearchResponse) {
    companion object {
        val INSTANCE = SearchMonitorV2Action()
        const val NAME = "cluster:admin/opensearch/alerting/v2/monitor/search"
    }
}
