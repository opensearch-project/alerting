/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType
import org.opensearch.action.search.SearchResponse

class SearchMonitorAction private constructor() : ActionType<SearchResponse>(NAME, ::SearchResponse) {
    companion object {
        val INSTANCE = SearchMonitorAction()
        const val NAME = "cluster:admin/opendistro/alerting/monitor/search"
    }
}
