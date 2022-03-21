/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType
import org.opensearch.action.search.SearchResponse

class SearchEmailAccountAction private constructor() : ActionType<SearchResponse>(NAME, ::SearchResponse) {
    companion object {
        val INSTANCE = SearchEmailAccountAction()
        const val NAME = "cluster:admin/opendistro/alerting/destination/email_account/search"
    }
}
