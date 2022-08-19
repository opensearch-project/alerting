/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class GetSuggestionsAction private constructor() : ActionType<GetSuggestionsResponse>(NAME, ::GetSuggestionsResponse) {
    companion object {
        val INSTANCE = GetSuggestionsAction()
        const val NAME = "cluster:admin/opensearch/alerting/suggestions/get" // TODO: is this an ok name to use?
    }
}
