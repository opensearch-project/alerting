/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.suggestions.suggestioninputs.util

interface SuggestionsObjectListener {
    fun onGetResponse(obj: Any)
    fun onFailure(e: Exception)
}
