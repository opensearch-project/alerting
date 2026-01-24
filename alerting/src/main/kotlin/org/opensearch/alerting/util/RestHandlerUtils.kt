/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.alerting.AlertingPlugin
import org.opensearch.core.common.Strings
import org.opensearch.rest.RestRequest
import org.opensearch.search.fetch.subphase.FetchSourceContext

/**
 * Checks to see if the request came from Kibana, if so we want to return the UI Metadata from the document.
 * If the request came from the client then we exclude the UI Metadata from the search result.
 *
 * @param request
 * @return FetchSourceContext
 */
fun context(request: RestRequest): FetchSourceContext? {
    val userAgent = if (request.header("User-Agent") == null) "" else request.header("User-Agent")
    return if (!userAgent.contains(AlertingPlugin.OPEN_SEARCH_DASHBOARDS_USER_AGENT)) {
        FetchSourceContext(true, Strings.EMPTY_ARRAY, AlertingPlugin.UI_METADATA_EXCLUDE)
    } else {
        null
    }
}

const val IF_SEQ_NO = "if_seq_no"
const val IF_PRIMARY_TERM = "if_primary_term"
const val REFRESH = "refresh"
