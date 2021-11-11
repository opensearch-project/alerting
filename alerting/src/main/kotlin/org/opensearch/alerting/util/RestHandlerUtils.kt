/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.alerting.AlertingPlugin
import org.opensearch.common.Strings
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
    val userAgent = Strings.coalesceToEmpty(request.header("User-Agent"))
    return if (!userAgent.contains(AlertingPlugin.OPEN_SEARCH_DASHBOARDS_USER_AGENT)) {
        FetchSourceContext(true, Strings.EMPTY_ARRAY, AlertingPlugin.UI_METADATA_EXCLUDE)
    } else null
}

const val _ID = "_id"
const val _VERSION = "_version"
const val _SEQ_NO = "_seq_no"
const val IF_SEQ_NO = "if_seq_no"
const val _PRIMARY_TERM = "_primary_term"
const val IF_PRIMARY_TERM = "if_primary_term"
const val REFRESH = "refresh"
