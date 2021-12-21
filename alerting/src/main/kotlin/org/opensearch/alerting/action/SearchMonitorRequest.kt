/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.search.SearchRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import java.io.IOException

class SearchMonitorRequest : ActionRequest {

    val searchRequest: SearchRequest

    constructor(
        searchRequest: SearchRequest
    ) : super() {
        this.searchRequest = searchRequest
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        searchRequest = SearchRequest(sin)
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        searchRequest.writeTo(out)
    }
}
