/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.rest.RestStatus
import java.io.IOException

class GetSuggestionsResponse : ActionResponse, ToXContentObject {
    var suggestions: List<String>
    var status: RestStatus

    constructor(
        suggestions: List<String>,
        status: RestStatus,
    ) : super() {
        this.suggestions = suggestions
        this.status = status
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readStringList(), // suggestions
        sin.readEnum(RestStatus::class.java), // RestStatus
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(this.suggestions)
        out.writeEnum(this.status)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field("suggestions", this.suggestions)

        return builder.endObject()
    }
}
