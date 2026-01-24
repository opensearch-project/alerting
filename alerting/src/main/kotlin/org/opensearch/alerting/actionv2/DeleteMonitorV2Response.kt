/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.commons.alerting.util.IndexUtils
import org.opensearch.commons.notifications.action.BaseResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder

class DeleteMonitorV2Response : BaseResponse {
    var id: String
    var version: Long

    constructor(
        id: String,
        version: Long,
    ) : super() {
        this.id = id
        this.version = version
    }

    constructor(sin: StreamInput) : this(
        sin.readString(), // id
        sin.readLong(), // version
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
    }

    override fun toXContent(
        builder: XContentBuilder,
        params: ToXContent.Params,
    ): XContentBuilder =
        builder
            .startObject()
            .field(IndexUtils._ID, id)
            .field(IndexUtils._VERSION, version)
            .endObject()
}
