/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.alerting.modelv2.AlertV2
import org.opensearch.commons.notifications.action.BaseResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException
import java.util.Collections

class GetAlertsV2Response : BaseResponse {
    val alertV2s: List<AlertV2>

    // totalAlertV2s is not the same as the size of alertV2s because there can be 30 alerts from the request, but
    // the request only asked for 5 alerts, so totalAlertV2s will be 30, but alertV2s will only contain 5 alerts
    val totalAlertV2s: Int?

    constructor(
        alertV2s: List<AlertV2>,
        totalAlertV2s: Int?
    ) : super() {
        this.alertV2s = alertV2s
        this.totalAlertV2s = totalAlertV2s
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        alertV2s = Collections.unmodifiableList(sin.readList(::AlertV2)),
        totalAlertV2s = sin.readOptionalInt()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeCollection(alertV2s)
        out.writeOptionalInt(totalAlertV2s)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field("alertV2s", alertV2s)
            .field("totalAlertV2s", totalAlertV2s)

        return builder.endObject()
    }
}
