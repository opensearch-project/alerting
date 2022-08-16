/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionResponse
import org.opensearch.alerting.model.Alert
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import java.io.IOException
import java.util.Collections

class AcknowledgeAlertResponse : ActionResponse, ToXContentObject {

    val acknowledged: List<Alert>
    val failed: List<Alert>
    val missing: List<String>

    constructor(
        acknowledged: List<Alert>,
        failed: List<Alert>,
        missing: List<String>
    ) : super() {
        this.acknowledged = acknowledged
        this.failed = failed
        this.missing = missing
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        Collections.unmodifiableList(sin.readList(::Alert)), // acknowledged
        Collections.unmodifiableList(sin.readList(::Alert)), // failed
        Collections.unmodifiableList(sin.readStringList()) // missing
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeCollection(acknowledged)
        out.writeCollection(failed)
        out.writeStringCollection(missing)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {

        builder.startObject().startArray("success")
        acknowledged.forEach { builder.value(it.id) }
        builder.endArray().startArray("failed")
        failed.forEach { buildFailedAlertAcknowledgeObject(builder, it) }
        missing.forEach { buildMissingAlertAcknowledgeObject(builder, it) }
        return builder.endArray().endObject()
    }

    private fun buildFailedAlertAcknowledgeObject(builder: XContentBuilder, failedAlert: Alert) {
        builder.startObject()
            .startObject(failedAlert.id)
        val reason = when (failedAlert.state) {
            Alert.State.ERROR -> "Alert is in an error state and can not be acknowledged."
            Alert.State.COMPLETED -> "Alert has already completed and can not be acknowledged."
            Alert.State.ACKNOWLEDGED -> "Alert has already been acknowledged."
            else -> "Alert state unknown and can not be acknowledged"
        }
        builder.field("failed_reason", reason)
            .endObject()
            .endObject()
    }

    private fun buildMissingAlertAcknowledgeObject(builder: XContentBuilder, alertID: String) {
        builder.startObject()
            .startObject(alertID)
            .field("failed_reason", "Alert: $alertID does not exist (it may have already completed).")
            .endObject()
            .endObject()
    }
}
