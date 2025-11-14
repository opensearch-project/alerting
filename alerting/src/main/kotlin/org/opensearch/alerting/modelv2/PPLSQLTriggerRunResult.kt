/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.modelv2

import org.opensearch.alerting.modelv2.TriggerV2RunResult.Companion.ERROR_FIELD
import org.opensearch.alerting.modelv2.TriggerV2RunResult.Companion.NAME_FIELD
import org.opensearch.alerting.modelv2.TriggerV2RunResult.Companion.TRIGGERED_FIELD
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException

/**
 * A class that stores the run results of an individual
 * PPL/SQL trigger within a PPL/SQL monitor
 *
 * @opensearch.experimental
 */
data class PPLSQLTriggerRunResult(
    override var triggerName: String,
    override var triggered: Boolean,
    override var error: Exception?,
) : TriggerV2RunResult {

    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    constructor(sin: StreamInput) : this(
        triggerName = sin.readString(),
        triggered = sin.readBoolean(),
        error = sin.readException()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field(NAME_FIELD, triggerName)
        builder.field(TRIGGERED_FIELD, triggered)
        builder.field(ERROR_FIELD, error?.message)
        builder.endObject()
        return builder
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(triggerName)
        out.writeBoolean(triggered)
        out.writeException(error)
    }

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): PPLSQLTriggerRunResult {
            return PPLSQLTriggerRunResult(sin)
        }
    }
}
