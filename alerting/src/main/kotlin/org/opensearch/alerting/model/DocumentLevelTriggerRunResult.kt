/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.alerts.AlertError
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.script.ScriptException
import java.io.IOException
import java.time.Instant

data class DocumentLevelTriggerRunResult(
    override var triggerName: String,
    var triggeredDocs: List<String>,
    override var error: Exception?,
    var actionResults: MutableMap<String, ActionRunResult> = mutableMapOf()
) : TriggerRunResult(triggerName, error) {

    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    constructor(sin: StreamInput) : this(
        triggerName = sin.readString(),
        error = sin.readException(),
        triggeredDocs = sin.readStringList(),
        actionResults = sin.readMap() as MutableMap<String, ActionRunResult>
    )

    override fun alertError(): AlertError? {
        if (error != null) {
            return AlertError(Instant.now(), "Failed evaluating trigger:\n${error!!.userErrorMessage()}")
        }
        for (actionResult in actionResults.values) {
            if (actionResult.error != null) {
                return AlertError(Instant.now(), "Failed running action:\n${actionResult.error.userErrorMessage()}")
            }
        }
        return null
    }

    override fun internalXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        if (error is ScriptException) error = Exception((error as ScriptException).toJsonString(), error)
        return builder
            .field("triggeredDocs", triggeredDocs as List<String>)
            .field("action_results", actionResults as Map<String, ActionRunResult>)
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeStringCollection(triggeredDocs)
        out.writeMap(actionResults as Map<String, ActionRunResult>)
    }

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): TriggerRunResult {
            return DocumentLevelTriggerRunResult(sin)
        }
    }
}
