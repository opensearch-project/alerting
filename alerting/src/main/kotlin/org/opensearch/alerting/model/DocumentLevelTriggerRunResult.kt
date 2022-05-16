/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.script.ScriptException
import java.io.IOException

data class DocumentLevelTriggerRunResult(
    override var triggerName: String,
    var triggeredDocs: List<String>,
    override var error: Exception?,
    var actionResultsMap: MutableMap<String, MutableMap<String, ActionRunResult>> = mutableMapOf()
) : TriggerRunResult(triggerName, error) {

    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    constructor(sin: StreamInput) : this(
        triggerName = sin.readString(),
        error = sin.readException(),
        triggeredDocs = sin.readStringList(),
        actionResultsMap = sin.readMap() as MutableMap<String, MutableMap<String, ActionRunResult>>
    )

    override fun internalXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        if (error is ScriptException) error = Exception((error as ScriptException).toJsonString(), error)
        return builder
            .field("triggeredDocs", triggeredDocs as List<String>)
            .field("action_results", actionResultsMap as Map<String, Any>)
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeStringCollection(triggeredDocs)
        out.writeMap(actionResultsMap as Map<String, Any>)
    }

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): TriggerRunResult {
            return DocumentLevelTriggerRunResult(sin)
        }
    }
}
