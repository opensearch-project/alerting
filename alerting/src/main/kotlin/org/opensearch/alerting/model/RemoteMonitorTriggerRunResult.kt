/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.script.ScriptException
import java.io.IOException

open class RemoteMonitorTriggerRunResult(
    override var triggerName: String,
    override var error: Exception? = null,
    var actionResultsMap: MutableMap<String, MutableMap<String, ActionRunResult>> = mutableMapOf()
) : TriggerRunResult(triggerName, error) {

    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    constructor(sin: StreamInput) : this(
        triggerName = sin.readString(),
        error = sin.readException(),
        actionResultsMap = readActionResults(sin)
    )

    override fun internalXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        if (error is ScriptException) error = Exception((error as ScriptException).toJsonString(), error)
        return builder.field("action_results", actionResultsMap as Map<String, Any>)
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeInt(actionResultsMap.size)
        actionResultsMap.forEach { (alert, actionResults) ->
            out.writeString(alert)
            out.writeInt(actionResults.size)
            actionResults.forEach { (id, result) ->
                out.writeString(id)
                result.writeTo(out)
            }
        }
    }

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): TriggerRunResult {
            return RemoteMonitorTriggerRunResult(sin)
        }

        @JvmStatic
        fun readActionResults(sin: StreamInput): MutableMap<String, MutableMap<String, ActionRunResult>> {
            val actionResultsMapReconstruct: MutableMap<String, MutableMap<String, ActionRunResult>> = mutableMapOf()
            val size = sin.readInt()
            var idx = 0
            while (idx < size) {
                val alert = sin.readString()
                val actionResultsSize = sin.readInt()
                val actionRunResultElem = mutableMapOf<String, ActionRunResult>()
                var i = 0
                while (i < actionResultsSize) {
                    val actionId = sin.readString()
                    val actionResult = ActionRunResult.readFrom(sin)
                    actionRunResultElem[actionId] = actionResult
                    ++i
                }
                actionResultsMapReconstruct[alert] = actionRunResultElem
                ++idx
            }
            return actionResultsMapReconstruct
        }
    }
}
