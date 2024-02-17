/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.commons.alerting.alerts.AlertError
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.script.ScriptException
import java.io.IOException
import java.time.Instant

data class ClusterMetricsTriggerRunResult(
    override var triggerName: String,
    override var triggered: Boolean,
    override var error: Exception?,
    override var actionResults: MutableMap<String, ActionRunResult> = mutableMapOf(),
    var clusterTriggerResults: List<ClusterTriggerResult> = listOf()
) : QueryLevelTriggerRunResult(
    triggerName = triggerName,
    error = error,
    triggered = triggered,
    actionResults = actionResults
) {

    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    constructor(sin: StreamInput) : this(
        triggerName = sin.readString(),
        error = sin.readException(),
        triggered = sin.readBoolean(),
        actionResults = sin.readMap() as MutableMap<String, ActionRunResult>,
        clusterTriggerResults = sin.readList((ClusterTriggerResult.Companion)::readFrom)
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
        builder
            .field(TRIGGERED_FIELD, triggered)
            .field(ACTION_RESULTS_FIELD, actionResults as Map<String, ActionRunResult>)
            .startArray(CLUSTER_RESULTS_FIELD)
        clusterTriggerResults.forEach { it.toXContent(builder, params) }
        return builder.endArray()
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeBoolean(triggered)
        out.writeMap(actionResults as Map<String, ActionRunResult>)
        clusterTriggerResults.forEach { it.writeTo(out) }
    }

    companion object {
        const val TRIGGERED_FIELD = "triggered"
        const val ACTION_RESULTS_FIELD = "action_results"
        const val CLUSTER_RESULTS_FIELD = "cluster_results"
    }

    data class ClusterTriggerResult(
        val cluster: String,
        val triggered: Boolean,
    ) : ToXContentObject, Writeable {

        @Throws(IOException::class)
        constructor(sin: StreamInput) : this(
            cluster = sin.readString(),
            triggered = sin.readBoolean()
        )

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .startObject(cluster)
                .field(TRIGGERED_FIELD, triggered)
                .endObject()
                .endObject()
        }

        override fun writeTo(out: StreamOutput) {
            out.writeString(cluster)
            out.writeBoolean(triggered)
        }

        companion object {
            @JvmStatic
            @Throws(IOException::class)
            fun readFrom(sin: StreamInput): ClusterTriggerResult {
                return ClusterTriggerResult(sin)
            }
        }
    }
}
