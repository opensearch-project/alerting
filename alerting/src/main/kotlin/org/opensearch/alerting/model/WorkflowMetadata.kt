/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.commons.alerting.util.instant
import org.opensearch.commons.alerting.util.optionalTimeField
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import java.io.IOException
import java.time.Instant

data class WorkflowMetadata(
    val id: String,
    val workflowId: String,
    val monitorIds: List<String>,
    val latestRunTime: Instant,
    val latestExecutionId: String
) : Writeable, ToXContent {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        workflowId = sin.readString(),
        monitorIds = sin.readStringList(),
        latestRunTime = sin.readInstant(),
        latestExecutionId = sin.readString()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeString(workflowId)
        out.writeStringCollection(monitorIds)
        out.writeInstant(latestRunTime)
        out.writeString(latestExecutionId)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (params.paramAsBoolean("with_type", false)) builder.startObject(METADATA)
        builder.field(WORKFLOW_ID_FIELD, workflowId)
            .field(MONITOR_IDS_FIELD, monitorIds)
            .optionalTimeField(LATEST_RUN_TIME, latestRunTime)
            .field(LATEST_EXECUTION_ID, latestExecutionId)
        if (params.paramAsBoolean("with_type", false)) builder.endObject()
        return builder.endObject()
    }

    companion object {
        const val METADATA = "workflow_metadata"
        const val WORKFLOW_ID_FIELD = "workflow_id"
        const val MONITOR_IDS_FIELD = "monitor_ids"
        const val LATEST_RUN_TIME = "latest_run_time"
        const val LATEST_EXECUTION_ID = "latest_execution_id"

        @JvmStatic @JvmOverloads
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): WorkflowMetadata {
            lateinit var workflowId: String
            var monitorIds = mutableListOf<String>()
            lateinit var latestRunTime: Instant
            lateinit var latestExecutionId: String

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    WORKFLOW_ID_FIELD -> workflowId = xcp.text()
                    MONITOR_IDS_FIELD -> {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            monitorIds.add(xcp.text())
                        }
                    }
                    LATEST_RUN_TIME -> latestRunTime = xcp.instant()!!
                    LATEST_EXECUTION_ID -> latestExecutionId = xcp.text()
                }
            }
            return WorkflowMetadata(
                "$workflowId-metadata",
                workflowId = workflowId,
                monitorIds = monitorIds,
                latestRunTime = latestRunTime,
                latestExecutionId = latestExecutionId
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): WorkflowMetadata {
            return WorkflowMetadata(sin)
        }

        fun getId(workflowId: String? = null) = "$workflowId-metadata"
    }
}
