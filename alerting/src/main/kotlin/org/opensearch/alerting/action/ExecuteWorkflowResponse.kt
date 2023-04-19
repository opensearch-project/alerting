/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionResponse
import org.opensearch.alerting.model.WorkflowRunResult
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException

class ExecuteWorkflowResponse : ActionResponse, ToXContentObject {
    val workflowRunResult: WorkflowRunResult
    constructor(
        workflowRunResult: WorkflowRunResult
    ) : super() {
        this.workflowRunResult = workflowRunResult
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        WorkflowRunResult(sin)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        workflowRunResult.writeTo(out)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return workflowRunResult.toXContent(builder, params)
    }
}
