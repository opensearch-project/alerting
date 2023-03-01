/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.Workflow
import java.io.IOException

/**
 * A class containing workflow details.
 */
class ExecuteWorkflowRequest : ActionRequest {
    val dryrun: Boolean
    val requestEnd: TimeValue
    val workflowId: String?
    val workflow: Workflow?

    constructor(
        dryrun: Boolean,
        requestEnd: TimeValue,
        workflowId: String?,
        workflow: Workflow?,
    ) : super() {
        this.dryrun = dryrun
        this.requestEnd = requestEnd
        this.workflowId = workflowId
        this.workflow = workflow
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readBoolean(),
        sin.readTimeValue(),
        sin.readOptionalString(),
        if (sin.readBoolean()) {
            Workflow.readFrom(sin)
        } else null
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (workflowId == null && workflow == null) {
            validationException = ValidateActions.addValidationError(
                "Both workflow and workflow id are missing", validationException
            )
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(dryrun)
        out.writeTimeValue(requestEnd)
        out.writeOptionalString(workflowId)
        if (workflow != null) {
            out.writeBoolean(true)
            workflow.writeTo(out)
        } else {
            out.writeBoolean(false)
        }
    }
}
