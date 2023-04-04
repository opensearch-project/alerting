/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class ExecuteWorkflowAction private constructor() : ActionType<ExecuteWorkflowResponse>(NAME, ::ExecuteWorkflowResponse) {
    companion object {
        val INSTANCE = ExecuteWorkflowAction()
        const val NAME = "cluster:admin/opendistro/alerting/workflow/execute"
    }
}
