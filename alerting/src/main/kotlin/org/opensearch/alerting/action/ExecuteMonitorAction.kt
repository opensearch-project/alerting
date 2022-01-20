/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class ExecuteMonitorAction private constructor() : ActionType<ExecuteMonitorResponse>(NAME, ::ExecuteMonitorResponse) {
    companion object {
        val INSTANCE = ExecuteMonitorAction()
        val NAME = "cluster:admin/opendistro/alerting/monitor/execute"
    }
}
