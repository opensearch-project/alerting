/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.action.ActionType

class ExecuteMonitorV2Action private constructor() : ActionType<ExecuteMonitorV2Response>(NAME, ::ExecuteMonitorV2Response) {
    companion object {
        val INSTANCE = ExecuteMonitorV2Action()
        const val NAME = "cluster:admin/opensearch/alerting/v2/monitor/execute"
    }
}
