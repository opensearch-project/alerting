package org.opensearch.alerting.action

import org.opensearch.action.ActionType

// TODO: should this and the ExecuteMonitorV2Request/Response be moved to common utils?
class ExecuteMonitorV2Action private constructor() : ActionType<ExecuteMonitorV2Response>(NAME, ::ExecuteMonitorV2Response) {
    companion object {
        val INSTANCE = ExecuteMonitorV2Action()
        const val NAME = "cluster:admin/opensearch/alerting/v2/monitor/execute"
    }
}
