package org.opensearch.alerting.actionv2

import org.opensearch.action.ActionType

class GetMonitorV2Action private constructor() : ActionType<GetMonitorV2Response>(NAME, ::GetMonitorV2Response) {
    companion object {
        val INSTANCE = GetMonitorV2Action()
        const val NAME = "cluster:admin/opensearch/alerting/v2/monitor/get"
    }
}
