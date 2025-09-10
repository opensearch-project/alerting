package org.opensearch.alerting.actionv2

import org.opensearch.action.ActionType

class IndexMonitorV2Action private constructor() : ActionType<IndexMonitorV2Response>(NAME, ::IndexMonitorV2Response) {
    companion object {
        val INSTANCE = IndexMonitorV2Action()
        const val NAME = "cluster:admin/opensearch/alerting/v2/monitor/write"
    }
}
