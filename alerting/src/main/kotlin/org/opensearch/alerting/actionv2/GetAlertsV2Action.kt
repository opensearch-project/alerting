package org.opensearch.alerting.actionv2

import org.opensearch.action.ActionType

class GetAlertsV2Action private constructor() : ActionType<GetAlertsV2Response>(NAME, ::GetAlertsV2Response) {
    companion object {
        val INSTANCE = GetAlertsV2Action()
        const val NAME = "cluster:admin/opensearch/alerting/v2/alerts/get"
    }
}
