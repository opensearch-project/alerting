/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType
import org.opensearch.commons.alerting.action.AcknowledgeAlertResponse

class AcknowledgeChainedAlertsAction private constructor() : ActionType<AcknowledgeAlertResponse>(NAME, ::AcknowledgeAlertResponse) {
    companion object {
        val INSTANCE = AcknowledgeChainedAlertsAction()
        const val NAME = "cluster:admin/opendistro/alerting/workflow /acknowledgeChainedAlert"
    }
}
