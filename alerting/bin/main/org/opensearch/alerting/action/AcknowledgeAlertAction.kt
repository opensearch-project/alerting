/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class AcknowledgeAlertAction private constructor() : ActionType<AcknowledgeAlertResponse>(NAME, ::AcknowledgeAlertResponse) {
    companion object {
        val INSTANCE = AcknowledgeAlertAction()
        const val NAME = "cluster:admin/opendistro/alerting/alerts/ack"
    }
}
