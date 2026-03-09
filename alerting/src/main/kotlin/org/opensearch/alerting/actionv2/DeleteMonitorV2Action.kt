/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.action.ActionType

class DeleteMonitorV2Action private constructor() : ActionType<DeleteMonitorV2Response>(NAME, ::DeleteMonitorV2Response) {
    companion object {
        val INSTANCE = DeleteMonitorV2Action()
        const val NAME = "cluster:admin/opensearch/alerting/v2/monitor/delete"
    }
}
