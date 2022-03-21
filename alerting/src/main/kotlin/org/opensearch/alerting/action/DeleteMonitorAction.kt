/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType
import org.opensearch.action.delete.DeleteResponse

class DeleteMonitorAction private constructor() : ActionType<DeleteResponse>(NAME, ::DeleteResponse) {
    companion object {
        val INSTANCE = DeleteMonitorAction()
        const val NAME = "cluster:admin/opendistro/alerting/monitor/delete"
    }
}
