/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class MonitorExplainAction private constructor() : ActionType<MonitorExplainResponse>(NAME, ::MonitorExplainResponse) {
    companion object {
        val INSTANCE = MonitorExplainAction()
        const val NAME = "cluster:admin/opendistro/alerting/monitor/explain"
    }
}
