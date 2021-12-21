/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class IndexMonitorAction private constructor() : ActionType<IndexMonitorResponse>(NAME, ::IndexMonitorResponse) {
    companion object {
        val INSTANCE = IndexMonitorAction()
        val NAME = "cluster:admin/opendistro/alerting/monitor/write"
    }
}
