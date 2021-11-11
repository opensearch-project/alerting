/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class GetMonitorAction private constructor() : ActionType<GetMonitorResponse>(NAME, ::GetMonitorResponse) {
    companion object {
        val INSTANCE = GetMonitorAction()
        val NAME = "cluster:admin/opendistro/alerting/monitor/get"
    }
}
