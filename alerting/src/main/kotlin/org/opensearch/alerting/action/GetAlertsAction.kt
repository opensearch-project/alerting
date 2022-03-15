/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class GetAlertsAction private constructor() : ActionType<GetAlertsResponse>(NAME, ::GetAlertsResponse) {
    companion object {
        val INSTANCE = GetAlertsAction()
        const val NAME = "cluster:admin/opendistro/alerting/alerts/get"
    }
}
