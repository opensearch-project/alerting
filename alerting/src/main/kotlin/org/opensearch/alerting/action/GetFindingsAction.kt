/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class GetFindingsAction private constructor() : ActionType<GetFindingsResponse>(NAME, ::GetFindingsResponse) {
    companion object {
        val INSTANCE = GetFindingsAction()
        val NAME = "cluster:admin/opendistro/alerting/findings/get"
    }
}
