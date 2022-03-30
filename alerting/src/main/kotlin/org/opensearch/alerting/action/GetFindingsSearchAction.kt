/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class GetFindingsSearchAction private constructor() : ActionType<GetFindingsSearchResponse>(NAME, ::GetFindingsSearchResponse) {
    companion object {
        val INSTANCE = GetFindingsSearchAction()
        val NAME = "cluster:admin/opendistro/alerting/findings/get"
    }
}
