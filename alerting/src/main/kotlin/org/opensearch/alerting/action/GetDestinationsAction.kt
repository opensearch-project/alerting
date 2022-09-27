/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class GetDestinationsAction private constructor() : ActionType<GetDestinationsResponse>(NAME, ::GetDestinationsResponse) {
    companion object {
        val INSTANCE = GetDestinationsAction()
        const val NAME = "cluster:admin/opendistro/alerting/destination/get"
    }
}
