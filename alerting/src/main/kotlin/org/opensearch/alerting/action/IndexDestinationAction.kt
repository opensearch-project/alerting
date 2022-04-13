/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class IndexDestinationAction private constructor() : ActionType<IndexDestinationResponse>(NAME, ::IndexDestinationResponse) {
    companion object {
        val INSTANCE = IndexDestinationAction()
        const val NAME = "cluster:admin/opendistro/alerting/destination/write"
    }
}
