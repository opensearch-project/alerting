/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class GetRemoteIndexesAction private constructor() : ActionType<GetRemoteIndexesResponse>(NAME, ::GetRemoteIndexesResponse) {
    companion object {
        val INSTANCE = GetRemoteIndexesAction()
        const val NAME = "cluster:admin/opensearch/alerting/remote/indexes/get"
    }
}
