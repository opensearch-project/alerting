/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType
import org.opensearch.action.delete.DeleteResponse

class DeleteDestinationAction private constructor() : ActionType<DeleteResponse>(NAME, ::DeleteResponse) {
    companion object {
        val INSTANCE = DeleteDestinationAction()
        val NAME = "cluster:admin/opendistro/alerting/destination/delete"
    }
}
