/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType
import org.opensearch.action.delete.DeleteResponse

class DeleteEmailGroupAction private constructor() : ActionType<DeleteResponse>(NAME, ::DeleteResponse) {
    companion object {
        val INSTANCE = DeleteEmailGroupAction()
        const val NAME = "cluster:admin/opendistro/alerting/destination/email_group/delete"
    }
}
