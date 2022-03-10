/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class IndexEmailGroupAction private constructor() : ActionType<IndexEmailGroupResponse>(NAME, ::IndexEmailGroupResponse) {
    companion object {
        val INSTANCE = IndexEmailGroupAction()
        val NAME = "cluster:admin/opendistro/alerting/destination/email_group/write"
    }
}
