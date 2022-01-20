/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class GetEmailGroupAction private constructor() : ActionType<GetEmailGroupResponse>(NAME, ::GetEmailGroupResponse) {
    companion object {
        val INSTANCE = GetEmailGroupAction()
        val NAME = "cluster:admin/opendistro/alerting/destination/email_group/get"
    }
}
