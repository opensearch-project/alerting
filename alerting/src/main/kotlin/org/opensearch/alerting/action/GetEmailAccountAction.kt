/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class GetEmailAccountAction private constructor() : ActionType<GetEmailAccountResponse>(NAME, ::GetEmailAccountResponse) {
    companion object {
        val INSTANCE = GetEmailAccountAction()
        const val NAME = "cluster:admin/opendistro/alerting/destination/email_account/get"
    }
}
