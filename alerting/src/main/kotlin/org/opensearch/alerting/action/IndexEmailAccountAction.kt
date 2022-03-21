/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class IndexEmailAccountAction private constructor() : ActionType<IndexEmailAccountResponse>(NAME, ::IndexEmailAccountResponse) {
    companion object {
        val INSTANCE = IndexEmailAccountAction()
        const val NAME = "cluster:admin/opendistro/alerting/destination/email_account/write"
    }
}
