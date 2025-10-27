/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.modelv2

import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent

interface TriggerV2RunResult : Writeable, ToXContent {

    val triggerName: String
    val triggered: Boolean
    val error: Exception?

    companion object {
        const val NAME_FIELD = "name"
        const val TRIGGERED_FIELD = "triggered"
        const val ERROR_FIELD = "error"
    }
}
