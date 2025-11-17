/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.script

import org.opensearch.alerting.modelv2.MonitorV2

abstract class TriggerV2ExecutionContext(
    open val monitorV2: MonitorV2,
    open val error: Exception? = null
) {

    open fun asTemplateArg(): Map<String, Any?> {
        return mapOf(
            "monitorV2" to monitorV2.asTemplateArg(),
            "error" to error
        )
    }
}
