/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.script

import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.Trigger
import java.time.Instant

abstract class TriggerExecutionContext(
    open val monitor: Monitor,
    open val results: List<Map<String, Any>>,
    open val periodStart: Instant,
    open val periodEnd: Instant,
    open val error: Exception? = null
) {

    constructor(monitor: Monitor, trigger: Trigger, monitorRunResult: MonitorRunResult<*>) :
        this(
            monitor, monitorRunResult.inputResults.results, monitorRunResult.periodStart,
            monitorRunResult.periodEnd, monitorRunResult.scriptContextError(trigger)
        )

    /**
     * Mustache templates need special permissions to reflectively introspect field names. To avoid doing this we
     * translate the context to a Map of Strings to primitive types, which can be accessed without reflection.
     */
    open fun asTemplateArg(): Map<String, Any?> {
        return mapOf(
            "monitor" to monitor.asTemplateArg(),
            "results" to results,
            "periodStart" to periodStart,
            "periodEnd" to periodEnd,
            "error" to error
        )
    }
}
