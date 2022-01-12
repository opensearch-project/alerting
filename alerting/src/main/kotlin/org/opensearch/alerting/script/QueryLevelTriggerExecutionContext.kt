/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.script

import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.QueryLevelTrigger
import org.opensearch.alerting.model.QueryLevelTriggerRunResult
import java.time.Instant

data class QueryLevelTriggerExecutionContext(
    override val monitor: Monitor,
    val trigger: QueryLevelTrigger,
    override val results: List<Map<String, Any>>,
    override val periodStart: Instant,
    override val periodEnd: Instant,
    val alert: Alert? = null,
    override val error: Exception? = null
) : TriggerExecutionContext(monitor, results, periodStart, periodEnd, error) {

    constructor(
        monitor: Monitor,
        trigger: QueryLevelTrigger,
        monitorRunResult: MonitorRunResult<QueryLevelTriggerRunResult>,
        alert: Alert? = null
    ) : this(
        monitor, trigger, monitorRunResult.inputResults.results, monitorRunResult.periodStart, monitorRunResult.periodEnd,
        alert, monitorRunResult.scriptContextError(trigger)
    )

    /**
     * Mustache templates need special permissions to reflectively introspect field names. To avoid doing this we
     * translate the context to a Map of Strings to primitive types, which can be accessed without reflection.
     */
    override fun asTemplateArg(): Map<String, Any?> {
        val tempArg = super.asTemplateArg().toMutableMap()
        tempArg["trigger"] = trigger.asTemplateArg()
        tempArg["alert"] = alert?.asTemplateArg()
        return tempArg
    }
}
