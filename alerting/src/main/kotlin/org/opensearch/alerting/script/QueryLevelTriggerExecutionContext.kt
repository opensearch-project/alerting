/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.script

import org.opensearch.alerting.model.AlertContext
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.QueryLevelTriggerRunResult
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.QueryLevelTrigger
import java.time.Instant

data class QueryLevelTriggerExecutionContext(
    override val monitor: Monitor,
    val trigger: QueryLevelTrigger,
    override val results: List<Map<String, Any>>,
    override val periodStart: Instant,
    override val periodEnd: Instant,
    val alert: Alert? = null,
    val alertContext: AlertContext? = null,
    override val error: Exception? = null
) : TriggerExecutionContext(monitor, results, periodStart, periodEnd, error) {

    constructor(
        monitor: Monitor,
        trigger: QueryLevelTrigger,
        monitorRunResult: MonitorRunResult<QueryLevelTriggerRunResult>,
        alert: Alert? = null,
        alertContext: AlertContext? = null
    ) : this(
        monitor, trigger, monitorRunResult.inputResults.results, monitorRunResult.periodStart, monitorRunResult.periodEnd,
        alert, alertContext, monitorRunResult.scriptContextError(trigger)
    )

    /**
     * Mustache templates need special permissions to reflectively introspect field names. To avoid doing this we
     * translate the context to a Map of Strings to primitive types, which can be accessed without reflection.
     */
    override fun asTemplateArg(): Map<String, Any?> {
        val tempArg = super.asTemplateArg().toMutableMap()
        tempArg["trigger"] = trigger.asTemplateArg()
        tempArg["alert"] = alertContext?.asTemplateArg() // map "alert" templateArg field to alertContext wrapper instead of alert
        return tempArg
    }
}
