/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.script

import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.BucketLevelTrigger
import org.opensearch.alerting.model.BucketLevelTriggerRunResult
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.MonitorRunResult
import java.time.Instant

data class BucketLevelTriggerExecutionContext(
    override val monitor: Monitor,
    val trigger: BucketLevelTrigger,
    override val results: List<Map<String, Any>>,
    override val periodStart: Instant,
    override val periodEnd: Instant,
    val dedupedAlerts: List<Alert> = listOf(),
    val newAlerts: List<Alert> = listOf(),
    val completedAlerts: List<Alert> = listOf(),
    override val error: Exception? = null
) : TriggerExecutionContext(monitor, results, periodStart, periodEnd, error) {

    constructor(
        monitor: Monitor,
        trigger: BucketLevelTrigger,
        monitorRunResult: MonitorRunResult<BucketLevelTriggerRunResult>,
        dedupedAlerts: List<Alert> = listOf(),
        newAlerts: List<Alert> = listOf(),
        completedAlerts: List<Alert> = listOf()
    ) : this(
        monitor, trigger, monitorRunResult.inputResults.results, monitorRunResult.periodStart, monitorRunResult.periodEnd,
        dedupedAlerts, newAlerts, completedAlerts, monitorRunResult.scriptContextError(trigger)
    )

    /**
     * Mustache templates need special permissions to reflectively introspect field names. To avoid doing this we
     * translate the context to a Map of Strings to primitive types, which can be accessed without reflection.
     */
    override fun asTemplateArg(): Map<String, Any?> {
        val tempArg = super.asTemplateArg().toMutableMap()
        tempArg["trigger"] = trigger.asTemplateArg()
        tempArg["dedupedAlerts"] = dedupedAlerts.map { it.asTemplateArg() }
        tempArg["newAlerts"] = newAlerts.map { it.asTemplateArg() }
        tempArg["completedAlerts"] = completedAlerts.map { it.asTemplateArg() }
        return tempArg
    }
}
