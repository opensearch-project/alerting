/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.script

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.model.AlertContext
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.BucketLevelTrigger
import org.opensearch.commons.alerting.model.BucketLevelTriggerRunResult
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorRunResult
import java.time.Instant

private val logger = LogManager.getLogger(BucketLevelTriggerExecutionContext::class.java)

data class BucketLevelTriggerExecutionContext(
    override val monitor: Monitor,
    val trigger: BucketLevelTrigger,
    override val results: List<Map<String, Any>>,
    override val periodStart: Instant,
    override val periodEnd: Instant,
    val dedupedAlerts: List<Alert> = listOf(),
    val newAlerts: List<AlertContext> = listOf(),
    val completedAlerts: List<Alert> = listOf(),
    override val error: Exception? = null
) : TriggerExecutionContext(monitor, results, periodStart, periodEnd, error) {

    constructor(
        monitor: Monitor,
        trigger: BucketLevelTrigger,
        monitorRunResult: MonitorRunResult<BucketLevelTriggerRunResult>,
        dedupedAlerts: List<Alert> = listOf(),
        newAlerts: List<AlertContext> = listOf(),
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
        tempArg[TRIGGER_FIELD] = trigger.asTemplateArg()
        tempArg[DEDUPED_ALERTS_FIELD] = dedupedAlerts.map { it.asTemplateArg() }
        tempArg[NEW_ALERTS_FIELD] = newAlerts.map { it.asTemplateArg() }
        tempArg[COMPLETED_ALERTS_FIELD] = completedAlerts.map { it.asTemplateArg() }
        tempArg[RESULTS_FIELD] = results
        return tempArg
    }

    companion object {
        const val TRIGGER_FIELD = "trigger"
        const val DEDUPED_ALERTS_FIELD = "dedupedAlerts"
        const val NEW_ALERTS_FIELD = "newAlerts"
        const val COMPLETED_ALERTS_FIELD = "completedAlerts"
        const val RESULTS_FIELD = "results"
    }
}
