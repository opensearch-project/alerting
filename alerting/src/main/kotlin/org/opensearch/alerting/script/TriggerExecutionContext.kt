/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.script

import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorRunResult
import org.opensearch.commons.alerting.model.Trigger
import java.time.Instant

abstract class TriggerExecutionContext(
    open val monitor: Monitor,
    open val results: List<Map<String, Any>>,
    open val periodStart: Instant,
    open val periodEnd: Instant,
    open val error: Exception? = null,
    open val clusterSettings: ClusterSettings
) {
    val results: List<Map<String, Any>>
        get() = {
            val resultsAllowedRoles = clusterSettings.get(
                AlertingSettings.NOTIFICATION_CONTEXT_RESULTS_ALLOWED_ROLES
            )
            if (resultsAllowedRoles.isEmpty() || resultsAllowedRoles.intersect(monitor.user!!.roles).isNotEmpty()) {
                return monitorRunResult.inputResults.results
            }
            return listOf()
        }

    constructor(monitor: Monitor, trigger: Trigger, monitorRunResult: MonitorRunResult<*>, clusterSettings) :
        this(
            monitor,
            monitorRunResult.inputResults.results,
            monitorRunResult.periodStart,
            monitorRunResult.periodEnd,
            monitorRunResult.scriptContextError(trigger),
            clusterSettings
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
