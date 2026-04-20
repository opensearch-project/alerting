/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.script

import org.opensearch.alerting.model.AlertContext
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorRunResult
import org.opensearch.commons.alerting.model.PPLTrigger
import org.opensearch.commons.alerting.model.QueryLevelTrigger
import org.opensearch.commons.alerting.model.QueryLevelTriggerRunResult
import org.opensearch.commons.alerting.model.Trigger
import java.time.Instant

data class QueryLevelTriggerExecutionContext(
    override val monitor: Monitor,
    val trigger: Trigger,
    override val results: List<Map<String, Any>>,
    val pplQueryResults: List<Map<String, Any?>>, // each list element is a result row
    override val periodStart: Instant,
    override val periodEnd: Instant,
    val alert: AlertContext? = null,
    override val error: Exception? = null,
    override val clusterSettings: ClusterSettings,
) : TriggerExecutionContext(monitor, results, periodStart, periodEnd, error, clusterSettings) {

    init {
        require(trigger is QueryLevelTrigger || trigger is PPLTrigger) {
            "QueryLevelTriggerExecutionContext must only store Triggers for per-query style monitoring, " +
                "like QueryLevelTrigger or PPLTrigger"
        }
    }

    constructor(
        monitor: Monitor,
        trigger: Trigger,
        monitorRunResult: MonitorRunResult<QueryLevelTriggerRunResult>,
        alertContext: AlertContext? = null,
        clusterSettings: ClusterSettings
    ) : this(
        monitor,
        trigger,
        monitorRunResult.inputResults.results,
        // PPL Alerting: this empty list is overridden post PPL Trigger execution
        listOf(),
        monitorRunResult.periodStart,
        monitorRunResult.periodEnd,
        alertContext,
        monitorRunResult.scriptContextError(trigger),
        clusterSettings
    )

    /**
     * Mustache templates need special permissions to reflectively introspect field names. To avoid doing this we
     * translate the context to a Map of Strings to primitive types, which can be accessed without reflection.
     */
    override fun asTemplateArg(): Map<String, Any?> {
        val tempArg = super.asTemplateArg().toMutableMap()
        tempArg["trigger"] = trigger.asTemplateArg()
        tempArg["alert"] = alert?.asTemplateArg() // map "alert" templateArg field to AlertContext wrapper instead of Alert object
        tempArg["ppl_query_results"] = pplQueryResults
        return tempArg
    }
}
