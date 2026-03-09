/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.script

import org.opensearch.common.settings.ClusterSettings
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.PPLSQLTrigger
import org.opensearch.commons.alerting.model.PPLSQLTrigger.Companion.PPL_SQL_TRIGGER_FIELD
import java.time.Instant

data class PPLTriggerExecutionContext(
    override val monitor: Monitor,
    override val error: Exception? = null,
    override val results: List<Map<String, Any>>,
    override val periodStart: Instant,
    override val periodEnd: Instant,
    override val clusterSettings: ClusterSettings,
    val pplTrigger: PPLSQLTrigger,
) : TriggerExecutionContext(monitor, results, periodStart, periodEnd, error, clusterSettings) {

    override fun asTemplateArg(): Map<String, Any?> {
        val templateArg = super.asTemplateArg().toMutableMap()
        templateArg[PPL_SQL_TRIGGER_FIELD] = pplTrigger.asTemplateArg()
        templateArg[PPL_QUERY_RESULTS_FIELD] = results[0] // PPL/SQL Monitors only ever return one set of results
        return templateArg.toMap()
    }

    companion object {
        const val PPL_QUERY_RESULTS_FIELD = "ppl_query_results"
    }
}
