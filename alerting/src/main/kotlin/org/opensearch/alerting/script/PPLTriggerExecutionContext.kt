/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.script

import org.json.JSONObject
import org.opensearch.alerting.modelv2.PPLSQLMonitor
import org.opensearch.alerting.modelv2.PPLSQLMonitorRunResult.Companion.PPL_QUERY_RESULTS_FIELD
import org.opensearch.alerting.modelv2.PPLSQLTrigger
import org.opensearch.alerting.modelv2.PPLSQLTrigger.Companion.PPL_SQL_TRIGGER_FIELD

data class PPLTriggerExecutionContext(
    override val monitorV2: PPLSQLMonitor,
    override val error: Exception? = null,
    val pplTrigger: PPLSQLTrigger,
    var pplQueryResults: JSONObject // can be a full set of PPL query results, or an individual result row
) : TriggerV2ExecutionContext(monitorV2, error) {

    override fun asTemplateArg(): Map<String, Any?> {
        val templateArg = super.asTemplateArg().toMutableMap()
        templateArg[PPL_SQL_TRIGGER_FIELD] = pplTrigger.asTemplateArg()
        templateArg[PPL_QUERY_RESULTS_FIELD] = pplQueryResults.toMap()
        return templateArg.toMap()
    }
}
