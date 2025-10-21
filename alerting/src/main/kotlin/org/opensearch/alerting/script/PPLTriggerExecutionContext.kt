/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.script

import org.json.JSONObject
import org.opensearch.alerting.modelv2.PPLMonitor
import org.opensearch.alerting.modelv2.PPLMonitorRunResult.Companion.PPL_QUERY_RESULTS_FIELD
import org.opensearch.alerting.modelv2.PPLTrigger
import org.opensearch.alerting.modelv2.PPLTrigger.Companion.PPL_TRIGGER_FIELD

data class PPLTriggerExecutionContext(
    override val monitorV2: PPLMonitor,
    override val error: Exception? = null,
    val pplTrigger: PPLTrigger,
    var pplQueryResults: JSONObject // can be a full set of PPL query results, or an individual result row
) : TriggerV2ExecutionContext(monitorV2, error) {

    override fun asTemplateArg(): Map<String, Any?> {
        val templateArg = super.asTemplateArg().toMutableMap()
        templateArg[PPL_TRIGGER_FIELD] = pplTrigger.asTemplateArg()
        templateArg[PPL_QUERY_RESULTS_FIELD] = pplQueryResults.toMap()
        return templateArg.toMap()
    }
}
