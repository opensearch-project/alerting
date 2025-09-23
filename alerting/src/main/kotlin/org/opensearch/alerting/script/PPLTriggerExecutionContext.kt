package org.opensearch.alerting.script

import org.json.JSONObject
import org.opensearch.alerting.core.modelv2.PPLMonitor
import org.opensearch.alerting.core.modelv2.PPLMonitorRunResult.Companion.PPL_QUERY_RESULTS_FIELD
import org.opensearch.alerting.core.modelv2.PPLTrigger
import org.opensearch.alerting.core.modelv2.PPLTrigger.Companion.PPL_TRIGGER_FIELD
import java.time.Instant

data class PPLTriggerExecutionContext(
    override val monitorV2: PPLMonitor,
    override val periodStart: Instant,
    override val periodEnd: Instant,
    override val error: Exception? = null,
    val pplTrigger: PPLTrigger,
    var pplQueryResults: JSONObject // can be a full set of PPL query results, or an individual result row
) : TriggerV2ExecutionContext(monitorV2, periodStart, periodEnd, error) {

    override fun asTemplateArg(): Map<String, Any?> {
        val templateArg = super.asTemplateArg().toMutableMap()
        templateArg[PPL_TRIGGER_FIELD] = pplTrigger.asTemplateArg()
        templateArg[PPL_QUERY_RESULTS_FIELD] = pplQueryResults.toMap()
        return templateArg.toMap()
    }
}
