package org.opensearch.alerting.script

import java.time.Instant
import org.opensearch.commons.alerting.model.MonitorV2
import org.opensearch.commons.alerting.model.PPLMonitor
import org.opensearch.commons.alerting.model.PPLMonitorRunResult
import org.opensearch.commons.alerting.model.PPLMonitorRunResult.Companion.PPL_QUERY_RESULTS_FIELD
import org.opensearch.commons.alerting.model.PPLTrigger
import org.opensearch.commons.alerting.model.PPLTrigger.Companion.PPL_TRIGGER_FIELD

data class PPLTriggerExecutionContext(
    override val monitorV2: MonitorV2,
    override val periodStart: Instant,
    override val periodEnd: Instant,
    override val error: Exception? = null,
    val pplTrigger: PPLTrigger,
    val pplQueryResults: Map<String, Any?> // keys are PPL query result fields, not trigger ID
    ) : TriggerV2ExecutionContext(monitorV2, periodStart, periodEnd, error) {

//        constructor(
//            pplMonitor: PPLMonitor,
//            pplTrigger: PPLTrigger,
//            pplMonitorRunResult: PPLMonitorRunResult
//        ) : this(
//            pplMonitor,
//            pplMonitorRunResult.periodStart,
//            pplMonitorRunResult.periodEnd,
//            pplMonitorRunResult.error,
//            pplTrigger,
//            pplMonitorRunResult.pplQueryResults[pplTrigger.id]!!
//        )

    override fun asTemplateArg(): Map<String, Any?> {
        val templateArg = super.asTemplateArg().toMutableMap()
        templateArg[PPL_TRIGGER_FIELD] = pplTrigger.asTemplateArg()
        templateArg[PPL_QUERY_RESULTS_FIELD] = pplQueryResults
        return templateArg.toMap()
    }
}
