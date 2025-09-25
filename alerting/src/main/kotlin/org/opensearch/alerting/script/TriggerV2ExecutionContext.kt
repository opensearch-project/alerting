package org.opensearch.alerting.script

import org.opensearch.alerting.core.modelv2.MonitorV2
import org.opensearch.alerting.core.modelv2.MonitorV2RunResult
import org.opensearch.alerting.core.modelv2.TriggerV2
import java.time.Instant

abstract class TriggerV2ExecutionContext(
    open val monitorV2: MonitorV2,
    open val periodStart: Instant,
    open val periodEnd: Instant,
    open val error: Exception? = null
) {

    constructor(monitorV2: MonitorV2, triggerV2: TriggerV2, monitorV2RunResult: MonitorV2RunResult<*>) :
        this(
            monitorV2,
            monitorV2RunResult.periodStart,
            monitorV2RunResult.periodEnd,
            monitorV2RunResult.triggerResults[triggerV2.id]?.error
        )

    open fun asTemplateArg(): Map<String, Any?> {
        return mapOf(
            "monitorV2" to monitorV2.asTemplateArg(),
            "periodStart" to periodStart,
            "periodEnd" to periodEnd,
            "error" to error
        )
    }
}
