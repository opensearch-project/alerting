package org.opensearch.alerting.core.modelv2

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import java.time.Instant

interface MonitorV2RunResult<out TriggerV2Result : TriggerV2RunResult> : Writeable, ToXContent {
    val monitorName: String
    val error: Exception?
    val periodStart: Instant
    val periodEnd: Instant
    val triggerResults: Map<String, TriggerV2Result>

    enum class MonitorV2RunResultType() {
        PPL_MONITOR_RUN_RESULT;
    }

    companion object {
        const val MONITOR_V2_NAME_FIELD = "monitor_v2_name"
        const val ERROR_FIELD = "error"
        const val PERIOD_START_FIELD = "period_start"
        const val PERIOD_END_FIELD = "period_end"
        const val TRIGGER_RESULTS_FIELD = "trigger_results"

        fun readFrom(sin: StreamInput): MonitorV2RunResult<TriggerV2RunResult> {
            val monitorRunResultType = sin.readEnum(MonitorV2RunResultType::class.java)
            return when (monitorRunResultType) {
                MonitorV2RunResultType.PPL_MONITOR_RUN_RESULT -> PPLMonitorRunResult(sin)
                else -> throw IllegalStateException("Unexpected input [$monitorRunResultType] when reading MonitorV2RunResult")
            }
        }

        fun writeTo(out: StreamOutput, monitorV2RunResult: MonitorV2RunResult<TriggerV2RunResult>) {
            when (monitorV2RunResult) {
                is PPLMonitorRunResult -> {
                    out.writeEnum(MonitorV2RunResultType.PPL_MONITOR_RUN_RESULT)
                    monitorV2RunResult.writeTo(out)
                }
            }
        }
    }
}
