/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.modelv2

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent

/**
 * Monitor V2 run result interface. All classes that store the results
 * of a monitor v2 run must implement this interface
 *
 * @opensearch.experimental
 */
interface MonitorV2RunResult<TriggerV2Result : TriggerV2RunResult> : Writeable, ToXContent {
    val monitorName: String
    val error: Exception?
    val triggerResults: Map<String, TriggerV2Result>

    enum class MonitorV2RunResultType {
        PPL_SQL_MONITOR_RUN_RESULT;
    }

    companion object {
        const val ERROR_FIELD = "error"
        const val TRIGGER_RESULTS_FIELD = "trigger_results"

        fun readFrom(sin: StreamInput): MonitorV2RunResult<*> {
            val monitorRunResultType = sin.readEnum(MonitorV2RunResultType::class.java)
            return when (monitorRunResultType) {
                MonitorV2RunResultType.PPL_SQL_MONITOR_RUN_RESULT -> PPLSQLMonitorRunResult(sin)
                else -> throw IllegalStateException("Unexpected input [$monitorRunResultType] when reading MonitorV2RunResult")
            }
        }

        fun writeTo(out: StreamOutput, monitorV2RunResult: MonitorV2RunResult<*>) {
            when (monitorV2RunResult) {
                is PPLSQLMonitorRunResult -> {
                    out.writeEnum(MonitorV2RunResultType.PPL_SQL_MONITOR_RUN_RESULT)
                    monitorV2RunResult.writeTo(out)
                }
            }
        }
    }
}
