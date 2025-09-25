package org.opensearch.alerting

import org.opensearch.alerting.core.modelv2.MonitorV2
import org.opensearch.alerting.core.modelv2.MonitorV2RunResult
import org.opensearch.transport.TransportService
import java.time.Instant

interface MonitorV2Runner {
    suspend fun runMonitorV2(
        monitorV2: MonitorV2,
        monitorCtx: MonitorRunnerExecutionContext, // MonitorV2 reads from same context as Monitor
        periodStart: Instant,
        periodEnd: Instant,
        dryRun: Boolean,
        manual: Boolean,
        executionId: String,
        transportService: TransportService
    ): MonitorV2RunResult<*>
}
