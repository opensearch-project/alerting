/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.modelv2.MonitorV2RunResult
import org.opensearch.transport.TransportService
import java.time.Instant

/**
 * Interface for monitor V2 runners. All monitor v2 runner classes that house
 * a specific v2 monitor type's execution logic must implement this interface.
 *
 * @opensearch.experimental
 */
interface MonitorV2Runner {
    suspend fun runMonitorV2(
        monitorV2: MonitorV2,
        monitorCtx: MonitorRunnerExecutionContext, // MonitorV2 reads from same context as Monitor does
        periodEnd: Instant,
        dryRun: Boolean,
        manual: Boolean,
        executionId: String,
        transportService: TransportService
    ): MonitorV2RunResult<*>
}
