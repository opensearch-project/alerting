package org.opensearch.alerting

import org.opensearch.alerting.model.ActionRunResult
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.Trigger
import org.opensearch.alerting.script.TriggerExecutionContext
import java.time.Instant
import org.opensearch.alerting.model.action.Action

interface MonitorRunner {

    suspend fun runMonitor(monitor: Monitor, monitorCtx: MonitorRunnerExecutionContext, periodStart: Instant, periodEnd: Instant, dryRun: Boolean) : MonitorRunResult<*>;

    suspend fun runAction(action: Action, ctx: TriggerExecutionContext, monitorCtx: MonitorRunnerExecutionContext, dryRun: Boolean): ActionRunResult;
}
