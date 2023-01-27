package org.opensearch.alerting.workflow

import org.opensearch.alerting.MonitorRunnerExecutionContext
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.commons.alerting.model.Workflow
import java.time.Instant

abstract class WorkflowRunner {
    abstract suspend fun runWorkflow(
        workflow: Workflow,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryRun: Boolean
    ): MonitorRunResult<*>
}
