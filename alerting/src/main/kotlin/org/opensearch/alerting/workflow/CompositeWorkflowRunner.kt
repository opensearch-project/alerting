package org.opensearch.alerting.workflow

import org.opensearch.alerting.MonitorRunnerExecutionContext
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.commons.alerting.model.Workflow
import java.time.Instant

class CompositeWorkflowRunner : WorkflowRunner() {
    override suspend fun runWorkflow(
        workflow: Workflow,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryRun: Boolean
    ): MonitorRunResult<*> {
        TODO("Not yet implemented")
    }

    companion object {
        fun runWorkflow(
            workflow: Workflow,
            monitorCtx: MonitorRunnerExecutionContext,
            periodStart: Instant,
            periodEnd: Instant,
            dryrun: Boolean
        ): MonitorRunResult<*> {
            TODO("Not yet implemented")
        }
    }
}
