/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.workflow

import org.opensearch.alerting.MonitorRunnerExecutionContext
import org.opensearch.alerting.model.WorkflowRunResult
import org.opensearch.commons.alerting.model.Workflow
import java.time.Instant

abstract class WorkflowRunner {
    abstract suspend fun runWorkflow(
        workflow: Workflow,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryRun: Boolean
    ): WorkflowRunResult
}
