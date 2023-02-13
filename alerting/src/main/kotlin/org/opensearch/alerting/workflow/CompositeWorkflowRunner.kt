/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.workflow

import org.opensearch.alerting.BucketLevelMonitorRunner
import org.opensearch.alerting.DocumentLevelMonitorRunner
import org.opensearch.alerting.MonitorRunnerExecutionContext
import org.opensearch.alerting.QueryLevelMonitorRunner
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.util.isDocLevelMonitor
import org.opensearch.commons.alerting.model.CompositeInput
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.util.isBucketLevelMonitor
import java.time.Instant
import java.util.UUID

object CompositeWorkflowRunner : WorkflowRunner() {

    override suspend fun runWorkflow(
        workflow: Workflow,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryRun: Boolean
    ): List<MonitorRunResult<*>> {
        val workflowExecutionId = UUID.randomUUID().toString()

        val delegates = (workflow.inputs[0] as CompositeInput).sequence.delegates.sortedBy { it.order }
        // Fetch monitors by ids
        val monitors = monitorCtx.workflowService!!.searchMonitors(delegates.map { it.monitorId }, delegates.size, workflow.owner)

        // Validate the monitors size
        if (delegates.size != monitors.size) {
            val diffMonitorIds = delegates.map { it.monitorId }.minus(monitors.map { it.id }.toSet()).joinToString()
            throw IllegalStateException("Delegate monitors don't exist $diffMonitorIds")
        }

        val monitorsById = monitors.associateBy { it.id }
        val resultList = mutableListOf<MonitorRunResult<*>>()

        for (delegate in delegates) {
            var delegateMonitor = monitorsById[delegate.monitorId]
                ?: throw IllegalStateException("Delegate monitor not found ${delegate.monitorId}")

            var indexToDocIds = mapOf<String, List<String>>()
            if (delegate.chainedFindings != null) {
                val chainedMonitor = monitorsById[delegate.chainedFindings!!.monitorId]
                    ?: throw IllegalStateException("Chained finding monitor not found ${delegate.monitorId}")

                indexToDocIds = monitorCtx.workflowService!!.getFindingDocIdsPerMonitorExecution(chainedMonitor, workflowExecutionId)
            }

            val workflowRunContext = WorkflowRunContext(delegate.chainedFindings?.monitorId, workflowExecutionId, indexToDocIds)

            val runResult = if (delegateMonitor.isBucketLevelMonitor()) {
                BucketLevelMonitorRunner.runMonitor(
                    delegateMonitor,
                    monitorCtx,
                    periodStart,
                    periodEnd,
                    dryRun,
                    workflowRunContext
                )
            } else if (delegateMonitor.isDocLevelMonitor()) {
                DocumentLevelMonitorRunner.runMonitor(
                    delegateMonitor,
                    monitorCtx,
                    periodStart,
                    periodEnd,
                    dryRun,
                    workflowRunContext
                )
            } else {
                QueryLevelMonitorRunner.runMonitor(
                    delegateMonitor,
                    monitorCtx,
                    periodStart,
                    periodEnd,
                    dryRun,
                    workflowRunContext
                )
            }
            resultList.add(runResult)
        }
        return resultList
    }
}
