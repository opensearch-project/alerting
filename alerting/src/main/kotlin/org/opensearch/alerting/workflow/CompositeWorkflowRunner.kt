/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.workflow

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.BucketLevelMonitorRunner
import org.opensearch.alerting.DocumentLevelMonitorRunner
import org.opensearch.alerting.MonitorRunnerExecutionContext
import org.opensearch.alerting.QueryLevelMonitorRunner
import org.opensearch.alerting.WorkflowMetadataService
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.WorkflowRunResult
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.isDocLevelMonitor
import org.opensearch.alerting.util.isQueryLevelMonitor
import org.opensearch.commons.alerting.model.CompositeInput
import org.opensearch.commons.alerting.model.Delegate
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.util.isBucketLevelMonitor
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.UUID

object CompositeWorkflowRunner : WorkflowRunner() {
    private val logger = LogManager.getLogger(javaClass)

    override suspend fun runWorkflow(
        workflow: Workflow,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryRun: Boolean
    ): WorkflowRunResult {
        val workflowExecutionStartTime = Instant.now()

        val isTempWorkflow = dryRun || workflow.id == Workflow.NO_ID

        val executionId = generateExecutionId(isTempWorkflow, workflow)

        val (workflowMetadata, _) = WorkflowMetadataService.getOrCreateWorkflowMetadata(
            workflow = workflow,
            skipIndex = isTempWorkflow,
            executionId = executionId
        )

        var workflowResult = WorkflowRunResult(mutableListOf(), workflowExecutionStartTime, null, executionId)

        logger.debug("Workflow ${workflow.id} in $executionId execution is running")
        val delegates = (workflow.inputs[0] as CompositeInput).sequence.delegates.sortedBy { it.order }
        var monitors: List<Monitor>

        try {
            monitors = monitorCtx.workflowService!!.getMonitorsById(delegates.map { it.monitorId }, delegates.size)
        } catch (e: Exception) {
            logger.error("Failed getting workflow delegates. Error: ${e.message}", e)
            return workflowResult.copy(error = AlertingException.wrap(e))
        }
        // Validate the monitors size
        validateMonitorSize(delegates, monitors, workflow)

        val monitorsById = monitors.associateBy { it.id }
        val resultList = mutableListOf<MonitorRunResult<*>>()
        var lastErrorDelegateRun: Exception? = null

        for (delegate in delegates) {
            var indexToDocIds = mapOf<String, List<String>>()
            var delegateMonitor: Monitor
            delegateMonitor = monitorsById[delegate.monitorId]
                ?: throw AlertingException.wrap(
                    IllegalStateException("Delegate monitor not found ${delegate.monitorId} for the workflow $workflow.id")
                )
            if (delegate.chainedMonitorFindings != null) {
                val chainedMonitor = monitorsById[delegate.chainedMonitorFindings!!.monitorId]
                    ?: throw AlertingException.wrap(
                        IllegalStateException("Chained finding monitor not found ${delegate.monitorId} for the workflow $workflow.id")
                    )

                try {
                    indexToDocIds = monitorCtx.workflowService!!.getFindingDocIdsByExecutionId(chainedMonitor, executionId)
                } catch (e: Exception) {
                    logger.error("Failed to execute workflow. Error: ${e.message}", e)
                    return workflowResult.copy(error = AlertingException.wrap(e))
                }
            }

            val workflowRunContext = WorkflowRunContext(
                workflowId = workflowMetadata.workflowId,
                workflowMetadataId = workflowMetadata.id,
                chainedMonitorId = delegate.chainedMonitorFindings?.monitorId,
                executionId = executionId,
                matchingDocIdsPerIndex = indexToDocIds
            )

            var delegateRunResult: MonitorRunResult<*>?
            try {
                delegateRunResult = if (delegateMonitor.isBucketLevelMonitor()) {
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
                } else if (delegateMonitor.isQueryLevelMonitor()) {
                    QueryLevelMonitorRunner.runMonitor(
                        delegateMonitor,
                        monitorCtx,
                        periodStart,
                        periodEnd,
                        dryRun,
                        workflowRunContext
                    )
                } else {
                    throw AlertingException.wrap(
                        IllegalStateException("Unsupported monitor type")
                    )
                }
            } catch (ex: Exception) {
                logger.error("Error executing workflow delegate. Error: ${ex.message}", ex)
                lastErrorDelegateRun = AlertingException.wrap(ex)
                continue
            }
            if (delegateRunResult != null) resultList.add(delegateRunResult)
        }
        logger.debug("Workflow ${workflow.id} in $executionId finished")
        // Update metadata only if the workflow is not temp
        if (!isTempWorkflow) {
            WorkflowMetadataService.upsertWorkflowMetadata(
                workflowMetadata.copy(latestRunTime = workflowExecutionStartTime, latestExecutionId = executionId),
                true
            )
        }

        return workflowResult.copy(workflowRunResult = resultList, executionEndTime = Instant.now(), error = lastErrorDelegateRun)
    }

    private fun generateExecutionId(
        isTempWorkflow: Boolean,
        workflow: Workflow,
    ): String {
        val randomPart = "${LocalDateTime.now(ZoneOffset.UTC)}${UUID.randomUUID()}"
        return if (isTempWorkflow) randomPart else workflow.id.plus(randomPart)
    }

    private fun validateMonitorSize(
        delegates: List<Delegate>,
        monitors: List<Monitor>,
        workflow: Workflow,
    ) {
        if (delegates.size != monitors.size) {
            val diffMonitorIds = delegates.map { it.monitorId }.minus(monitors.map { it.id }.toSet()).joinToString()
            logger.error("Delegate monitors don't exist $diffMonitorIds for the workflow $workflow.id")
            throw AlertingException.wrap(
                IllegalStateException("Delegate monitors don't exist $diffMonitorIds for the workflow $workflow.id")
            )
        }
    }
}
