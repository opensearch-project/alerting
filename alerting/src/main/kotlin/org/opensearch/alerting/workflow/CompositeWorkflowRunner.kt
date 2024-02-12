/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.workflow

import org.apache.logging.log4j.LogManager
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.BucketLevelMonitorRunner
import org.opensearch.alerting.DocumentLevelMonitorRunner
import org.opensearch.alerting.MonitorRunnerExecutionContext
import org.opensearch.alerting.QueryLevelMonitorRunner
import org.opensearch.alerting.WorkflowMetadataService
import org.opensearch.alerting.model.ChainedAlertTriggerRunResult
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.WorkflowRunResult
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.script.ChainedAlertTriggerExecutionContext
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.isDocLevelMonitor
import org.opensearch.alerting.util.isQueryLevelMonitor
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.ChainedAlertTrigger
import org.opensearch.commons.alerting.model.CompositeInput
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.Delegate
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.StreamingIndex
import org.opensearch.commons.alerting.model.Trigger
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.util.isBucketLevelMonitor
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.query.QueryBuilders.boolQuery
import org.opensearch.index.query.QueryBuilders.existsQuery
import org.opensearch.index.query.QueryBuilders.termsQuery
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
        dryRun: Boolean,
    ): WorkflowRunResult {
        val workflowExecutionStartTime = Instant.now()

        val isTempWorkflow = dryRun || workflow.id == Workflow.NO_ID

        val executionId = generateExecutionId(isTempWorkflow, workflow)

        val (workflowMetadata, _) = WorkflowMetadataService.getOrCreateWorkflowMetadata(
            workflow = workflow,
            skipIndex = isTempWorkflow,
            executionId = executionId
        )
        var dataSources: DataSources? = null
        logger.debug("Workflow ${workflow.id} in $executionId execution is running")
        val delegates = (workflow.inputs[0] as CompositeInput).sequence.delegates.sortedBy { it.order }
        val monitors: List<Monitor>

        try {
            monitors = monitorCtx.workflowService!!.getMonitorsById(delegates.map { it.monitorId }, delegates.size)
        } catch (e: Exception) {
            logger.error("Failed getting workflow delegates. Error: ${e.message}", e)
            return buildWorkflowRunResult(workflow, workflowExecutionStartTime, executionId, AlertingException.wrap(e))
        }
        // Validate the monitors size
        validateMonitorSize(delegates, monitors, workflow)
        val monitorsById = monitors.associateBy { it.id }
        val resultList = mutableListOf<MonitorRunResult<*>>()
        var lastErrorDelegateRun: Exception? = null

        for (delegate in delegates) {
            val delegateMonitor: Monitor = monitorsById[delegate.monitorId]
                ?: throw AlertingException.wrap(
                    IllegalStateException("Delegate monitor not found ${delegate.monitorId} for the workflow $workflow.id")
                )

            val workflowRunContext: WorkflowRunContext
            try {
                workflowRunContext = getWorkflowRunContext(delegate, monitorCtx, workflow, monitorsById, executionId, workflowMetadata.id)
            } catch (e: Exception) {
                return buildWorkflowRunResult(workflow, workflowExecutionStartTime, executionId, AlertingException.wrap(e))
            }

            try {
                dataSources = delegateMonitor.dataSources
                val delegateRunResult =
                    runDelegateMonitor(delegateMonitor, monitorCtx, periodStart, periodEnd, dryRun, workflowRunContext, executionId)
                resultList.add(delegateRunResult!!)
            } catch (ex: Exception) {
                logger.error("Error executing workflow delegate monitor ${delegate.monitorId}", ex)
                lastErrorDelegateRun = AlertingException.wrap(ex)
                break
            }
        }
        logger.debug("Workflow ${workflow.id} delegate monitors in execution $executionId completed")
        // Update metadata only if the workflow is not temp
        if (!isTempWorkflow) {
            WorkflowMetadataService.upsertWorkflowMetadata(
                workflowMetadata.copy(latestRunTime = workflowExecutionStartTime, latestExecutionId = executionId),
                true
            )
        }

        val workflowRunResult = runTriggersAndPopulateWorkflowResult(
            workflow, monitorCtx, workflowExecutionStartTime, executionId, dryRun, dataSources, resultList, lastErrorDelegateRun
        )
        refreshAlertsIndex(dataSources, monitorCtx)

        return workflowRunResult
    }

    private fun buildWorkflowRunResult(
        workflow: Workflow,
        workflowExecutionStartTime: Instant,
        executionId: String,
        error: Exception?,
        monitorRunResults: List<MonitorRunResult<*>> = emptyList(),
        triggerResults: Map<String, ChainedAlertTriggerRunResult> = emptyMap()
    ): WorkflowRunResult {
        return WorkflowRunResult(
            workflowId = workflow.id,
            workflowName = workflow.name,
            monitorRunResults = monitorRunResults,
            executionStartTime = workflowExecutionStartTime,
            executionEndTime = Instant.now(),
            executionId = executionId,
            error = error,
            triggerResults = triggerResults
        )
    }

    private suspend fun getWorkflowRunContext(
        delegate: Delegate,
        monitorCtx: MonitorRunnerExecutionContext,
        workflow: Workflow,
        monitorsById: Map<String, Monitor>,
        executionId: String,
        workflowMetadataId: String
    ): WorkflowRunContext {
        var indexToDocIds = mapOf<String, List<String>>()
        if (delegate.chainedMonitorFindings != null) {
            val chainedMonitorIds: MutableList<String> = mutableListOf()
            if (delegate.chainedMonitorFindings!!.monitorId.isNullOrBlank()) {
                chainedMonitorIds.addAll(delegate.chainedMonitorFindings!!.monitorIds)
            } else {
                chainedMonitorIds.add(delegate.chainedMonitorFindings!!.monitorId!!)
            }
            val chainedMonitors = mutableListOf<Monitor>()
            chainedMonitorIds.forEach {
                val chainedMonitor = monitorsById[it]
                    ?: throw IllegalStateException("Chained finding monitor not found ${delegate.monitorId} for the workflow $workflow.id")
                chainedMonitors.add(chainedMonitor)
            }

            try {
                indexToDocIds = monitorCtx.workflowService!!.getFindingDocIdsByExecutionId(chainedMonitors, executionId)
            } catch (e: Exception) {
                logger.error("Failed to execute workflow due to failure in chained findings.  Error: ${e.message}", e)
                throw e
            }
        }
        return WorkflowRunContext(
            workflowId = workflow.id,
            workflowMetadataId = workflowMetadataId,
            chainedMonitorId = delegate.chainedMonitorFindings?.monitorId,
            matchingDocIdsPerIndex = indexToDocIds,
            auditDelegateMonitorAlerts = if (workflow.auditDelegateMonitorAlerts == null) true
            else workflow.auditDelegateMonitorAlerts!!
        )
    }

    private suspend fun runDelegateMonitor(
        delegateMonitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryRun: Boolean,
        workflowRunContext: WorkflowRunContext,
        executionId: String,
    ): MonitorRunResult<*>? {

        if (delegateMonitor.isBucketLevelMonitor()) {
            return BucketLevelMonitorRunner.runMonitor(
                delegateMonitor,
                monitorCtx,
                periodStart,
                periodEnd,
                dryRun,
                workflowRunContext,
                executionId
            )
        } else if (delegateMonitor.isDocLevelMonitor()) {
            return DocumentLevelMonitorRunner.runMonitor(
                delegateMonitor,
                monitorCtx,
                periodStart,
                periodEnd,
                dryRun,
                workflowRunContext,
                executionId
            )
        } else if (delegateMonitor.isQueryLevelMonitor()) {
            return QueryLevelMonitorRunner.runMonitor(
                delegateMonitor,
                monitorCtx,
                periodStart,
                periodEnd,
                dryRun,
                workflowRunContext,
                executionId
            )
        } else {
            throw AlertingException.wrap(
                IllegalStateException("Unsupported monitor type ${delegateMonitor.monitorType}")
            )
        }
    }

    private suspend fun getCurrentAlerts(
        workflow: Workflow,
        monitorCtx: MonitorRunnerExecutionContext,
        dataSources: DataSources
    ): Map<Trigger, Alert?> {
        try {
            monitorCtx.alertIndices!!.createOrUpdateAlertIndex(dataSources)
            monitorCtx.alertIndices!!.createOrUpdateInitialAlertHistoryIndex(dataSources)
            return monitorCtx.alertService!!.loadCurrentAlertsForWorkflow(workflow, dataSources)
        } catch (e: Exception) {
            logger.error("Failed to fetch current alerts for workflow", e)
            // We can't save ERROR alerts to the index here as we don't know if there are existing ACTIVE alerts
            val id = if (workflow.id.trim().isEmpty()) "_na_" else workflow.id
            logger.error("Error loading alerts for workflow: $id", e)
            throw e
        }
    }

    private suspend fun runTriggersAndPopulateWorkflowResult(
        workflow: Workflow,
        monitorCtx: MonitorRunnerExecutionContext,
        workflowExecutionStartTime: Instant,
        executionId: String,
        dryRun: Boolean,
        dataSources: DataSources?,
        resultList: List<MonitorRunResult<*>>,
        lastErrorDelegateRun: Exception?
    ): WorkflowRunResult {
        val triggerResults = mutableMapOf<String, ChainedAlertTriggerRunResult>()
        val workflowRunResult = buildWorkflowRunResult(
            workflow, workflowExecutionStartTime, executionId, lastErrorDelegateRun, resultList, triggerResults
        )
        workflowRunResult.executionEndTime = null
        val currentAlerts: Map<Trigger, Alert?>
        try {
            currentAlerts = getCurrentAlerts(workflow, monitorCtx, dataSources!!)
        } catch (e: Exception) {
            return buildWorkflowRunResult(workflow, workflowExecutionStartTime, executionId, e, resultList, triggerResults)
        }
        try {
            runTriggers(workflow, monitorCtx, dataSources, executionId, dryRun, workflowRunResult, currentAlerts, triggerResults)
        } catch (e: Exception) {
            return buildWorkflowRunResult(workflow, workflowExecutionStartTime, executionId, AlertingException.wrap(e))
        }

        workflowRunResult.executionEndTime = Instant.now()
        return workflowRunResult
    }

    private suspend fun runTriggers(
        workflow: Workflow,
        monitorCtx: MonitorRunnerExecutionContext,
        dataSources: DataSources,
        executionId: String,
        dryRun: Boolean,
        workflowRunResult: WorkflowRunResult,
        currentAlerts: Map<Trigger, Alert?>,
        triggerResults: MutableMap<String, ChainedAlertTriggerRunResult>
    ) {
        try {
            monitorCtx.alertIndices!!.createOrUpdateAlertIndex(dataSources)
            val updatedAlerts = mutableListOf<Alert>()
            val monitorIdToAlertIdsMap = fetchAlertsGeneratedInCurrentExecution(dataSources, executionId, monitorCtx, workflow)
            for (trigger in workflow.triggers) {
                val currentAlert = currentAlerts[trigger]
                val caTrigger = trigger as ChainedAlertTrigger
                val triggerCtx = ChainedAlertTriggerExecutionContext(
                    workflow = workflow,
                    workflowRunResult = workflowRunResult,
                    periodStart = workflowRunResult.executionStartTime,
                    periodEnd = workflowRunResult.executionEndTime,
                    trigger = caTrigger,
                    alertGeneratingMonitors = monitorIdToAlertIdsMap.keys,
                    monitorIdToAlertIdsMap = monitorIdToAlertIdsMap,
                    alert = currentAlert
                )
                runChainedAlertTrigger(
                    monitorCtx,
                    workflow,
                    trigger,
                    executionId,
                    triggerCtx,
                    dryRun,
                    triggerResults,
                    updatedAlerts
                )
            }
            if (!dryRun && workflow.id != Workflow.NO_ID && updatedAlerts.isNotEmpty()) {
                monitorCtx.retryPolicy?.let {
                    monitorCtx.alertService!!.saveAlerts(
                        dataSources,
                        updatedAlerts,
                        it,
                        routingId = workflow.id
                    )
                }
            }
        } catch (e: Exception) {
            // We can't save ERROR alerts to the index here as we don't know if there are existing ACTIVE alerts
            val id = if (workflow.id.trim().isEmpty()) "_na_" else workflow.id
            logger.error("Error loading current chained alerts for workflow: $id", e)
            throw e
        }
    }

    private suspend fun refreshAlertsIndex(dataSources: DataSources?, monitorCtx: MonitorRunnerExecutionContext) {
        val sr = SearchRequest(dataSources!!.alertsIndex)
        sr.source().query(QueryBuilders.matchAllQuery()).size(10)
        val searchResponse: SearchResponse = monitorCtx.client!!.suspendUntil { monitorCtx.client!!.search(sr, it) }
        searchResponse.hits
    }

    fun generateExecutionId(
        isTempWorkflow: Boolean,
        workflow: Workflow,
    ): String {
        val randomPart = "_${LocalDateTime.now(ZoneOffset.UTC)}_${UUID.randomUUID()}"
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

    private suspend fun runChainedAlertTrigger(
        monitorCtx: MonitorRunnerExecutionContext,
        workflow: Workflow,
        trigger: ChainedAlertTrigger,
        executionId: String,
        triggerCtx: ChainedAlertTriggerExecutionContext,
        dryRun: Boolean,
        triggerResults: MutableMap<String, ChainedAlertTriggerRunResult>,
        updatedAlerts: MutableList<Alert>,
    ) {
        val triggerRunResult = monitorCtx.triggerService!!.runChainedAlertTrigger(
            workflow, trigger, triggerCtx.alertGeneratingMonitors, triggerCtx.monitorIdToAlertIdsMap
        )
        triggerResults[trigger.id] = triggerRunResult
        if (monitorCtx.triggerService!!.isChainedAlertTriggerActionable(triggerCtx, triggerRunResult)) {
            val actionCtx = triggerCtx
            for (action in trigger.actions) {
                triggerRunResult.actionResults[action.id] = this.runAction(action, actionCtx, monitorCtx, workflow, dryRun)
            }
        }
        val alert = monitorCtx.alertService!!.composeChainedAlert(
            triggerCtx, executionId, workflow, triggerRunResult.associatedAlertIds.toList(), triggerRunResult
        )
        if (alert != null) {
            updatedAlerts.add(alert)
        }
    }

    private suspend fun fetchAlertsGeneratedInCurrentExecution(
        dataSources: DataSources,
        executionId: String,
        monitorCtx: MonitorRunnerExecutionContext,
        workflow: Workflow,
    ): MutableMap<String, MutableSet<String>> {
        try {
            val searchRequest =
                SearchRequest(getDelegateMonitorAlertIndex(dataSources, workflow, monitorCtx.alertIndices!!.isAlertHistoryEnabled()))
            val queryBuilder = boolQuery()
            queryBuilder.must(QueryBuilders.termQuery("execution_id", executionId))
            queryBuilder.must(QueryBuilders.termQuery("state", getDelegateMonitorAlertState(workflow)))
            val noErrorQuery = boolQuery()
                .should(boolQuery().mustNot(existsQuery(Alert.ERROR_MESSAGE_FIELD)))
                .should(termsQuery(Alert.ERROR_MESSAGE_FIELD, ""))
            queryBuilder.must(noErrorQuery)
            searchRequest.source().query(queryBuilder).size(9999)
            val searchResponse: SearchResponse = monitorCtx.client!!.suspendUntil { monitorCtx.client!!.search(searchRequest, it) }
            val alerts = searchResponse.hits.map { hit ->
                val xcp = XContentHelper.createParser(
                    monitorCtx.xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    hit.sourceRef, XContentType.JSON
                )
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                val alert = Alert.parse(xcp, hit.id, hit.version)
                alert
            }
            val map = mutableMapOf<String, MutableSet<String>>()
            for (alert in alerts) {
                if (map.containsKey(alert.monitorId)) {
                    map[alert.monitorId]!!.add(alert.id)
                } else {
                    map[alert.monitorId] = mutableSetOf(alert.id)
                }
            }
            return map
        } catch (e: Exception) {
            logger.error("failed to get alerts generated by delegate monitors in current execution $executionId", e)
            return mutableMapOf()
        }
    }

    fun getDelegateMonitorAlertIndex(
        dataSources: DataSources,
        workflow: Workflow,
        isAlertHistoryEnabled: Boolean,
    ): String {
        return if (workflow.triggers.isNotEmpty()) {
            if (isAlertHistoryEnabled) {
                dataSources.alertsHistoryIndex!!
            } else dataSources.alertsIndex
        } else dataSources.alertsIndex
    }

    fun getDelegateMonitorAlertState(
        workflow: Workflow,
    ): Alert.State {
        return if (workflow.triggers.isNotEmpty()) {
            Alert.State.AUDIT
        } else Alert.State.ACTIVE
    }

    override suspend fun runStreamingWorkflow(
        workflow: Workflow,
        monitorCtx: MonitorRunnerExecutionContext,
        dryRun: Boolean,
        streamingIndices: List<StreamingIndex>
    ): WorkflowRunResult {
        val workflowExecutionStartTime = Instant.now()
        val isTempWorkflow = dryRun || workflow.id == Workflow.NO_ID
        val executionId = generateExecutionId(isTempWorkflow, workflow)
        logger.debug("Workflow ${workflow.id} in $executionId execution is running")

        var dataSources: DataSources? = null
        val delegates = (workflow.inputs[0] as CompositeInput).sequence.delegates.sortedBy { it.order }
        val monitors: List<Monitor>
        try {
            monitors = getMonitors(monitorCtx, delegates)
        } catch (e: Exception) {
            return buildWorkflowRunResult(workflow, workflowExecutionStartTime, executionId, AlertingException.wrap(e))
        }

        // Validate the monitors size
        validateMonitorSize(delegates, monitors, workflow)
        val monitorsById = monitors.associateBy { it.id }
        val resultList = mutableListOf<MonitorRunResult<*>>()
        var lastErrorDelegateRun: Exception? = null

        for (delegate in delegates) {
            val delegateMonitor: Monitor = monitorsById[delegate.monitorId]
                ?: throw AlertingException.wrap(
                    IllegalStateException("Delegate monitor not found ${delegate.monitorId} for the workflow $workflow.id")
                )

            val workflowRunContext: WorkflowRunContext
            try {
                workflowRunContext = getWorkflowRunContext(delegate, monitorCtx, workflow, monitorsById, executionId, "")
            } catch (e: Exception) {
                return buildWorkflowRunResult(workflow, workflowExecutionStartTime, executionId, AlertingException.wrap(e))
            }

            try {
                dataSources = delegateMonitor.dataSources
                val delegateRunResult =
                    runDelegateStreamingMonitor(delegateMonitor, monitorCtx, dryRun, workflowRunContext, executionId, streamingIndices)
                resultList.add(delegateRunResult)
            } catch (ex: Exception) {
                logger.error("Error executing workflow delegate monitor ${delegate.monitorId}", ex)
                lastErrorDelegateRun = AlertingException.wrap(ex)
                break
            }
        }

        logger.debug("Workflow ${workflow.id} delegate streaming monitors in execution $executionId completed")
        val workflowRunResult = runTriggersAndPopulateWorkflowResult(
            workflow, monitorCtx, workflowExecutionStartTime, executionId, dryRun, dataSources, resultList, lastErrorDelegateRun
        )
        refreshAlertsIndex(dataSources, monitorCtx)

        return workflowRunResult
    }

    private suspend fun getMonitors(monitorCtx: MonitorRunnerExecutionContext, delegates: List<Delegate>): List<Monitor> {
        try {
            return monitorCtx.workflowService!!.getMonitorsById(delegates.map { it.monitorId }, delegates.size)
        } catch (e: Exception) {
            logger.error("Failed getting workflow delegates. Error: ${e.message}", e)
            throw e
        }
    }

    private suspend fun runDelegateStreamingMonitor(
        delegateMonitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        dryRun: Boolean,
        workflowRunContext: WorkflowRunContext,
        executionId: String,
        streamingIndices: List<StreamingIndex>
    ): MonitorRunResult<*> {
        if (delegateMonitor.isBucketLevelMonitor()) {
            return BucketLevelMonitorRunner.runStreamingMonitor(
                delegateMonitor,
                monitorCtx,
                dryRun,
                workflowRunContext,
                executionId,
                streamingIndices
            )
        } else if (delegateMonitor.isDocLevelMonitor()) {
            return DocumentLevelMonitorRunner.runStreamingMonitor(
                delegateMonitor,
                monitorCtx,
                dryRun,
                workflowRunContext,
                executionId,
                streamingIndices
            )
        } else if (delegateMonitor.isQueryLevelMonitor()) {
            return QueryLevelMonitorRunner.runStreamingMonitor(
                delegateMonitor,
                monitorCtx,
                dryRun,
                workflowRunContext,
                executionId,
                streamingIndices
            )
        } else {
            throw AlertingException.wrap(
                IllegalStateException("Unsupported streaming monitor type ${delegateMonitor.monitorType}")
            )
        }
    }
}
