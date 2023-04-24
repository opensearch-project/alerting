/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.workflow

import org.apache.logging.log4j.LogManager
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.BucketLevelMonitorRunner
import org.opensearch.alerting.DocumentLevelMonitorRunner
import org.opensearch.alerting.MonitorRunnerExecutionContext
import org.opensearch.alerting.QueryLevelMonitorRunner
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.chainedAlertCondition.parsers.CAExpressionParser
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.WorkflowRunResult
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.isDocLevelMonitor
import org.opensearch.alerting.util.isQueryLevelMonitor
import org.opensearch.client.Client
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.CompositeInput
import org.opensearch.commons.alerting.model.Delegate
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.util.isBucketLevelMonitor
import org.opensearch.index.query.QueryBuilders
import java.time.Instant
import java.time.LocalDateTime
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
        val workflowExecutionId = UUID.randomUUID().toString() + LocalDateTime.now()
        var workflowResult = WorkflowRunResult(mutableListOf(), periodStart, periodEnd, workflowExecutionId)
        logger.debug("Workflow ${workflow.id} in $workflowExecutionId execution is running")
        try {
            val delegates = (workflow.inputs[0] as CompositeInput).sequence.delegates.sortedBy { it.order }
            var monitors = monitorCtx.workflowService!!.getMonitorsById(delegates.map { it.monitorId }, delegates.size)
            // Validate the monitors size
            validateMonitorSize(delegates, monitors, workflow)

            val monitorsById = monitors.associateBy { it.id }
            val resultList = mutableListOf<MonitorRunResult<*>>()

            for (delegate in delegates) {
                var indexToDocIds = mapOf<String, List<String>>()
                var delegateMonitor: Monitor
                delegateMonitor = monitorsById[delegate.monitorId]
                    ?: throw AlertingException.wrap(
                        IllegalStateException("Delegate monitor not found ${delegate.monitorId} for the workflow $workflow.id")
                    )
                if (delegate.chainedFindings != null) {
                    val chainedMonitor = monitorsById[delegate.chainedFindings!!.monitorId]
                        ?: throw AlertingException.wrap(
                            IllegalStateException("Chained finding monitor not found ${delegate.monitorId} for the workflow $workflow.id")
                        )
                    indexToDocIds = monitorCtx.workflowService!!.getFindingDocIdsByExecutionId(chainedMonitor, workflowExecutionId)
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
                resultList.add(runResult)
            }
            logger.debug("Workflow ${workflow.id} in $workflowExecutionId finished")

            val executeChainedAlerts = if(workflow.chainedAlerts.isNotEmpty())
                executeChainedAlerts(monitorCtx, workflow, workflowExecutionId, delegates)
            else null
            return workflowResult.copy(workflowRunResult = resultList, chainedAlert = executeChainedAlerts)
        } catch (e: Exception) {
            logger.error("Failed to execute workflow. Error: ${e.message}")
            return workflowResult.copy(error = AlertingException.wrap(e))
        }
    }

    private suspend fun executeChainedAlerts(
        monitorCtx: MonitorRunnerExecutionContext,
        workflow: Workflow,
        workflowExecutionId: String,
        delegates: List<Delegate>
    ): Alert? {
        val searchRequest = SearchRequest(AlertIndices.ALERT_INDEX)
        val queryBuilder = QueryBuilders.boolQuery()

        queryBuilder.filter(QueryBuilders.termQuery("workflow_execution_id", workflowExecutionId))

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
        val map = mutableMapOf<String, Boolean>()
        for (alert in alerts) {
            map.put(alert.monitorId!!, true);
        }
        workflow.chainedAlerts.get(0).condition
        if(CAExpressionParser(workflow.chainedAlerts.get(0).condition).parse()
                .evaluate(map)) {
            try {
                val alert = Alert(
                    Instant.now(),
                    Instant.now(),
                    Alert.State.ACTIVE,
                    null, -1,
                    "dunno", workflowExecutionId,
                    "test chained alert")

                val indexRequest = IndexRequest(AlertIndices.ALERT_INDEX)
                    .routing(workflow.id)
                    .id(UUID.randomUUID().toString())
                    .source(alert.toXContentWithUser(XContentFactory.jsonBuilder()))
                val indexResponse = monitorCtx.client!!.suspendUntil<Client, IndexResponse> {
                    monitorCtx.client!!.index(indexRequest, it)
                }
                return alert.copy(id=indexResponse.id)
            } catch (e: Exception) {
                println(e)
                return null
            }


        } else {
            println("ChainedAlertConditionNotSatisfied")
            return null;
        }
    }

    private fun validateMonitorSize(
        delegates: List<Delegate>,
        monitors: List<Monitor>,
        workflow: Workflow,
    ) {
        if (delegates.size != monitors.size) {
            val diffMonitorIds = delegates.map { it.monitorId }.minus(monitors.map { it.id }.toSet()).joinToString()
            throw IllegalStateException("Delegate monitors don't exist $diffMonitorIds for the workflow $workflow.id")
        }
    }
}
