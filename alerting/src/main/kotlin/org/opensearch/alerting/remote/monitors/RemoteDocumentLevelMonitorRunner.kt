/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.remote.monitors

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.Version
import org.opensearch.alerting.MonitorMetadataService
import org.opensearch.alerting.MonitorRunner
import org.opensearch.alerting.MonitorRunnerExecutionContext
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.routing.ShardRouting
import org.opensearch.cluster.service.ClusterService
import org.opensearch.commons.alerting.action.DocLevelMonitorFanOutResponse
import org.opensearch.commons.alerting.model.ActionRunResult
import org.opensearch.commons.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.commons.alerting.model.InputRunResults
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorRunResult
import org.opensearch.commons.alerting.model.WorkflowRunContext
import org.opensearch.commons.alerting.model.remote.monitors.RemoteDocLevelMonitorInput
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.transport.TransportService
import java.io.IOException
import java.time.Instant

class RemoteDocumentLevelMonitorRunner : MonitorRunner() {
    private val logger = LogManager.getLogger(javaClass)

    override suspend fun runMonitor(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryRun: Boolean,
        workflowRunContext: WorkflowRunContext?,
        executionId: String,
        transportService: TransportService
    ): MonitorRunResult<*> {
        logger.debug("Remote Document-level-monitor is running ...")
        val isTempMonitor = dryRun || monitor.id == Monitor.NO_ID
        var monitorResult = MonitorRunResult<DocumentLevelTriggerRunResult>(monitor.name, periodStart, periodEnd)

        try {
            validate(monitor)
        } catch (e: Exception) {
            logger.error("Failed to start Document-level-monitor. Error: $e")
            monitorResult = monitorResult.copy(error = AlertingException.wrap(e))
        }

        var (monitorMetadata, _) = MonitorMetadataService.getOrCreateMetadata(
            monitor = monitor,
            createWithRunContext = false,
            skipIndex = isTempMonitor,
            workflowRunContext?.workflowMetadataId
        )
        logger.info(monitorMetadata.lastRunContext.toMutableMap().toString())
        val lastRunContext = if (monitorMetadata.lastRunContext.isNullOrEmpty()) mutableMapOf()
        else monitorMetadata.lastRunContext.toMutableMap() as MutableMap<String, MutableMap<String, Any>>
        val updatedLastRunContext = lastRunContext.toMutableMap()

        val remoteDocLevelMonitorInput = monitor.inputs[0] as RemoteDocLevelMonitorInput
        val docLevelMonitorInput = remoteDocLevelMonitorInput.docLevelMonitorInput
        var shards: Set<String> = mutableSetOf()
        var concreteIndices = listOf<String>()

        // Resolve all passed indices to concrete indices
        val allConcreteIndices = IndexUtils.resolveAllIndices(
            docLevelMonitorInput.indices,
            monitorCtx.clusterService!!,
            monitorCtx.indexNameExpressionResolver!!
        )
        // cleanup old indices that are not monitored anymore from the same monitor
        val runContextKeys = updatedLastRunContext.keys.toMutableSet()
        for (ind in runContextKeys) {
            if (!allConcreteIndices.contains(ind)) {
                updatedLastRunContext.remove(ind)
                lastRunContext.remove(ind)
            }
        }

        try {
            docLevelMonitorInput.indices.forEach { indexName ->
                concreteIndices = IndexUtils.resolveAllIndices(
                    listOf(indexName),
                    monitorCtx.clusterService!!,
                    monitorCtx.indexNameExpressionResolver!!
                )
                var lastWriteIndex: String? = null
                if (IndexUtils.isAlias(indexName, monitorCtx.clusterService!!.state()) ||
                    IndexUtils.isDataStream(indexName, monitorCtx.clusterService!!.state())
                ) {
                    lastWriteIndex = concreteIndices.find { lastRunContext.containsKey(it) }
                    if (lastWriteIndex != null) {
                        val lastWriteIndexCreationDate =
                            IndexUtils.getCreationDateForIndex(lastWriteIndex, monitorCtx.clusterService!!.state())
                        concreteIndices = IndexUtils.getNewestIndicesByCreationDate(
                            concreteIndices,
                            monitorCtx.clusterService!!.state(),
                            lastWriteIndexCreationDate
                        )
                    }
                }

                concreteIndices.forEach { concreteIndexName ->
                    // Prepare lastRunContext for each index
                    val indexLastRunContext = lastRunContext.getOrPut(concreteIndexName) {
                        val isIndexCreatedRecently = createdRecently(
                            monitor,
                            periodStart,
                            periodEnd,
                            monitorCtx.clusterService!!.state().metadata.index(concreteIndexName)
                        )
                        MonitorMetadataService.createRunContextForIndex(concreteIndexName, isIndexCreatedRecently)
                    }

                    val indexUpdatedRunContext = initializeNewLastRunContext(
                        indexLastRunContext.toMutableMap(),
                        monitorCtx,
                        concreteIndexName
                    ) as MutableMap<String, Any>
                    if (IndexUtils.isAlias(indexName, monitorCtx.clusterService!!.state()) ||
                        IndexUtils.isDataStream(indexName, monitorCtx.clusterService!!.state())
                    ) {
                        if (concreteIndexName == IndexUtils.getWriteIndex(
                                indexName,
                                monitorCtx.clusterService!!.state()
                            )
                        ) {
                            updatedLastRunContext.remove(lastWriteIndex)
                            updatedLastRunContext[concreteIndexName] = indexUpdatedRunContext
                        }
                    } else {
                        updatedLastRunContext[concreteIndexName] = indexUpdatedRunContext
                    }
                }

                concreteIndices.forEach {
                    val shardCount = getShardsCount(monitorCtx.clusterService!!, it)
                    for (i in 0 until shardCount) {
                        shards = shards.plus("$it:$i")
                    }
                }
            }

            val nodeMap = getNodes(monitorCtx)
            val nodeShardAssignments = distributeShards(
                monitorCtx,
                nodeMap.keys.toList(),
                shards.toList()
            )

            val docLevelMonitorFanOutResponses = monitorCtx.remoteMonitors[monitor.monitorType]!!.monitorRunner.doFanOut(
                monitorCtx.clusterService!!,
                monitor,
                monitorMetadata.copy(lastRunContext = lastRunContext),
                executionId,
                concreteIndices,
                workflowRunContext,
                dryRun,
                transportService,
                nodeMap,
                nodeShardAssignments
            )
            updateLastRunContextFromFanOutResponses(docLevelMonitorFanOutResponses, updatedLastRunContext)
            val triggerResults = buildTriggerResults(docLevelMonitorFanOutResponses)
            val inputRunResults = buildInputRunResults(docLevelMonitorFanOutResponses)
            if (!isTempMonitor) {
                MonitorMetadataService.upsertMetadata(
                    monitorMetadata.copy(lastRunContext = updatedLastRunContext),
                    true
                )
            }
            return monitorResult.copy(triggerResults = triggerResults, inputResults = inputRunResults)
        } catch (e: Exception) {
            logger.error("Failed running Document-level-monitor ${monitor.name}", e)
            val errorMessage = ExceptionsHelper.detailedMessage(e)
            monitorCtx.alertService!!.upsertMonitorErrorAlert(monitor, errorMessage, executionId, workflowRunContext)
            val alertingException = AlertingException(
                errorMessage,
                RestStatus.INTERNAL_SERVER_ERROR,
                e
            )
            return monitorResult.copy(error = alertingException, inputResults = InputRunResults(emptyList(), alertingException))
        }
    }

    private fun validate(monitor: Monitor) {
        if (monitor.inputs.size > 1) {
            throw IOException("Only one input is supported with remote document-level-monitor.")
        }

        if (monitor.inputs[0].name() != RemoteDocLevelMonitorInput.REMOTE_DOC_LEVEL_MONITOR_INPUT_FIELD) {
            throw IOException("Invalid input with remote document-level-monitor.")
        }

        if ((monitor.inputs[0] as RemoteDocLevelMonitorInput).docLevelMonitorInput.indices.isEmpty()) {
            throw IllegalArgumentException("DocLevelMonitorInput has no indices")
        }
    }

    private fun getNodes(monitorCtx: MonitorRunnerExecutionContext): Map<String, DiscoveryNode> {
        return monitorCtx.clusterService!!.state().nodes.dataNodes.filter { it.value.version >= Version.CURRENT }
    }

    private fun distributeShards(
        monitorCtx: MonitorRunnerExecutionContext,
        allNodes: List<String>,
        shards: List<String>,
    ): Map<String, MutableSet<ShardId>> {
        val totalShards = shards.size
        val numFanOutNodes = allNodes.size.coerceAtMost((totalShards + 1) / 2)
        val totalNodes = monitorCtx.totalNodesFanOut.coerceAtMost(numFanOutNodes)
        val shardsPerNode = totalShards / totalNodes
        var shardsRemaining = totalShards % totalNodes

        val shardIdList = shards.map {
            val index = it.split(":")[0]
            val shardId = it.split(":")[1]
            ShardId(monitorCtx.clusterService!!.state().metadata.index(index).index, shardId.toInt())
        }
        val nodes = allNodes.subList(0, totalNodes)

        val nodeShardAssignments = mutableMapOf<String, MutableSet<ShardId>>()
        var idx = 0
        for (node in nodes) {
            val nodeShardAssignment = mutableSetOf<ShardId>()
            for (i in 1..shardsPerNode) {
                nodeShardAssignment.add(shardIdList[idx++])
            }
            nodeShardAssignments[node] = nodeShardAssignment
        }

        for (node in nodes) {
            if (shardsRemaining == 0) {
                break
            }
            nodeShardAssignments[node]!!.add(shardIdList[idx++])
            --shardsRemaining
        }
        return nodeShardAssignments
    }

    private fun getShardsCount(clusterService: ClusterService, index: String): Int {
        val allShards: List<ShardRouting> = clusterService!!.state().routingTable().allShards(index)
        return allShards.filter { it.primary() }.size
    }

    private fun updateLastRunContextFromFanOutResponses(
        docLevelMonitorFanOutResponses: MutableList<DocLevelMonitorFanOutResponse>,
        updatedLastRunContext: MutableMap<String, MutableMap<String, Any>>,
    ) {

        // Prepare updatedLastRunContext for each index
        for (indexName in updatedLastRunContext.keys) {
            for (fanOutResponse in docLevelMonitorFanOutResponses) {
                if (fanOutResponse.exception == null) {
                    // fanOutResponse.lastRunContexts //updatedContexts for relevant shards
                    val indexLastRunContext = updatedLastRunContext[indexName] as MutableMap<String, Any>

                    if (fanOutResponse.lastRunContexts.contains(indexName)) {
                        (fanOutResponse.lastRunContexts[indexName] as Map<String, Any>).forEach {

                            val seq_no = it.value.toString().toIntOrNull()
                            if (
                                it.key != "shards_count" &&
                                it.key != "index" &&
                                seq_no != null &&
                                seq_no >= 0
                            ) {
                                indexLastRunContext[it.key] = seq_no
                            }
                        }
                    }
                }
            }
        }
    }

    private fun buildTriggerResults(
        docLevelMonitorFanOutResponses: MutableList<DocLevelMonitorFanOutResponse>,
    ): MutableMap<String, DocumentLevelTriggerRunResult> {
        val triggerResults = mutableMapOf<String, DocumentLevelTriggerRunResult>()
        val triggerErrorMap = mutableMapOf<String, MutableList<AlertingException>>()
        for (res in docLevelMonitorFanOutResponses) {
            if (res.exception == null) {
                for (triggerId in res.triggerResults.keys) {
                    val documentLevelTriggerRunResult = res.triggerResults[triggerId]
                    if (documentLevelTriggerRunResult != null) {
                        if (false == triggerResults.contains(triggerId)) {
                            triggerResults[triggerId] = documentLevelTriggerRunResult
                            triggerErrorMap[triggerId] = if (documentLevelTriggerRunResult.error != null) {
                                val error = if (documentLevelTriggerRunResult.error is AlertingException) {
                                    documentLevelTriggerRunResult.error as AlertingException
                                } else {
                                    AlertingException.wrap(documentLevelTriggerRunResult.error!!) as AlertingException
                                }
                                mutableListOf(error)
                            } else {
                                mutableListOf()
                            }
                        } else {
                            val currVal = triggerResults[triggerId]
                            val newTriggeredDocs = mutableListOf<String>()
                            newTriggeredDocs.addAll(currVal!!.triggeredDocs)
                            newTriggeredDocs.addAll(documentLevelTriggerRunResult.triggeredDocs)
                            val newActionResults = mutableMapOf<String, MutableMap<String, ActionRunResult>>()
                            newActionResults.putAll(currVal.actionResultsMap)
                            newActionResults.putAll(documentLevelTriggerRunResult.actionResultsMap)
                            triggerResults[triggerId] = currVal.copy(
                                triggeredDocs = newTriggeredDocs,
                                actionResultsMap = newActionResults
                            )

                            if (documentLevelTriggerRunResult.error != null) {
                                triggerErrorMap[triggerId]!!.add(documentLevelTriggerRunResult.error as AlertingException)
                            }
                        }
                    }
                }
            }
        }

        triggerErrorMap.forEach { triggerId, errorList ->
            if (errorList.isNotEmpty()) {
                triggerResults[triggerId]!!.error = AlertingException.merge(*errorList.toTypedArray())
            }
        }
        return triggerResults
    }

    private fun buildInputRunResults(docLevelMonitorFanOutResponses: MutableList<DocLevelMonitorFanOutResponse>): InputRunResults {
        val inputRunResults = mutableMapOf<String, MutableSet<String>>()
        val errors: MutableList<AlertingException> = mutableListOf()
        for (response in docLevelMonitorFanOutResponses) {
            if (response.exception == null) {
                if (response.inputResults.error != null) {
                    if (response.inputResults.error is AlertingException) {
                        errors.add(response.inputResults.error as AlertingException)
                    } else {
                        errors.add(AlertingException.wrap(response.inputResults.error as Exception) as AlertingException)
                    }
                }
                val partialResult = response.inputResults.results
                for (result in partialResult) {
                    for (id in result.keys) {
                        inputRunResults.getOrPut(id) { mutableSetOf() }.addAll(result[id] as Collection<String>)
                    }
                }
            }
        }
        return InputRunResults(listOf(inputRunResults), if (!errors.isEmpty()) AlertingException.merge(*errors.toTypedArray()) else null)
    }

    private fun createdRecently(
        monitor: Monitor,
        periodStart: Instant,
        periodEnd: Instant,
        indexMetadata: IndexMetadata
    ): Boolean {
        val lastExecutionTime = if (periodStart == periodEnd) monitor.lastUpdateTime else periodStart
        val indexCreationDate = indexMetadata.settings.get("index.creation_date")?.toLong() ?: 0L
        return indexCreationDate > lastExecutionTime.toEpochMilli()
    }

    private fun initializeNewLastRunContext(
        lastRunContext: Map<String, Any>,
        monitorCtx: MonitorRunnerExecutionContext,
        index: String,
    ): Map<String, Any> {
        val count: Int = getShardsCount(monitorCtx.clusterService!!, index)
        val updatedLastRunContext = lastRunContext.toMutableMap()
        for (i: Int in 0 until count) {
            val shard = i.toString()
            updatedLastRunContext[shard] = SequenceNumbers.UNASSIGNED_SEQ_NO
        }
        return updatedLastRunContext
    }
}
