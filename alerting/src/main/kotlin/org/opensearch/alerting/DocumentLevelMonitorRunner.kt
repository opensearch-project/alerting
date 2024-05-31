/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.Version
import org.opensearch.action.ActionListenerResponseHandler
import org.opensearch.action.support.GroupedActionListener
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.routing.ShardRouting
import org.opensearch.cluster.service.ClusterService
import org.opensearch.commons.alerting.action.DocLevelMonitorFanOutAction
import org.opensearch.commons.alerting.action.DocLevelMonitorFanOutRequest
import org.opensearch.commons.alerting.action.DocLevelMonitorFanOutResponse
import org.opensearch.commons.alerting.model.ActionRunResult
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.commons.alerting.model.IndexExecutionContext
import org.opensearch.commons.alerting.model.InputRunResults
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorRunResult
import org.opensearch.commons.alerting.model.WorkflowRunContext
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.breaker.CircuitBreakingException
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.node.NodeClosedException
import org.opensearch.transport.ActionNotFoundTransportException
import org.opensearch.transport.ConnectTransportException
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.TransportException
import org.opensearch.transport.TransportRequestOptions
import org.opensearch.transport.TransportService
import java.io.IOException
import java.time.Instant
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.math.max

class DocumentLevelMonitorRunner : MonitorRunner() {
    private val logger = LogManager.getLogger(javaClass)
    private var totalTimeTakenStat = 0L

    override suspend fun runMonitor(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryrun: Boolean,
        workflowRunContext: WorkflowRunContext?,
        executionId: String,
        transportService: TransportService
    ): MonitorRunResult<DocumentLevelTriggerRunResult> {
        logger.debug("Document-level-monitor is running ...")
        val startTime = System.currentTimeMillis()
        val isTempMonitor = dryrun || monitor.id == Monitor.NO_ID
        var monitorResult = MonitorRunResult<DocumentLevelTriggerRunResult>(monitor.name, periodStart, periodEnd)
        monitorCtx.findingsToTriggeredQueries = mutableMapOf()

        try {
            monitorCtx.alertIndices!!.createOrUpdateAlertIndex(monitor.dataSources)
            monitorCtx.alertIndices!!.createOrUpdateInitialAlertHistoryIndex(monitor.dataSources)
            monitorCtx.alertIndices!!.createOrUpdateInitialFindingHistoryIndex(monitor.dataSources)
        } catch (e: Exception) {
            val id = if (monitor.id.trim().isEmpty()) "_na_" else monitor.id
            val unwrappedException = ExceptionsHelper.unwrapCause(e)
            if (unwrappedException is IllegalArgumentException && unwrappedException.message?.contains("Limit of total fields") == true) {
                val errorMessage =
                    "Monitor [$id] can't process index [$monitor.dataSources] due to field mapping limit"
                logger.error("Exception: ${unwrappedException.message}")
                monitorResult = monitorResult.copy(error = AlertingException(errorMessage, RestStatus.INTERNAL_SERVER_ERROR, e))
            } else {
                logger.error("Error setting up alerts and findings indices for monitor: $id", e)
                monitorResult = monitorResult.copy(error = AlertingException.wrap(e))
            }
        }
        try {
            validate(monitor)
        } catch (e: Exception) {
            logger.error("Failed to start Document-level-monitor. Error: ${e.message}")
            monitorResult = monitorResult.copy(error = AlertingException.wrap(e))
        }

        var (monitorMetadata, _) = MonitorMetadataService.getOrCreateMetadata(
            monitor = monitor,
            createWithRunContext = false,
            skipIndex = isTempMonitor,
            workflowRunContext?.workflowMetadataId
        )

        val docLevelMonitorInput = monitor.inputs[0] as DocLevelMonitorInput

        val queries: List<DocLevelQuery> = docLevelMonitorInput.queries

        val lastRunContext = if (monitorMetadata.lastRunContext.isNullOrEmpty()) mutableMapOf()
        else monitorMetadata.lastRunContext.toMutableMap() as MutableMap<String, MutableMap<String, Any>>

        val updatedLastRunContext = lastRunContext.toMutableMap()

        try {
            // Resolve all passed indices to concrete indices
            val allConcreteIndices = IndexUtils.resolveAllIndices(
                docLevelMonitorInput.indices,
                monitorCtx.clusterService!!,
                monitorCtx.indexNameExpressionResolver!!
            )
            if (allConcreteIndices.isEmpty()) {
                logger.error("indices not found-${docLevelMonitorInput.indices.joinToString(",")}")
                throw IndexNotFoundException(docLevelMonitorInput.indices.joinToString(","))
            }

            monitorCtx.docLevelMonitorQueries!!.initDocLevelQueryIndex(monitor.dataSources)
            monitorCtx.docLevelMonitorQueries!!.indexDocLevelQueries(
                monitor = monitor,
                monitorId = monitor.id,
                monitorMetadata,
                indexTimeout = monitorCtx.indexTimeout!!
            )

            // cleanup old indices that are not monitored anymore from the same monitor
            val runContextKeys = updatedLastRunContext.keys.toMutableSet()
            for (ind in runContextKeys) {
                if (!allConcreteIndices.contains(ind)) {
                    updatedLastRunContext.remove(ind)
                }
            }

            // Map of document ids per index when monitor is workflow delegate and has chained findings
            val matchingDocIdsPerIndex = workflowRunContext?.matchingDocIdsPerIndex

            val concreteIndicesSeenSoFar = mutableListOf<String>()
            val updatedIndexNames = mutableListOf<String>()
            val docLevelMonitorFanOutResponses: MutableList<DocLevelMonitorFanOutResponse> = mutableListOf()
            docLevelMonitorInput.indices.forEach { indexName ->
                var concreteIndices = IndexUtils.resolveAllIndices(
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
                concreteIndicesSeenSoFar.addAll(concreteIndices)
                val updatedIndexName = indexName.replace("*", "_")
                updatedIndexNames.add(updatedIndexName)
                val conflictingFields = monitorCtx.docLevelMonitorQueries!!.getAllConflictingFields(
                    monitorCtx.clusterService!!.state(),
                    concreteIndices
                )

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

                    // Prepare updatedLastRunContext for each index
                    val indexUpdatedRunContext = initializeNewLastRunContext(
                        indexLastRunContext.toMutableMap(),
                        monitorCtx,
                        concreteIndexName,
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

                    val count: Int = indexLastRunContext["shards_count"] as Int
                    for (i: Int in 0 until count) {
                        val shard = i.toString()

                        // update lastRunContext if its a temp monitor as we only want to view the last bit of data then
                        // TODO: If dryrun, we should make it so we limit the search as this could still potentially give us lots of data
                        if (isTempMonitor) {
                            indexLastRunContext[shard] =
                                max(-1, (indexUpdatedRunContext[shard] as String).toInt() - 10)
                        }
                    }
                    val indexExecutionContext = IndexExecutionContext(
                        queries,
                        indexLastRunContext,
                        indexUpdatedRunContext,
                        updatedIndexName,
                        concreteIndexName,
                        updatedIndexNames,
                        concreteIndices,
                        conflictingFields.toList(),
                        matchingDocIdsPerIndex?.get(concreteIndexName),
                    )

                    val shards = mutableSetOf<String>()
                    shards.addAll(indexUpdatedRunContext.keys)
                    shards.remove("index")
                    shards.remove("shards_count")

                    val nodeMap = getNodes(monitorCtx)
                    val nodeShardAssignments = distributeShards(
                        monitorCtx,
                        nodeMap.keys.toList(),
                        shards.toList(),
                        concreteIndexName
                    )

                    val responses: Collection<DocLevelMonitorFanOutResponse> = suspendCoroutine { cont ->
                        val listener = GroupedActionListener(
                            object : ActionListener<Collection<DocLevelMonitorFanOutResponse>> {
                                override fun onResponse(response: Collection<DocLevelMonitorFanOutResponse>) {
                                    cont.resume(response)
                                }

                                override fun onFailure(e: Exception) {
                                    if (e.cause is Exception)
                                        cont.resumeWithException(e.cause as Exception)
                                    else
                                        cont.resumeWithException(e)
                                }
                            },
                            nodeShardAssignments.size
                        )
                        val responseReader = Writeable.Reader {
                            DocLevelMonitorFanOutResponse(it)
                        }
                        for (node in nodeMap) {
                            if (nodeShardAssignments.containsKey(node.key)) {
                                val docLevelMonitorFanOutRequest = DocLevelMonitorFanOutRequest(
                                    monitor,
                                    dryrun,
                                    monitorMetadata,
                                    executionId,
                                    indexExecutionContext,
                                    nodeShardAssignments[node.key]!!.toList(),
                                    concreteIndicesSeenSoFar,
                                    workflowRunContext
                                )

                                transportService.sendRequest(
                                    node.value,
                                    DocLevelMonitorFanOutAction.NAME,
                                    docLevelMonitorFanOutRequest,
                                    TransportRequestOptions.EMPTY,
                                    object : ActionListenerResponseHandler<DocLevelMonitorFanOutResponse>(
                                        listener,
                                        responseReader
                                    ) {
                                        override fun handleException(e: TransportException) {
                                            val cause = e.unwrapCause()
                                            if (cause is ConnectTransportException ||
                                                (
                                                    e is RemoteTransportException &&
                                                        (
                                                            cause is NodeClosedException ||
                                                                cause is CircuitBreakingException ||
                                                                cause is ActionNotFoundTransportException
                                                            )
                                                    )
                                            ) {
                                                val localNode = monitorCtx.clusterService!!.localNode()
                                                // retry in local node
                                                transportService.sendRequest(
                                                    localNode,
                                                    DocLevelMonitorFanOutAction.NAME,
                                                    docLevelMonitorFanOutRequest,
                                                    TransportRequestOptions.EMPTY,
                                                    object :
                                                        ActionListenerResponseHandler<DocLevelMonitorFanOutResponse>(
                                                            listener,
                                                            responseReader
                                                        ) {
                                                        override fun handleException(e: TransportException) {
                                                            logger.error("Fan out retry failed in node ${localNode.id}", e)
                                                            listener.onResponse(
                                                                DocLevelMonitorFanOutResponse(
                                                                    "",
                                                                    "",
                                                                    "",
                                                                    mutableMapOf(),
                                                                    exception = if (e.cause is AlertingException) {
                                                                        e.cause as AlertingException
                                                                    } else {
                                                                        AlertingException.wrap(e) as AlertingException
                                                                    }
                                                                )
                                                            )
                                                        }

                                                        override fun handleResponse(response: DocLevelMonitorFanOutResponse) {
                                                            listener.onResponse(response)
                                                        }
                                                    }
                                                )
                                            } else {
                                                logger.error("Fan out failed in node ${node.key}", e)
                                                listener.onResponse(
                                                    DocLevelMonitorFanOutResponse(
                                                        "",
                                                        "",
                                                        "",
                                                        mutableMapOf(),
                                                        exception = if (e.cause is AlertingException) {
                                                            e.cause as AlertingException
                                                        } else {
                                                            AlertingException.wrap(e) as AlertingException
                                                        }
                                                    )
                                                )
                                            }
                                        }

                                        override fun handleResponse(response: DocLevelMonitorFanOutResponse) {
                                            listener.onResponse(response)
                                        }
                                    }
                                )
                            }
                        }
                    }
                    docLevelMonitorFanOutResponses.addAll(responses)
                }
            }

            val isFanOutSuccessful = checkAndThrowExceptionIfAllFanOutsFailed(docLevelMonitorFanOutResponses)
            if (isFanOutSuccessful != null) {
                throw isFanOutSuccessful
            }
            updateLastRunContextFromFanOutResponses(docLevelMonitorFanOutResponses, updatedLastRunContext)
            val triggerResults = buildTriggerResults(docLevelMonitorFanOutResponses)
            val inputRunResults = buildInputRunResults(docLevelMonitorFanOutResponses)
            if (!isTempMonitor) {
                MonitorMetadataService.upsertMetadata(
                    monitorMetadata.copy(lastRunContext = updatedLastRunContext),
                    true
                )
            } else {
                // Clean up any queries created by the dry run monitor
                monitorCtx.docLevelMonitorQueries!!.deleteDocLevelQueriesOnDryRun(monitorMetadata)
            }
            // TODO: Update the Document as part of the Trigger and return back the trigger action result
            return monitorResult.copy(triggerResults = triggerResults, inputResults = inputRunResults)
        } catch (e: Exception) {
            val errorMessage = ExceptionsHelper.detailedMessage(e)
            monitorCtx.alertService!!.upsertMonitorErrorAlert(monitor, errorMessage, executionId, workflowRunContext)
            logger.error("Failed running Document-level-monitor ${monitor.name}", e)
            val alertingException = AlertingException(
                errorMessage,
                RestStatus.INTERNAL_SERVER_ERROR,
                e
            )
            return monitorResult.copy(error = alertingException, inputResults = InputRunResults(emptyList(), alertingException))
        } finally {
            val endTime = System.currentTimeMillis()
            totalTimeTakenStat = endTime - startTime
            logger.debug(
                "Monitor {} Time spent on monitor run: {}",
                monitor.id,
                totalTimeTakenStat
            )
        }
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

                    if (fanOutResponse.lastRunContexts.contains("index") && fanOutResponse.lastRunContexts["index"] == indexName) {
                        fanOutResponse.lastRunContexts.keys.forEach {

                            val seq_no = fanOutResponse.lastRunContexts[it].toString().toIntOrNull()
                            if (
                                it != "shards_count" &&
                                it != "index" &&
                                seq_no != null &&
                                seq_no >= 0
                            ) {
                                indexLastRunContext[it] = seq_no
                            }
                        }
                    }
                }
            }
        }
    }

    private fun checkAndThrowExceptionIfAllFanOutsFailed(
        docLevelMonitorFanOutResponses: MutableList<DocLevelMonitorFanOutResponse>
    ): AlertingException? {
        val exceptions = mutableListOf<AlertingException>()
        for (res in docLevelMonitorFanOutResponses) {
            if (res.exception == null) {
                return null
            } else {
                exceptions.add(res.exception!!)
            }
        }
        return AlertingException.merge(*exceptions.toTypedArray())
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

    private fun initializeNewLastRunContext(
        lastRunContext: Map<String, Any>,
        monitorCtx: MonitorRunnerExecutionContext,
        index: String,
    ): Map<String, Any> {
        val count: Int = getShardsCount(monitorCtx.clusterService!!, index)
        val updatedLastRunContext = lastRunContext.toMutableMap()
        for (i: Int in 0 until count) {
            val shard = i.toString()
            updatedLastRunContext[shard] = SequenceNumbers.UNASSIGNED_SEQ_NO.toString()
        }
        return updatedLastRunContext
    }

    private fun validate(monitor: Monitor) {
        if (monitor.inputs.size > 1) {
            throw IOException("Only one input is supported with document-level-monitor.")
        }

        if (monitor.inputs[0].name() != DocLevelMonitorInput.DOC_LEVEL_INPUT_FIELD) {
            throw IOException("Invalid input with document-level-monitor.")
        }

        if ((monitor.inputs[0] as DocLevelMonitorInput).indices.isEmpty()) {
            throw IllegalArgumentException("DocLevelMonitorInput has no indices")
        }
    }

    // Checks if the index was created from the last execution run or when the monitor was last updated to ensure that
    // new index is monitored from the beginning of that index
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

    private fun getShardsCount(clusterService: ClusterService, index: String): Int {
        val allShards: List<ShardRouting> = clusterService!!.state().routingTable().allShards(index)
        return allShards.filter { it.primary() }.size
    }

    private fun getNodes(monitorCtx: MonitorRunnerExecutionContext): Map<String, DiscoveryNode> {
        return monitorCtx.clusterService!!.state().nodes.dataNodes.filter { it.value.version >= Version.CURRENT }
    }

    private fun distributeShards(
        monitorCtx: MonitorRunnerExecutionContext,
        allNodes: List<String>,
        shards: List<String>,
        index: String,
    ): Map<String, MutableSet<ShardId>> {
        val totalShards = shards.size
        val numFanOutNodes = allNodes.size.coerceAtMost((totalShards + 1) / 2)
        val totalNodes = monitorCtx.totalNodesFanOut.coerceAtMost(numFanOutNodes)
        val shardsPerNode = totalShards / totalNodes
        var shardsRemaining = totalShards % totalNodes

        val shardIdList = shards.map {
            ShardId(monitorCtx.clusterService!!.state().metadata.index(index).index, it.toInt())
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
}
