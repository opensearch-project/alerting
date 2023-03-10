/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.alerting.MonitorMetadataService
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.model.MonitorMetadata
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.IndexUtils.Companion.isIndexWriteIndex
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.index.IndexNotFoundException
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool

class QueryIndexManagement(
    settings: Settings,
    private val client: Client,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService
) : ClusterStateListener,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("QueryIndexManagement")) {

    @Volatile private var runPeriod = AlertingSettings.QUERY_INDEX_CLEANUP_PERIOD.get(settings)

    init {
        clusterService.addListener(this)
        clusterService.clusterSettings
            .addSettingsUpdateConsumer(AlertingSettings.QUERY_INDEX_CLEANUP_PERIOD) {
                runPeriod = it
                sweepJob?.cancel()
                sweepJob = threadPool
                    .scheduleWithFixedDelay({ runQueryIndexSweep() }, runPeriod, executorName())
            }
    }

    @Volatile private var isClusterManager: Boolean = false
    @Volatile private var sweepJob: Scheduler.Cancellable? = null

    private var runLock: Mutex = Mutex(false)

    fun onMaster() {
        try {
            runQueryIndexSweep()
        } catch (e: Exception) {
            logger.error(
                "Error cleaning up query indices",
                e
            )
        } finally {
            sweepJob = threadPool
                .scheduleWithFixedDelay({ runQueryIndexSweep() }, runPeriod, executorName())
        }
    }

    fun offMaster() {
        sweepJob?.cancel()
    }

    private fun executorName(): String {
        return ThreadPool.Names.MANAGEMENT
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        // Instead of using a LocalNodeClusterManagerListener to track master changes, this service will
        // track them here to avoid conditions where master listener events run after other
        // listeners that depend on what happened in the master listener
        if (this.isClusterManager != event.localNodeClusterManager()) {
            this.isClusterManager = event.localNodeClusterManager()
            if (this.isClusterManager) {
                onMaster()
            } else {
                offMaster()
            }
        }
    }

    private fun runQueryIndexSweep() {
        launch {
            if (runLock.tryLock() == false) {
                return@launch
            }
            try {
                val allMonitorMetadataDocs = MonitorMetadataService.getAllMetadataDocs()
                // Populate queryIndex --> source indices map
                val queryIndexToSourceIndicesMapping = getQueryIndexToSourceIndicesMapping(allMonitorMetadataDocs)
                // Delete all unused concrete query indices.
                // "Unused" here as in, all source indices, for which query docs are present in query index, are deleted
                val deletedQueryIndices = deleteUnusedQueryIndices(queryIndexToSourceIndicesMapping)
                // If we didn't delete any query index, we can stop here
                if (deletedQueryIndices.size == 0) {
                    return@launch
                }
                // Refresh sourceToQueryIndexMapping field in all MonitorMetadatas, now that we deleted some query indices
                refreshMonitorMetadataDocs(allMonitorMetadataDocs, deletedQueryIndices)
            } catch (e: Exception) {
                logger.error(
                    "Error cleaning up query indices",
                    e
                )
            } finally {
                runLock.unlock()
                sweepJob = threadPool
                    .scheduleWithFixedDelay({ runQueryIndexSweep() }, runPeriod, executorName())
            }
        }
    }

    private fun getQueryIndexToSourceIndicesMapping(allMonitorMetadataDocs: List<MonitorMetadata>): Map<String, Set<String>> {
        val queryIndexToSourceIndicesMapping = mutableMapOf<String, MutableSet<String>>()
        allMonitorMetadataDocs.forEach { metadata ->
            metadata.sourceToQueryIndexMapping.forEach {
                // Key is sourceIndex and monitor id concatenated
                val sourceIndexAndMonitorId = it.key
                // Take out index name from key
                val sourceIndex = sourceIndexAndMonitorId.substring(0, sourceIndexAndMonitorId.length - metadata.monitorId.length)
                val concreteQueryIndex = it.value
                // Skip writeIndex
                if (isIndexWriteIndex(clusterService, concreteQueryIndex) == false) {
                    if (queryIndexToSourceIndicesMapping.containsKey(concreteQueryIndex)) {
                        queryIndexToSourceIndicesMapping[concreteQueryIndex]?.add(sourceIndex)
                    } else {
                        queryIndexToSourceIndicesMapping[concreteQueryIndex] = mutableSetOf(sourceIndex)
                    }
                }
            }
        }
        return queryIndexToSourceIndicesMapping
    }

    private suspend fun deleteUnusedQueryIndices(queryIndexToSourceIndicesMapping: Map<String, Set<String>>): MutableSet<String> {
        val deletedQueryIndices = mutableSetOf<String>()
        queryIndexToSourceIndicesMapping.forEach {
            val concreteQueryIndex = it.key
            val sourceIndices = it.value
            // Exists API returns true only if ALL indices exist
            val indexExistsResponse: IndicesExistsResponse =
                client.admin().indices().suspendUntil { exists(IndicesExistsRequest(*sourceIndices.toTypedArray()), it) }
            if (indexExistsResponse.isExists == false) {
                try {
                    val ack: AcknowledgedResponse =
                        client.admin().indices().suspendUntil { delete(DeleteIndexRequest(concreteQueryIndex), it) }
                } catch (e: IndexNotFoundException) {
                    /** Continue if index does not exists **/
                }
                deletedQueryIndices.add(concreteQueryIndex)
            }
        }
        return deletedQueryIndices
    }

    private suspend fun refreshMonitorMetadataDocs(allMonitorMetadataDocs: List<MonitorMetadata>, deletedQueryIndices: Set<String>) {
        for (monitorMetadata in allMonitorMetadataDocs) {
            var sourceToQueryIndexMapping = monitorMetadata.sourceToQueryIndexMapping
            if (sourceToQueryIndexMapping == null) {
                continue
            }
            val removedAny = sourceToQueryIndexMapping.entries.removeIf {
                val concreteQueryIndex = it.value
                return@removeIf deletedQueryIndices.contains(concreteQueryIndex)
            }
            if (removedAny) {
                MonitorMetadataService.upsertMetadata(monitorMetadata, true)
            }
        }
    }

    companion object {
        private val logger = LogManager.getLogger(AlertIndices::class.java)
    }
}
