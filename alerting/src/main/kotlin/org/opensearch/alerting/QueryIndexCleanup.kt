/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings.Companion.QUERY_INDEX_CLEANUP_ENABLED
import org.opensearch.alerting.settings.AlertingSettings.Companion.QUERY_INDEX_CLEANUP_PERIOD
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorMetadata
import org.opensearch.core.action.ActionListener
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.threadpool.Scheduler.Cancellable
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client

private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

private const val SEARCH_QUERY_RESULT_SIZE = 10000
private const val METADATA_FIELD = "metadata"

class QueryIndexCleanup(
    settings: Settings,
    private val client: Client,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService,
) : ClusterStateListener {

    private val logger = LogManager.getLogger(javaClass)

    @Volatile private var queryIndexCleanupEnabled = QUERY_INDEX_CLEANUP_ENABLED.get(settings)
    @Volatile private var queryIndexCleanupPeriod = QUERY_INDEX_CLEANUP_PERIOD.get(settings)

    private var scheduledCleanup: Cancellable? = null
    private var isClusterManager = false

    init {
        clusterService.addListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(QUERY_INDEX_CLEANUP_ENABLED) {
            queryIndexCleanupEnabled = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(QUERY_INDEX_CLEANUP_PERIOD) {
            queryIndexCleanupPeriod = it
            rescheduleCleanup()
        }
    }

    fun onClusterManager() {
        try {
            scheduledCleanup = threadPool.scheduleWithFixedDelay(
                { cleanupQueryIndices() },
                queryIndexCleanupPeriod,
                ThreadPool.Names.MANAGEMENT
            )
            logger.info("Query index cleanup scheduled with period: $queryIndexCleanupPeriod")
        } catch (e: Exception) {
            logger.error("Error scheduling query index cleanup", e)
        }
    }

    fun offClusterManager() {
        scheduledCleanup?.cancel()
        logger.info("Query index cleanup cancelled")
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        if (this.isClusterManager != event.localNodeClusterManager()) {
            this.isClusterManager = event.localNodeClusterManager()
            if (this.isClusterManager) {
                onClusterManager()
            } else {
                offClusterManager()
            }
        }
    }

    private fun rescheduleCleanup() {
        if (clusterService.state().nodes.isLocalNodeElectedClusterManager) {
            scheduledCleanup?.cancel()
            scheduledCleanup = threadPool.scheduleWithFixedDelay(
                { cleanupQueryIndices() },
                queryIndexCleanupPeriod,
                ThreadPool.Names.MANAGEMENT
            )
            logger.info("Query index cleanup rescheduled with period: $queryIndexCleanupPeriod")
        }
    }

    private fun cleanupQueryIndices() {
        if (!queryIndexCleanupEnabled) {
            logger.debug("Query index cleanup is disabled")
            return
        }

        scope.launch {
            try {
                logger.info("Starting query index cleanup")
                val startTime = System.currentTimeMillis()

                val allMetadata = fetchAllMonitorMetadata()
                val queryIndexUsageMap = buildQueryIndexUsageMap(allMetadata)
                val indicesToDelete = determineIndicesToDelete(queryIndexUsageMap)

                if (indicesToDelete.isNotEmpty()) {
                    cleanupMetadataMappings(allMetadata, indicesToDelete)
                    deleteQueryIndices(indicesToDelete)
                } else {
                    logger.info("No query indices eligible for deletion")
                }

                val duration = System.currentTimeMillis() - startTime
                logger.info("Query index cleanup completed in ${duration}ms")
            } catch (e: Exception) {
                logger.error("Error during query index cleanup", e)
            }
        }
    }

    private suspend fun fetchAllMonitorMetadata(): List<MonitorMetadata> {
        val configIndex = ".opendistro-alerting-config"

        // Check if index exists first
        val indexExists = try {
            val response: IndicesExistsResponse = client.suspendUntil {
                admin().indices().exists(IndicesExistsRequest(configIndex), it)
            }
            response.isExists
        } catch (e: Exception) {
            logger.warn("Failed to check if config index exists", e)
            false
        }

        if (!indexExists) {
            logger.info("Config index does not exist yet, skipping metadata fetch")
            return emptyList()
        }

        val searchRequest = SearchRequest(configIndex)
            .source(
                SearchSourceBuilder()
                    .query(QueryBuilders.existsQuery(METADATA_FIELD))
                    .size(SEARCH_QUERY_RESULT_SIZE)
            )
            .indicesOptions(IndicesOptions.lenientExpandOpen())

        return try {
            val response: SearchResponse = client.suspendUntil {
                search(searchRequest, it)
            }

            logger.info("Metadata query returned ${response.hits.hits.size} documents")

            response.hits.hits.mapNotNull { hit ->
                try {
                    val xcp = XContentHelper.createParser(
                        NamedXContentRegistry.EMPTY,
                        LoggingDeprecationHandler.INSTANCE,
                        hit.sourceRef,
                        XContentType.JSON
                    )
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                    MonitorMetadata.parse(xcp, hit.id, hit.seqNo, hit.primaryTerm)
                } catch (e: Exception) {
                    logger.warn("Failed to parse monitor metadata: ${hit.id}", e)
                    null
                }
            }
        } catch (e: Exception) {
            logger.error("Failed to fetch monitor metadata", e)
            emptyList()
        }
    }

    data class QueryIndexUsage(
        val concreteQueryIndex: String,
        val aliasName: String,
        val isWriteIndex: Boolean,
        val monitorUsages: MutableMap<String, MutableSet<String>>,
    )

    private suspend fun buildQueryIndexUsageMap(allMetadata: List<MonitorMetadata>): Map<String, QueryIndexUsage> {
        val queryIndexUsageMap = mutableMapOf<String, QueryIndexUsage>()

        for (metadata in allMetadata) {
            val monitorId = extractMonitorId(metadata.id)
            val monitor = getMonitor(monitorId)

            if (monitor == null) {
                for ((sourceKey, concreteQueryIndex) in metadata.sourceToQueryIndexMapping) {
                    val aliasName = extractAliasFromConcreteIndex(concreteQueryIndex)
                    val isWriteIndex = checkIfWriteIndex(aliasName, concreteQueryIndex)

                    queryIndexUsageMap.getOrPut(concreteQueryIndex) {
                        QueryIndexUsage(concreteQueryIndex, aliasName, isWriteIndex, mutableMapOf())
                    }.monitorUsages.getOrPut(monitorId) { mutableSetOf() }
                }
                continue
            }

            for ((sourceKey, concreteQueryIndex) in metadata.sourceToQueryIndexMapping) {
                val sourceIndexName = extractSourceIndexName(sourceKey, monitorId)
                val aliasName = monitor.dataSources.queryIndex
                val isWriteIndex = checkIfWriteIndex(aliasName, concreteQueryIndex)

                logger.info("Monitor $monitorId: sourceKey=$sourceKey, extracted=$sourceIndexName, queryIndex=$concreteQueryIndex")

                queryIndexUsageMap.getOrPut(concreteQueryIndex) {
                    QueryIndexUsage(concreteQueryIndex, aliasName, isWriteIndex, mutableMapOf())
                }.monitorUsages.getOrPut(monitorId) { mutableSetOf() }.add(sourceIndexName)
            }
        }

        return queryIndexUsageMap
    }

    private fun determineIndicesToDelete(queryIndexUsageMap: Map<String, QueryIndexUsage>): List<String> {
        val concreteIndicesByAlias = queryIndexUsageMap.values.groupBy { it.aliasName }
        val indicesToDelete = mutableListOf<String>()

        logger.info("Determining indices to delete. Total aliases: ${concreteIndicesByAlias.size}")

        for ((aliasName, concreteIndices) in concreteIndicesByAlias) {
            // Get ALL backing indices from cluster state, not just ones in metadata
            val allBackingIndices = try {
                val aliasMetadata = clusterService.state().metadata.indicesLookup[aliasName]
                aliasMetadata?.indices?.map { it.index.name } ?: emptyList()
            } catch (e: Exception) {
                logger.warn("Failed to get backing indices for alias $aliasName", e)
                emptyList()
            }

            logger.info("Processing alias $aliasName: ${concreteIndices.size} in metadata, ${allBackingIndices.size} in cluster")

            // Never delete if there's only one backing index in the cluster
            if (allBackingIndices.size == 1) {
                logger.info("Retaining only backing index for alias $aliasName: ${allBackingIndices.first()}")
                continue
            }

            val sortedIndices = concreteIndices.sortedBy { extractIndexNumber(it.concreteQueryIndex) }

            // Determine latest index from ALL backing indices in cluster, not just metadata
            val latestIndexInCluster = allBackingIndices.maxByOrNull { extractIndexNumber(it) }

            // Check if alias still exists
            val aliasExists = clusterService.state().metadata().indicesLookup?.get(aliasName) != null
            if (!aliasExists) {
                logger.debug("Alias $aliasName no longer exists, skipping all indices")
                continue
            }

            for (queryIndexInfo in sortedIndices) {
                // Re-check write index status at deletion time
                val isCurrentlyWriteIndex = checkIfWriteIndex(queryIndexInfo.aliasName, queryIndexInfo.concreteQueryIndex)
                if (isCurrentlyWriteIndex) {
                    logger.info("Retaining write index: ${queryIndexInfo.concreteQueryIndex}")
                    continue
                }

                // Don't delete the latest index in the cluster
                if (queryIndexInfo.concreteQueryIndex == latestIndexInCluster) {
                    logger.info("Retaining latest index in cluster: ${queryIndexInfo.concreteQueryIndex}")
                    continue
                }

                var hasActiveUsage = false
                var retentionReason: String? = null

                for ((monitorId, sourceIndices) in queryIndexInfo.monitorUsages) {
                    if (sourceIndices.isEmpty()) continue

                    logger.info("Checking sources for monitor $monitorId: $sourceIndices")

                    val firstExistingIndex = sourceIndices.firstOrNull { indexExists(it) }
                    if (firstExistingIndex != null) {
                        hasActiveUsage = true
                        retentionReason = "Source index $firstExistingIndex still exists (monitor: $monitorId)"
                        break
                    } else {
                        logger.info("All source indices deleted for monitor $monitorId: $sourceIndices")
                    }
                }

                if (!hasActiveUsage) {
                    indicesToDelete.add(queryIndexInfo.concreteQueryIndex)
                    logger.info("Marking for deletion: ${queryIndexInfo.concreteQueryIndex}")
                } else {
                    logger.debug("Retaining ${queryIndexInfo.concreteQueryIndex}: $retentionReason")
                }
            }
        }

        val retainedCount = queryIndexUsageMap.size - indicesToDelete.size
        logger.info("Query index cleanup summary: ${indicesToDelete.size} to delete, $retainedCount retained")
        return indicesToDelete
    }

    private suspend fun cleanupMetadataMappings(allMetadata: List<MonitorMetadata>, indicesToDelete: List<String>) {
        for (metadata in allMetadata) {
            val monitorId = extractMonitorId(metadata.id)
            val entriesToRemove = mutableListOf<String>()

            for ((sourceKey, concreteQueryIndex) in metadata.sourceToQueryIndexMapping) {
                val sourceIndexName = extractSourceIndexName(sourceKey, monitorId)

                if (!indexExists(sourceIndexName) || indicesToDelete.contains(concreteQueryIndex)) {
                    entriesToRemove.add(sourceKey)
                }
            }

            if (entriesToRemove.isNotEmpty()) {
                val updatedMapping = metadata.sourceToQueryIndexMapping.toMutableMap()
                entriesToRemove.forEach { updatedMapping.remove(it) }

                try {
                    MonitorMetadataService.upsertMetadata(
                        metadata.copy(sourceToQueryIndexMapping = updatedMapping),
                        true
                    )
                    logger.debug("Cleaned up ${entriesToRemove.size} entries from metadata: ${metadata.id}")
                } catch (e: Exception) {
                    logger.error("Failed to update metadata: ${metadata.id}", e)
                }
            }
        }
    }

    private fun deleteQueryIndices(indicesToDelete: List<String>) {
        if (indicesToDelete.isEmpty()) return

        logger.info("Deleting query indices: $indicesToDelete")

        // Filter to only indices that actually exist
        val existingIndices = indicesToDelete.filter { indexExists(it) }
        if (existingIndices.isEmpty()) {
            logger.info("No existing indices to delete")
            return
        }

        val deleteIndexRequest = DeleteIndexRequest(*existingIndices.toTypedArray())
        client.admin().indices().delete(
            deleteIndexRequest,
            object : ActionListener<AcknowledgedResponse> {
                override fun onResponse(response: AcknowledgedResponse) {
                    logger.info("Successfully deleted query indices: $indicesToDelete")
                }

                override fun onFailure(e: Exception) {
                    logger.error("Batch delete failed for: $indicesToDelete. Retrying individually.", e)
                    deleteQueryIndicesOneByOne(indicesToDelete)
                }
            }
        )
    }

    private fun deleteQueryIndicesOneByOne(indicesToDelete: List<String>) {
        for (index in indicesToDelete) {
            val deleteRequest = DeleteIndexRequest(index)
            client.admin().indices().delete(
                deleteRequest,
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(response: AcknowledgedResponse) {
                        logger.info("Successfully deleted query index: $index")
                    }

                    override fun onFailure(e: Exception) {
                        logger.error("Failed to delete query index: $index", e)
                    }
                }
            )
        }
    }

    internal fun extractMonitorId(metadataId: String): String {
        val parts = metadataId.split("-metadata")
        return if (parts.size > 1 && parts[0].contains("-metadata-")) {
            parts[0].substringAfterLast("-metadata-")
        } else {
            parts[0]
        }
    }

    private fun extractSourceIndexName(sourceKey: String, monitorId: String): String {
        return sourceKey.removeSuffix(monitorId)
    }

    internal fun extractAliasFromConcreteIndex(concreteQueryIndex: String): String {
        return concreteQueryIndex.substringBeforeLast("-")
    }

    internal fun extractIndexNumber(concreteQueryIndex: String): Int {
        return concreteQueryIndex.substringAfterLast("-").toIntOrNull() ?: 0
    }

    private fun checkIfWriteIndex(aliasName: String, concreteQueryIndex: String): Boolean {
        val indicesLookup = clusterService.state().metadata().indicesLookup ?: return false
        val indexAbstraction = indicesLookup.get(aliasName) ?: return false
        val writeIndexName = indexAbstraction.writeIndex?.index?.name
        logger.debug("Checking write index for alias $aliasName: writeIndex=$writeIndexName, concrete=$concreteQueryIndex")
        return writeIndexName == concreteQueryIndex
    }

    private suspend fun getMonitor(monitorId: String): Monitor? {
        val getRequest = GetRequest(".opendistro-alerting-config", monitorId)
        return try {
            val response: GetResponse = client.suspendUntil {
                get(getRequest, it)
            }
            if (response.isExists) {
                val xcp = XContentHelper.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    response.sourceAsBytesRef,
                    XContentType.JSON
                )
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                Monitor.parse(xcp, response.id, response.version)
            } else null
        } catch (e: Exception) {
            logger.warn("Error getting monitor: $monitorId", e)
            null
        }
    }

    private fun indexExists(indexName: String): Boolean {
        return try {
            clusterService.state().metadata().hasIndex(indexName)
        } catch (e: Exception) {
            logger.warn("Error checking if index exists: $indexName", e)
            true
        }
    }
}
