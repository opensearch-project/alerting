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
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
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
    private val xContentRegistry: NamedXContentRegistry,
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
            offClusterManager()
            onClusterManager()
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

                // Always clean metadata mappings for dead source indices
                cleanupMetadataMappings(allMetadata, indicesToDelete)

                if (indicesToDelete.isNotEmpty()) {
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
        // Check if index exists first
        val indexExists = try {
            val response: IndicesExistsResponse = client.suspendUntil {
                admin().indices().exists(IndicesExistsRequest(SCHEDULED_JOBS_INDEX), it)
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

        val allMetadata = mutableListOf<MonitorMetadata>()
        var searchAfterValues: Array<Any>? = null

        try {
            do {
                val searchSourceBuilder = SearchSourceBuilder()
                    .query(QueryBuilders.existsQuery(METADATA_FIELD))
                    .size(SEARCH_QUERY_RESULT_SIZE)
                    .sort("_id")

                if (searchAfterValues != null) {
                    searchSourceBuilder.searchAfter(searchAfterValues)
                }

                val searchRequest = SearchRequest(SCHEDULED_JOBS_INDEX)
                    .source(searchSourceBuilder)
                    .indicesOptions(IndicesOptions.lenientExpandOpen())

                val response: SearchResponse = client.suspendUntil {
                    search(searchRequest, it)
                }

                val hits = response.hits.hits
                if (allMetadata.isEmpty()) {
                    logger.info("Metadata query: total ${response.hits.totalHits} documents")
                }

                for (hit in hits) {
                    try {
                        val xcp = XContentHelper.createParser(
                            NamedXContentRegistry.EMPTY,
                            LoggingDeprecationHandler.INSTANCE,
                            hit.sourceRef,
                            XContentType.JSON
                        )
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                        allMetadata.add(MonitorMetadata.parse(xcp, hit.id, hit.seqNo, hit.primaryTerm))
                    } catch (e: Exception) {
                        logger.warn("Failed to parse monitor metadata: ${hit.id}", e)
                    }
                }

                searchAfterValues = if (hits.isNotEmpty()) hits.last().sortValues else null
            } while (searchAfterValues != null)
        } catch (e: Exception) {
            logger.error("Failed to fetch monitor metadata", e)
        }

        logger.info("Fetched ${allMetadata.size} monitor metadata documents")
        return allMetadata
    }

    data class QueryIndexUsage(
        val concreteQueryIndex: String,
        val aliasName: String,
        val isWriteIndex: Boolean,
        val monitorUsages: MutableMap<String, MutableSet<String>>,
    )

    private suspend fun buildQueryIndexUsageMap(allMetadata: List<MonitorMetadata>): Map<String, QueryIndexUsage> {
        val queryIndexUsageMap = mutableMapOf<String, QueryIndexUsage>()

        // Batch-fetch all monitors instead of one-by-one
        val monitorIds = allMetadata.map { it.monitorId }.toSet()
        val monitors = fetchMonitors(monitorIds)

        for (metadata in allMetadata) {
            val monitorId = metadata.monitorId
            val monitor = monitors[monitorId]

            if (monitor == null) {
                for ((sourceKey, concreteQueryIndex) in metadata.sourceToQueryIndexMapping) {
                    val aliasName = getAliasForIndex(concreteQueryIndex)
                    if (aliasName == null) {
                        logger.debug("Skipping $concreteQueryIndex: not backed by an alias")
                        continue
                    }
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

                // Only process query indices that are part of an alias
                if (!isAlias(aliasName)) {
                    logger.debug("Skipping $concreteQueryIndex: queryIndex $aliasName is not an alias")
                    continue
                }

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

            val sortedIndices = concreteIndices.sortedBy { getIndexCreationDate(it.concreteQueryIndex) }

            // Determine latest index from ALL backing indices in cluster, not just metadata
            val latestIndexInCluster = allBackingIndices.maxByOrNull { getIndexCreationDate(it) }

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
            val monitorId = metadata.monitorId
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
                    logger.info("Successfully deleted query indices: $existingIndices")
                }

                override fun onFailure(e: Exception) {
                    logger.error("Failed to delete query indices: $existingIndices. Will retry on next cleanup run.", e)
                }
            }
        )
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

    private fun isAlias(name: String): Boolean {
        return clusterService.state().metadata().hasAlias(name)
    }

    /**
     * Looks up the alias that a concrete index belongs to from cluster state.
     * Returns null if the index is not part of any alias.
     */
    private fun getAliasForIndex(concreteIndex: String): String? {
        return try {
            val indexMetadata = clusterService.state().metadata().index(concreteIndex) ?: return null
            val aliases = indexMetadata.aliases
            if (aliases.isEmpty()) return null
            // Return the first alias — query indices typically belong to exactly one alias
            aliases.keys.iterator().next()
        } catch (e: Exception) {
            logger.warn("Failed to look up alias for index: $concreteIndex", e)
            null
        }
    }

    private fun getIndexCreationDate(indexName: String): Long {
        return try {
            clusterService.state().metadata().index(indexName)?.settings?.get("index.creation_date")?.toLong() ?: 0L
        } catch (e: Exception) {
            logger.warn("Failed to get creation date for index: $indexName", e)
            0L
        }
    }

    private fun checkIfWriteIndex(aliasName: String, concreteQueryIndex: String): Boolean {
        val indicesLookup = clusterService.state().metadata().indicesLookup ?: return false
        val indexAbstraction = indicesLookup.get(aliasName) ?: return false
        val writeIndexName = indexAbstraction.writeIndex?.index?.name
        logger.debug("Checking write index for alias $aliasName: writeIndex=$writeIndexName, concrete=$concreteQueryIndex")
        return writeIndexName == concreteQueryIndex
    }

    private suspend fun fetchMonitors(monitorIds: Set<String>): Map<String, Monitor> {
        if (monitorIds.isEmpty()) return emptyMap()

        val monitors = mutableMapOf<String, Monitor>()
        // Batch in chunks to avoid overly large queries
        for (batch in monitorIds.chunked(SEARCH_QUERY_RESULT_SIZE)) {
            val searchRequest = SearchRequest(SCHEDULED_JOBS_INDEX)
                .source(
                    SearchSourceBuilder()
                        .query(QueryBuilders.termsQuery("_id", batch))
                        .size(batch.size)
                )
                .indicesOptions(IndicesOptions.lenientExpandOpen())

            try {
                val response: SearchResponse = client.suspendUntil {
                    search(searchRequest, it)
                }
                for (hit in response.hits.hits) {
                    try {
                        val xcp = XContentHelper.createParser(
                            xContentRegistry,
                            LoggingDeprecationHandler.INSTANCE,
                            hit.sourceRef,
                            XContentType.JSON
                        )
                        val job = ScheduledJob.parse(xcp, hit.id, hit.version)
                        if (job is Monitor) {
                            monitors[hit.id] = job
                        }
                    } catch (e: Exception) {
                        logger.warn("Failed to parse monitor: ${hit.id}", e)
                    }
                }
            } catch (e: Exception) {
                logger.error("Failed to batch-fetch monitors", e)
            }
        }
        logger.info("Fetched ${monitors.size} monitors out of ${monitorIds.size} requested")
        return monitors
    }

    // Uses locally cached cluster state — not a remote call to the cluster manager
    private fun indexExists(indexName: String): Boolean {
        return try {
            clusterService.state().metadata().hasIndex(indexName)
        } catch (e: Exception) {
            logger.warn("Error checking if index exists: $indexName", e)
            true
        }
    }
}
