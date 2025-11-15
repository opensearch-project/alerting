/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.alertsv2

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.action.admin.cluster.state.ClusterStateResponse
import org.opensearch.action.admin.indices.alias.Alias
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.admin.indices.rollover.RolloverRequest
import org.opensearch.action.admin.indices.rollover.RolloverResponse
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_V2_HISTORY_ENABLED
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_V2_HISTORY_INDEX_MAX_AGE
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_V2_HISTORY_MAX_DOCS
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_V2_HISTORY_RETENTION_PERIOD
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_V2_HISTORY_ROLLOVER_PERIOD
import org.opensearch.alerting.settings.AlertingSettings.Companion.REQUEST_TIMEOUT
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.threadpool.Scheduler.Cancellable
import org.opensearch.threadpool.ThreadPool
import java.time.Instant

private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
private val logger = LogManager.getLogger(AlertV2Indices::class.java)

/**
 * This class handles the rollover and management of v2 alerts history indices
 *
 * @opensearch.experimental
 */
class AlertV2Indices(
    settings: Settings,
    private val client: Client,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService
) : ClusterStateListener {

    init {
        clusterService.addListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_V2_HISTORY_ENABLED) { alertV2HistoryEnabled = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_V2_HISTORY_MAX_DOCS) { alertV2HistoryMaxDocs = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_V2_HISTORY_INDEX_MAX_AGE) { alertV2HistoryMaxAge = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_V2_HISTORY_ROLLOVER_PERIOD) {
            alertV2HistoryRolloverPeriod = it
            rescheduleAlertRollover()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_V2_HISTORY_RETENTION_PERIOD) {
            alertV2HistoryRetentionPeriod = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(REQUEST_TIMEOUT) { requestTimeout = it }
    }

    companion object {

        /** The in progress alert history index. */
        const val ALERT_V2_INDEX = ".opensearch-alerting-v2-alerts"

        /** The alias of the index in which to write alert history */
        const val ALERT_V2_HISTORY_WRITE_INDEX = ".opensearch-alerting-v2-alert-history-write"

        /** The index name pattern referring to all alert history indices */
        const val ALERT_V2_HISTORY_ALL = ".opensearch-alerting-v2-alert-history*"

        /** The index name pattern to create alert history indices */
        const val ALERT_V2_HISTORY_INDEX_PATTERN = "<.opensearch-alerting-v2-alert-history-{now/d}-1>"

        /** The index name pattern to query all alerts, history and current alerts. */
        const val ALL_ALERT_V2_INDEX_PATTERN = ".opensearch-alerting-v2-alert*"

        @JvmStatic
        fun alertV2Mapping() =
            AlertV2Indices::class.java.getResource("alert_v2_mapping.json").readText()
    }

    @Volatile private var alertV2HistoryEnabled = ALERT_V2_HISTORY_ENABLED.get(settings)

    @Volatile private var alertV2HistoryMaxDocs = ALERT_V2_HISTORY_MAX_DOCS.get(settings)

    @Volatile private var alertV2HistoryMaxAge = ALERT_V2_HISTORY_INDEX_MAX_AGE.get(settings)

    @Volatile private var alertV2HistoryRolloverPeriod = ALERT_V2_HISTORY_ROLLOVER_PERIOD.get(settings)

    @Volatile private var alertV2HistoryRetentionPeriod = ALERT_V2_HISTORY_RETENTION_PERIOD.get(settings)

    @Volatile private var requestTimeout = REQUEST_TIMEOUT.get(settings)

    @Volatile private var isClusterManager = false

    // for JobsMonitor to report
    var lastRolloverTime: TimeValue? = null

    private var alertV2HistoryIndexInitialized: Boolean = false

    private var alertV2IndexInitialized: Boolean = false

    private var scheduledAlertV2Rollover: Cancellable? = null

    fun onClusterManager() {
        try {
            // try to rollover immediately as we might be restarting the cluster
            rolloverAlertV2HistoryIndex()

            // schedule the next rollover for approx MAX_AGE later
            scheduledAlertV2Rollover = threadPool
                .scheduleWithFixedDelay({ rolloverAndDeleteAlertV2HistoryIndices() }, alertV2HistoryRolloverPeriod, executorName())
        } catch (e: Exception) {
            logger.error("Error rolling over alerts v2 history index.", e)
        }
    }

    fun offClusterManager() {
        scheduledAlertV2Rollover?.cancel()
    }

    private fun executorName(): String {
        return ThreadPool.Names.MANAGEMENT
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        // Instead of using a LocalNodeClusterManagerListener to track clustermanager changes, this service will
        // track them here to avoid conditions where clustermanager listener events run after other
        // listeners that depend on what happened in the clustermanager listener
        if (this.isClusterManager != event.localNodeClusterManager()) {
            this.isClusterManager = event.localNodeClusterManager()
            if (this.isClusterManager) {
                onClusterManager()
            } else {
                offClusterManager()
            }
        }

        // if the indexes have been deleted they need to be reinitialized
        alertV2IndexInitialized = event.state().routingTable().hasIndex(ALERT_V2_INDEX)
        alertV2HistoryIndexInitialized = event.state().metadata().hasAlias(ALERT_V2_HISTORY_WRITE_INDEX)
    }

    private fun rescheduleAlertRollover() {
        if (clusterService.state().nodes.isLocalNodeElectedClusterManager) {
            scheduledAlertV2Rollover?.cancel()
            scheduledAlertV2Rollover = threadPool
                .scheduleWithFixedDelay({ rolloverAndDeleteAlertV2HistoryIndices() }, alertV2HistoryRolloverPeriod, executorName())
        }
    }

    suspend fun createOrUpdateAlertV2Index() {
        if (!alertV2IndexInitialized) {
            alertV2IndexInitialized = createIndex(ALERT_V2_INDEX, alertV2Mapping())
            if (alertV2IndexInitialized) IndexUtils.alertIndexUpdated()
        } else {
            if (!IndexUtils.alertIndexUpdated) updateIndexMapping(ALERT_V2_INDEX, alertV2Mapping())
        }
        alertV2IndexInitialized
    }

    suspend fun createOrUpdateInitialAlertV2HistoryIndex() {
        if (!alertV2HistoryIndexInitialized) {
            alertV2HistoryIndexInitialized = createIndex(ALERT_V2_HISTORY_INDEX_PATTERN, alertV2Mapping(), ALERT_V2_HISTORY_WRITE_INDEX)
            if (alertV2HistoryIndexInitialized)
                IndexUtils.lastUpdatedAlertV2HistoryIndex = IndexUtils.getIndexNameWithAlias(
                    clusterService.state(),
                    ALERT_V2_HISTORY_WRITE_INDEX
                )
        } else {
            updateIndexMapping(ALERT_V2_HISTORY_WRITE_INDEX, alertV2Mapping(), true)
        }
        alertV2HistoryIndexInitialized
    }

    fun isAlertV2Initialized(): Boolean {
        return alertV2IndexInitialized && alertV2HistoryIndexInitialized
    }

    private fun rolloverAndDeleteAlertV2HistoryIndices() {
        if (alertV2HistoryEnabled) rolloverAlertV2HistoryIndex()
        deleteOldIndices("History", ALERT_V2_HISTORY_ALL)
    }

    private suspend fun createIndex(index: String, schemaMapping: String, alias: String? = null): Boolean {
        // This should be a fast check of local cluster state. Should be exceedingly rare that the local cluster
        // state does not contain the index and multiple nodes concurrently try to create the index.
        // If it does happen that error is handled we catch the ResourceAlreadyExistsException
        val existsResponse: IndicesExistsResponse = client.admin().indices().suspendUntil {
            exists(IndicesExistsRequest(index).local(true), it)
        }
        if (existsResponse.isExists) return true

        logger.debug("index: [$index] schema mappings: [$schemaMapping]")
        val request = CreateIndexRequest(index)
            .mapping(schemaMapping)
            .settings(Settings.builder().put("index.hidden", true).build())

        if (alias != null) request.alias(Alias(alias))
        return try {
            val createIndexResponse: CreateIndexResponse = client.admin().indices().suspendUntil { create(request, it) }
            createIndexResponse.isAcknowledged
        } catch (t: Exception) {
            if (ExceptionsHelper.unwrapCause(t) is ResourceAlreadyExistsException) {
                true
            } else {
                throw AlertingException.wrap(t)
            }
        }
    }

    private suspend fun updateIndexMapping(index: String, mapping: String, alias: Boolean = false) {
        val clusterState = clusterService.state()
        var targetIndex = index
        if (alias) {
            targetIndex = IndexUtils.getIndexNameWithAlias(clusterState, index)
        }

        if (targetIndex == IndexUtils.lastUpdatedAlertV2HistoryIndex) {
            return
        }

        val putMappingRequest: PutMappingRequest = PutMappingRequest(targetIndex)
            .source(mapping, XContentType.JSON)
        val updateResponse: AcknowledgedResponse = client.admin().indices().suspendUntil { putMapping(putMappingRequest, it) }
        if (updateResponse.isAcknowledged) {
            logger.info("Index mapping of $targetIndex is updated")
            setIndexUpdateFlag(index, targetIndex)
        } else {
            logger.info("Failed to update index mapping of $targetIndex")
        }
    }

    private fun setIndexUpdateFlag(index: String, targetIndex: String) {
        when (index) {
            ALERT_V2_INDEX -> IndexUtils.alertV2IndexUpdated()
            ALERT_V2_HISTORY_WRITE_INDEX -> IndexUtils.lastUpdatedAlertV2HistoryIndex = targetIndex
        }
    }

    private fun rolloverIndex(
        initialized: Boolean,
        index: String,
        pattern: String,
        map: String,
        docsCondition: Long,
        ageCondition: TimeValue,
        writeIndex: String
    ) {
        if (!initialized) {
            return
        }

        // We have to pass null for newIndexName in order to get Elastic to increment the index count.
        val request = RolloverRequest(index, null)
        request.createIndexRequest.index(pattern)
            .mapping(map)
            .settings(Settings.builder().put("index.hidden", true).build())
        request.addMaxIndexDocsCondition(docsCondition)
        request.addMaxIndexAgeCondition(ageCondition)
        client.admin().indices().rolloverIndex(
            request,
            object : ActionListener<RolloverResponse> {
                override fun onResponse(response: RolloverResponse) {
                    if (!response.isRolledOver) {
                        logger.info("$writeIndex not rolled over. Conditions were: ${response.conditionStatus}")
                    } else {
                        lastRolloverTime = TimeValue.timeValueMillis(threadPool.absoluteTimeInMillis())
                    }
                }
                override fun onFailure(e: Exception) {
                    logger.error("$writeIndex not roll over failed.")
                }
            }
        )
    }

    private fun rolloverAlertV2HistoryIndex() {
        rolloverIndex(
            alertV2HistoryIndexInitialized,
            ALERT_V2_HISTORY_WRITE_INDEX,
            ALERT_V2_HISTORY_INDEX_PATTERN,
            alertV2Mapping(),
            alertV2HistoryMaxDocs,
            alertV2HistoryMaxAge,
            ALERT_V2_HISTORY_WRITE_INDEX
        )
    }

    private fun deleteOldIndices(tag: String, indices: String) {
        val clusterStateRequest = ClusterStateRequest()
            .clear()
            .indices(indices)
            .metadata(true)
            .local(true)
            .indicesOptions(IndicesOptions.strictExpand())
        client.admin().cluster().state(
            clusterStateRequest,
            object : ActionListener<ClusterStateResponse> {
                override fun onResponse(clusterStateResponse: ClusterStateResponse) {
                    if (clusterStateResponse.state.metadata.indices.isNotEmpty()) {
                        scope.launch {
                            val indicesToDelete = getIndicesToDelete(clusterStateResponse)
                            logger.info("Deleting old $tag indices viz $indicesToDelete")
                            deleteAllOldHistoryIndices(indicesToDelete)
                        }
                    } else {
                        logger.info("No Old $tag Indices to delete")
                    }
                }
                override fun onFailure(e: Exception) {
                    logger.error("Error fetching cluster state")
                }
            }
        )
    }

    private fun getIndicesToDelete(clusterStateResponse: ClusterStateResponse): List<String> {
        val indicesToDelete = mutableListOf<String>()
        for (entry in clusterStateResponse.state.metadata.indices) {
            val indexMetaData = entry.value
            getHistoryIndexToDelete(
                indexMetaData,
                alertV2HistoryRetentionPeriod.millis,
                ALERT_V2_HISTORY_WRITE_INDEX,
                alertV2HistoryEnabled
            )?.let { indicesToDelete.add(it) }
        }
        return indicesToDelete
    }

    private fun getHistoryIndexToDelete(
        indexMetadata: IndexMetadata,
        retentionPeriodMillis: Long,
        writeIndex: String,
        historyEnabled: Boolean
    ): String? {
        val creationTime = indexMetadata.creationDate
        if ((Instant.now().toEpochMilli() - creationTime) > retentionPeriodMillis) {
            val alias = indexMetadata.aliases.entries.firstOrNull { writeIndex == it.value.alias }
            if (alias != null) {
                if (historyEnabled) {
                    // If the index has the write alias and history is enabled, don't delete the index
                    return null
                } else if (writeIndex == ALERT_V2_HISTORY_WRITE_INDEX) {
                    // Otherwise reset alertHistoryIndexInitialized since index will be deleted
                    alertV2HistoryIndexInitialized = false
                }
            }

            return indexMetadata.index.name
        }
        return null
    }

    private fun deleteAllOldHistoryIndices(indicesToDelete: List<String>) {
        if (indicesToDelete.isNotEmpty()) {
            val deleteIndexRequest = DeleteIndexRequest(*indicesToDelete.toTypedArray())
            client.admin().indices().delete(
                deleteIndexRequest,
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(deleteIndicesResponse: AcknowledgedResponse) {
                        if (!deleteIndicesResponse.isAcknowledged) {
                            logger.error(
                                "Could not delete one or more Alerting V2 history indices: $indicesToDelete. Retrying one by one."
                            )
                            deleteOldHistoryIndex(indicesToDelete)
                        }
                    }
                    override fun onFailure(e: Exception) {
                        logger.error("Delete for Alerting V2 History Indices $indicesToDelete Failed. Retrying one by one.")
                        deleteOldHistoryIndex(indicesToDelete)
                    }
                }
            )
        }
    }

    private fun deleteOldHistoryIndex(indicesToDelete: List<String>) {
        for (index in indicesToDelete) {
            val singleDeleteRequest = DeleteIndexRequest(*indicesToDelete.toTypedArray())
            client.admin().indices().delete(
                singleDeleteRequest,
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(acknowledgedResponse: AcknowledgedResponse?) {
                        if (acknowledgedResponse != null) {
                            if (!acknowledgedResponse.isAcknowledged) {
                                logger.error("Could not delete one or more Alerting V2 history indices: $index")
                            }
                        }
                    }
                    override fun onFailure(e: Exception) {
                        logger.error("Exception ${e.message} while deleting the index $index")
                    }
                }
            )
        }
    }
}
