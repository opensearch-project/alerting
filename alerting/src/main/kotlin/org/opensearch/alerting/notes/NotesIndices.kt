/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.notes

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
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.action.ActionListener
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import java.time.Instant

/**
 * Initialize the OpenSearch components required to run Notes.
 *
 */
class NotesIndices(
    settings: Settings,
    private val client: Client,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService
) : ClusterStateListener {

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.NOTES_HISTORY_ENABLED) { notesHistoryEnabled = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.NOTES_HISTORY_MAX_DOCS) { notesHistoryMaxDocs = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.NOTES_HISTORY_INDEX_MAX_AGE) { notesHistoryMaxAge = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.NOTES_HISTORY_ROLLOVER_PERIOD) {
            notesHistoryRolloverPeriod = it
            rescheduleNotesRollover()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.NOTES_HISTORY_RETENTION_PERIOD) {
            notesHistoryRetentionPeriod = it
        }
    }

    companion object {
        /** The alias of the index in which to write notes finding */
        const val NOTES_HISTORY_WRITE_INDEX = ".opensearch-alerting-notes-history-write"

        /** The index name pattern referring to all notes history indices */
        const val NOTES_HISTORY_ALL = ".opensearch-alerting-notes-history*"

        /** The index name pattern to create notes history indices */
        const val NOTES_HISTORY_INDEX_PATTERN = "<.opensearch-alerting-notes-history-{now/d}-1>"

        /** The index name pattern to query all notes, history and current notes. */
        const val ALL_NOTES_INDEX_PATTERN = ".opensearch-alerting-notes*"

        @JvmStatic
        fun notesMapping() =
            NotesIndices::class.java.getResource("alerting_notes.json").readText()

        private val logger = LogManager.getLogger(AlertIndices::class.java)
    }

    @Volatile private var notesHistoryEnabled = AlertingSettings.NOTES_HISTORY_ENABLED.get(settings)

    @Volatile private var notesHistoryMaxDocs = AlertingSettings.NOTES_HISTORY_MAX_DOCS.get(settings)

    @Volatile private var notesHistoryMaxAge = AlertingSettings.NOTES_HISTORY_INDEX_MAX_AGE.get(settings)

    @Volatile private var notesHistoryRolloverPeriod = AlertingSettings.NOTES_HISTORY_ROLLOVER_PERIOD.get(settings)

    @Volatile private var notesHistoryRetentionPeriod = AlertingSettings.NOTES_HISTORY_RETENTION_PERIOD.get(settings)

    @Volatile private var isClusterManager = false

    // for JobsMonitor to report
    var lastRolloverTime: TimeValue? = null

    private var notesHistoryIndexInitialized: Boolean = false

    private var scheduledNotesRollover: Scheduler.Cancellable? = null

    /**
     * Initialize the indices required for Alerting Notes.
     * First check if the index exists, and if not create the index with the provided callback listeners.
     *
     * @param actionListener A callback listener for the index creation call. Generally in the form of onSuccess, onFailure
     */

    fun onMaster() {
        try {
            // try to rollover immediately as we might be restarting the cluster
            rolloverNotesHistoryIndex()
            // schedule the next rollover for approx MAX_AGE later
            scheduledNotesRollover = threadPool
                .scheduleWithFixedDelay({ rolloverAndDeleteNotesHistoryIndices() }, notesHistoryRolloverPeriod, executorName())
        } catch (e: Exception) {
            // This should be run on cluster startup
            logger.error(
                "Error creating notes indices. Notes can't be recorded until master node is restarted.",
                e
            )
        }
    }

    fun offMaster() {
        scheduledNotesRollover?.cancel()
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

        // if the indexes have been deleted they need to be reinitialized
        notesHistoryIndexInitialized = event.state().metadata().hasAlias(NOTES_HISTORY_WRITE_INDEX)
    }

    private fun rescheduleNotesRollover() {
        if (clusterService.state().nodes.isLocalNodeElectedMaster) {
            scheduledNotesRollover?.cancel()
            scheduledNotesRollover = threadPool
                .scheduleWithFixedDelay({ rolloverAndDeleteNotesHistoryIndices() }, notesHistoryRolloverPeriod, executorName())
        }
    }

    fun isNotesHistoryInitialized(): Boolean {
        return clusterService.state().metadata.hasAlias(NOTES_HISTORY_WRITE_INDEX)
    }

    fun isNotesHistoryEnabled(): Boolean {
        return notesHistoryEnabled
    }

    suspend fun createOrUpdateInitialNotesHistoryIndex() {
        if (!isNotesHistoryInitialized()) {
            notesHistoryIndexInitialized = createIndex(NOTES_HISTORY_INDEX_PATTERN, notesMapping(), NOTES_HISTORY_WRITE_INDEX)
            if (notesHistoryIndexInitialized)
                IndexUtils.lastUpdatedNotesHistoryIndex = IndexUtils.getIndexNameWithAlias(
                    clusterService.state(),
                    NOTES_HISTORY_WRITE_INDEX
                )
        } else {
            updateIndexMapping(NOTES_HISTORY_WRITE_INDEX, notesMapping(), true)
        }
        notesHistoryIndexInitialized
    }

    private fun rolloverAndDeleteNotesHistoryIndices() {
        rolloverNotesHistoryIndex()
        deleteOldIndices("Notes", NOTES_HISTORY_ALL)
    }

    private fun rolloverNotesHistoryIndex() {
        rolloverIndex(
            notesHistoryIndexInitialized,
            NOTES_HISTORY_WRITE_INDEX,
            NOTES_HISTORY_INDEX_PATTERN,
            notesMapping(),
            notesHistoryMaxDocs,
            notesHistoryMaxAge,
            NOTES_HISTORY_WRITE_INDEX
        )
    }

    // TODO: Everything below here are util functions straight from AlertIndices.kt
    // TODO: might need to reuse their code or refactor
    // TODO: may merge into AlertIndices.kt if we decide to make notes indices
    // TODO: component-specific instead of universal and component-agnostic

    private fun getIndicesToDelete(clusterStateResponse: ClusterStateResponse): List<String> {
        val indicesToDelete = mutableListOf<String>()
        for (entry in clusterStateResponse.state.metadata.indices) {
            val indexMetaData = entry.value
            getHistoryIndexToDelete(indexMetaData, notesHistoryRetentionPeriod.millis, NOTES_HISTORY_WRITE_INDEX, true)
                ?.let { indicesToDelete.add(it) }
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
                } else if (writeIndex == NOTES_HISTORY_WRITE_INDEX) {
                    // Otherwise reset notesHistoryIndexInitialized since index will be deleted
                    notesHistoryIndexInitialized = false
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
                                "Could not delete one or more Notes history indices: $indicesToDelete." +
                                    "Retrying one by one."
                            )
                            deleteOldHistoryIndex(indicesToDelete)
                        }
                    }
                    override fun onFailure(e: Exception) {
                        logger.error("Delete for Notes History Indices $indicesToDelete Failed. Retrying one By one.")
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
                                logger.error("Could not delete one or more Notes history indices: $index")
                            }
                        }
                    }
                    override fun onFailure(e: Exception) {
                        logger.debug("Exception ${e.message} while deleting the index $index")
                    }
                }
            )
        }
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
                throw t
            }
        }
    }

    private suspend fun updateIndexMapping(index: String, mapping: String, alias: Boolean = false) {
        val clusterState = clusterService.state()
        var targetIndex = index
        if (alias) {
            targetIndex = IndexUtils.getIndexNameWithAlias(clusterState, index)
        }

        if (targetIndex == IndexUtils.lastUpdatedNotesHistoryIndex
        ) {
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
            NOTES_HISTORY_WRITE_INDEX -> IndexUtils.lastUpdatedNotesHistoryIndex = targetIndex
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
        logger.info("in rolloverIndex, initialize: $initialized")
        if (!initialized) {
            return
        }

        logger.info("sending rollover request")
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
                        logger.info("$writeIndex rolled over. Conditions were: ${response.conditionStatus}")
                        lastRolloverTime = TimeValue.timeValueMillis(threadPool.absoluteTimeInMillis())
                    }
                }
                override fun onFailure(e: Exception) {
                    logger.error("$writeIndex not roll over failed.")
                }
            }
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
                        val indicesToDelete = getIndicesToDelete(clusterStateResponse)
                        logger.info("Deleting old $tag indices viz $indicesToDelete")
                        deleteAllOldHistoryIndices(indicesToDelete)
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
}
