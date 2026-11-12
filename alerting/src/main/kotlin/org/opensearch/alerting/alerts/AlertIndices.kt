/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.alerts

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
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
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALERT_HISTORY_WRITE_INDEX
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALERT_INDEX
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_HISTORY_ENABLED
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_HISTORY_INDEX_MAX_AGE
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_HISTORY_MAX_DOCS
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_HISTORY_RETENTION_PERIOD
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_HISTORY_ROLLOVER_PERIOD
import org.opensearch.alerting.settings.AlertingSettings.Companion.FINDING_HISTORY_ENABLED
import org.opensearch.alerting.settings.AlertingSettings.Companion.FINDING_HISTORY_INDEX_MAX_AGE
import org.opensearch.alerting.settings.AlertingSettings.Companion.FINDING_HISTORY_MAX_DOCS
import org.opensearch.alerting.settings.AlertingSettings.Companion.FINDING_HISTORY_RETENTION_PERIOD
import org.opensearch.alerting.settings.AlertingSettings.Companion.FINDING_HISTORY_ROLLOVER_PERIOD
import org.opensearch.alerting.settings.AlertingSettings.Companion.REQUEST_TIMEOUT
import org.opensearch.alerting.util.CommentsUtils
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.WriteAliasRepair
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterManagerThrottlingException
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.snapshots.SnapshotInProgressException
import org.opensearch.threadpool.Scheduler.Cancellable
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.time.Instant

private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

/**
 * Class to manage the creation and rollover of alert indices and alert history indices.  In progress alerts are stored
 * in [ALERT_INDEX].  Completed alerts are written to [ALERT_HISTORY_WRITE_INDEX] which is an alias that points at the
 * current index to which completed alerts are written. [ALERT_HISTORY_WRITE_INDEX] is periodically rolled over to a new
 * date based index. The frequency of rolling over indices is controlled by the `opendistro.alerting.alert_rollover_period` setting.
 *
 * These indexes are created when first used and are then rolled over every `alert_rollover_period`. The rollover is
 * initiated on the cluster manager node to ensure only a single node tries to roll it over.  Once we have a curator functionality
 * in Scheduled Jobs we can migrate to using that to rollover the index.
 */
// TODO: reafactor to make a generic version of this class for finding and alerts
class AlertIndices(
    settings: Settings,
    private val client: Client,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService,
    private val multiTenancyEnabled: Boolean = AlertingSettings.MULTI_TENANCY_ENABLED.get(settings),
    private val writeAliasRepair: WriteAliasRepair? = null
) : ClusterStateListener {

    init {
        clusterService.addListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_HISTORY_ENABLED) { alertHistoryEnabled = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_HISTORY_MAX_DOCS) { alertHistoryMaxDocs = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_HISTORY_INDEX_MAX_AGE) { alertHistoryMaxAge = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_HISTORY_ROLLOVER_PERIOD) {
            alertHistoryRolloverPeriod = it
            rescheduleAlertRollover()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_HISTORY_RETENTION_PERIOD) {
            alertHistoryRetentionPeriod = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(REQUEST_TIMEOUT) { requestTimeout = it }

        clusterService.clusterSettings.addSettingsUpdateConsumer(FINDING_HISTORY_ENABLED) { findingHistoryEnabled = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(FINDING_HISTORY_MAX_DOCS) { findingHistoryMaxDocs = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(FINDING_HISTORY_INDEX_MAX_AGE) { findingHistoryMaxAge = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(FINDING_HISTORY_ROLLOVER_PERIOD) {
            findingHistoryRolloverPeriod = it
            rescheduleFindingRollover()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(FINDING_HISTORY_RETENTION_PERIOD) {
            findingHistoryRetentionPeriod = it
        }
    }

    companion object {

        // Deleting an index fails while it is part of an in-progress snapshot. Snapshots of large clusters routinely run
        // for several minutes, so blocked deletes are retried with exponential backoff (5, 10 and 20 minutes).
        const val MAX_HISTORY_DELETE_RETRIES = 3
        val HISTORY_DELETE_RETRY_BASE_DELAY: TimeValue = TimeValue.timeValueMinutes(5)

        /** Exponential backoff for retrying a blocked history index delete: base delay * 2^attempt. */
        fun historyDeleteRetryDelay(attempt: Int): TimeValue =
            TimeValue.timeValueMillis(HISTORY_DELETE_RETRY_BASE_DELAY.millis shl attempt)

        /**
         * Returns the indices older than [retentionPeriodMillis], except the index that still holds [writeIndex]
         * while history is enabled.
         */
        fun getIndicesToDelete(
            indicesMetadata: Collection<IndexMetadata>,
            nowMillis: Long,
            retentionPeriodMillis: Long,
            writeIndex: String,
            historyEnabled: Boolean
        ): List<String> = indicesMetadata
            .filter { nowMillis - it.creationDate > retentionPeriodMillis }
            .filterNot { historyEnabled && it.aliases.containsKey(writeIndex) }
            .map { it.index.name }

        /**
         * A delete blocked by an in-progress snapshot or by cluster-manager throttling is transient and is retried
         * later, instead of waiting a full rollover period for the next scheduled run.
         */
        fun isRetryableHistoryDeleteFailure(e: Exception): Boolean {
            val cause = ExceptionsHelper.unwrapCause(e)
            return cause is SnapshotInProgressException || cause is ClusterManagerThrottlingException
        }

        /** The in progress alert history index. */
        const val ALERT_INDEX = ".opendistro-alerting-alerts"

        /** The alias of the index in which to write alert history */
        const val ALERT_HISTORY_WRITE_INDEX = ".opendistro-alerting-alert-history-write"

        /** The alias of the index in which to write alert finding */
        const val FINDING_HISTORY_WRITE_INDEX = ".opensearch-alerting-finding-history-write"

        /** The index name pattern referring to all alert history indices */
        const val ALERT_HISTORY_ALL = ".opendistro-alerting-alert-history*"

        /** The index name pattern referring to all alert history indices */
        const val FINDING_HISTORY_ALL = ".opensearch-alerting-finding-history*"

        /** The index name pattern to create alert history indices */
        const val ALERT_HISTORY_INDEX_PATTERN = "<.opendistro-alerting-alert-history-{now/d}-1>"

        /** The index name pattern to create finding history indices */
        const val FINDING_HISTORY_INDEX_PATTERN = "<.opensearch-alerting-finding-history-{now/d}-1>"

        /** The index name pattern to query all alerts, history and current alerts. */
        const val ALL_ALERT_INDEX_PATTERN = ".opendistro-alerting-alert*"

        /** The index name pattern to query all findings, history and current findings. */
        const val ALL_FINDING_INDEX_PATTERN = ".opensearch-alerting-finding*"

        @JvmStatic
        fun alertMapping() =
            AlertIndices::class.java.getResource("alert_mapping.json").readText()

        @JvmStatic
        fun findingMapping() =
            AlertIndices::class.java.getResource("finding_mapping.json").readText()

        private val logger = LogManager.getLogger(AlertIndices::class.java)
    }

    @Volatile private var alertHistoryEnabled = AlertingSettings.ALERT_HISTORY_ENABLED.get(settings)

    @Volatile private var findingHistoryEnabled = AlertingSettings.FINDING_HISTORY_ENABLED.get(settings)

    @Volatile private var alertHistoryMaxDocs = AlertingSettings.ALERT_HISTORY_MAX_DOCS.get(settings)

    @Volatile private var findingHistoryMaxDocs = AlertingSettings.FINDING_HISTORY_MAX_DOCS.get(settings)

    @Volatile private var alertHistoryMaxAge = AlertingSettings.ALERT_HISTORY_INDEX_MAX_AGE.get(settings)

    @Volatile private var findingHistoryMaxAge = AlertingSettings.FINDING_HISTORY_INDEX_MAX_AGE.get(settings)

    @Volatile private var alertHistoryRolloverPeriod = AlertingSettings.ALERT_HISTORY_ROLLOVER_PERIOD.get(settings)

    @Volatile private var findingHistoryRolloverPeriod = AlertingSettings.FINDING_HISTORY_ROLLOVER_PERIOD.get(settings)

    @Volatile private var alertHistoryRetentionPeriod = AlertingSettings.ALERT_HISTORY_RETENTION_PERIOD.get(settings)

    @Volatile private var findingHistoryRetentionPeriod = AlertingSettings.FINDING_HISTORY_RETENTION_PERIOD.get(settings)

    @Volatile private var requestTimeout = AlertingSettings.REQUEST_TIMEOUT.get(settings)

    @Volatile private var isClusterManager = false

    // for JobsMonitor to report
    var lastRolloverTime: TimeValue? = null

    private var alertHistoryIndexInitialized: Boolean = false

    private var findingHistoryIndexInitialized: Boolean = false

    private var alertIndexInitialized: Boolean = false

    private var scheduledAlertRollover: Cancellable? = null

    private var scheduledFindingRollover: Cancellable? = null

    fun onClusterManager() {
        try {
            // try to rollover immediately as we might be restarting the cluster
            rolloverAlertHistoryIndex()
            rolloverFindingHistoryIndex()
            // schedule the next rollover for approx MAX_AGE later
            scheduledAlertRollover = threadPool
                .scheduleWithFixedDelay({ rolloverAndDeleteAlertHistoryIndices() }, alertHistoryRolloverPeriod, executorName())
            scheduledFindingRollover = threadPool
                .scheduleWithFixedDelay({ rolloverAndDeleteFindingHistoryIndices() }, findingHistoryRolloverPeriod, executorName())
        } catch (e: Exception) {
            // This should be run on cluster startup
            logger.error(
                "Error creating alert/finding indices. " +
                    "Alerts/Findings can't be recorded until clustermanager node is restarted.",
                e
            )
        }
    }

    fun offClusterManager() {
        scheduledAlertRollover?.cancel()
        scheduledFindingRollover?.cancel()
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
        alertIndexInitialized = event.state().routingTable().hasIndex(ALERT_INDEX)
        alertHistoryIndexInitialized = event.state().metadata().hasAlias(ALERT_HISTORY_WRITE_INDEX)
        findingHistoryIndexInitialized = event.state().metadata().hasAlias(FINDING_HISTORY_WRITE_INDEX)
    }

    private fun rescheduleAlertRollover() {
        if (clusterService.state().nodes.isLocalNodeElectedClusterManager) {
            scheduledAlertRollover?.cancel()
            scheduledAlertRollover = threadPool
                .scheduleWithFixedDelay({ rolloverAndDeleteAlertHistoryIndices() }, alertHistoryRolloverPeriod, executorName())
        }
    }

    private fun rescheduleFindingRollover() {
        if (clusterService.state().nodes.isLocalNodeElectedClusterManager) {
            scheduledFindingRollover?.cancel()
            scheduledFindingRollover = threadPool
                .scheduleWithFixedDelay({ rolloverAndDeleteFindingHistoryIndices() }, findingHistoryRolloverPeriod, executorName())
        }
    }

    fun isAlertInitialized(): Boolean {
        if (multiTenancyEnabled) return true
        return alertIndexInitialized && alertHistoryIndexInitialized
    }

    fun isAlertInitialized(dataSources: DataSources): Boolean {
        if (multiTenancyEnabled) return true
        val alertsIndex = dataSources.alertsIndex
        val alertsHistoryIndex = dataSources.alertsHistoryIndex
        if (alertsIndex == ALERT_INDEX && alertsHistoryIndex == ALERT_HISTORY_WRITE_INDEX) {
            return alertIndexInitialized && alertHistoryIndexInitialized
        }
        if (
            clusterService.state().metadata.indices.containsKey(alertsIndex) &&
            clusterService.state().metadata.hasAlias(alertsHistoryIndex)
        ) {
            return true
        }
        return false
    }

    fun isAlertHistoryEnabled(): Boolean {
        return alertHistoryEnabled
    }

    fun isFindingHistoryEnabled(): Boolean = findingHistoryEnabled

    suspend fun createOrUpdateAlertIndex() {
        if (multiTenancyEnabled) return
        if (!alertIndexInitialized) {
            alertIndexInitialized = createIndex(ALERT_INDEX, alertMapping())
            if (alertIndexInitialized) IndexUtils.alertIndexUpdated()
        } else {
            if (!IndexUtils.alertIndexUpdated) updateIndexMapping(ALERT_INDEX, alertMapping())
        }
        alertIndexInitialized
    }
    suspend fun createOrUpdateAlertIndex(dataSources: DataSources) {
        if (multiTenancyEnabled) return
        if (dataSources.alertsIndex == ALERT_INDEX) {
            return createOrUpdateAlertIndex()
        }
        val alertsIndex = dataSources.alertsIndex
        if (!clusterService.state().routingTable().hasIndex(alertsIndex)) {
            alertIndexInitialized = createIndex(alertsIndex!!, alertMapping())
        } else {
            updateIndexMapping(alertsIndex!!, alertMapping())
        }
    }

    suspend fun createOrUpdateInitialAlertHistoryIndex(dataSources: DataSources) {
        if (multiTenancyEnabled) return
        if (dataSources.alertsIndex == ALERT_INDEX) {
            return createOrUpdateInitialAlertHistoryIndex()
        }
        if (!clusterService.state().metadata.hasAlias(dataSources.alertsHistoryIndex)) {
            createIndex(
                dataSources.alertsHistoryIndexPattern ?: ALERT_HISTORY_INDEX_PATTERN,
                alertMapping(),
                dataSources.alertsHistoryIndex
            )
        } else {
            updateIndexMapping(
                dataSources.alertsHistoryIndex ?: ALERT_HISTORY_WRITE_INDEX,
                alertMapping(),
                true
            )
        }
    }
    suspend fun createOrUpdateInitialAlertHistoryIndex() {
        if (multiTenancyEnabled) return
        if (!alertHistoryIndexInitialized) {
            alertHistoryIndexInitialized = createIndex(ALERT_HISTORY_INDEX_PATTERN, alertMapping(), ALERT_HISTORY_WRITE_INDEX)
            if (alertHistoryIndexInitialized)
                IndexUtils.lastUpdatedAlertHistoryIndex = IndexUtils.getIndexNameWithAlias(
                    clusterService.state(),
                    ALERT_HISTORY_WRITE_INDEX
                )
        } else {
            updateIndexMapping(ALERT_HISTORY_WRITE_INDEX, alertMapping(), true)
        }
        alertHistoryIndexInitialized
    }

    suspend fun createOrUpdateInitialFindingHistoryIndex() {
        if (multiTenancyEnabled) return
        if (!findingHistoryIndexInitialized) {
            findingHistoryIndexInitialized = createIndex(FINDING_HISTORY_INDEX_PATTERN, findingMapping(), FINDING_HISTORY_WRITE_INDEX)
            if (findingHistoryIndexInitialized) {
                IndexUtils.lastUpdatedFindingHistoryIndex = IndexUtils.getIndexNameWithAlias(
                    clusterService.state(),
                    FINDING_HISTORY_WRITE_INDEX
                )
            }
        } else {
            updateIndexMapping(FINDING_HISTORY_WRITE_INDEX, findingMapping(), true)
        }
        findingHistoryIndexInitialized
    }

    suspend fun createOrUpdateInitialFindingHistoryIndex(dataSources: DataSources) {
        if (multiTenancyEnabled) return
        if (dataSources.findingsIndex == FINDING_HISTORY_WRITE_INDEX) {
            return createOrUpdateInitialFindingHistoryIndex()
        }
        val findingsIndex = dataSources.findingsIndex
        val findingsIndexPattern = dataSources.findingsIndexPattern ?: FINDING_HISTORY_INDEX_PATTERN
        if (!clusterService.state().metadata().hasAlias(findingsIndex)) {
            createIndex(
                findingsIndexPattern,
                findingMapping(),
                findingsIndex
            )
        } else {
            updateIndexMapping(findingsIndex, findingMapping(), true)
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
            .settings(
                Settings.builder()
                    .put("index.hidden", true)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put("index.auto_expand_replicas", "1-20")
                    .build()
            )

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

        // TODO call getMapping and compare actual mappings here instead of this
        if (targetIndex == IndexUtils.lastUpdatedAlertHistoryIndex || targetIndex == IndexUtils.lastUpdatedFindingHistoryIndex) {
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
            ALERT_INDEX -> IndexUtils.alertIndexUpdated()
            ALERT_HISTORY_WRITE_INDEX -> IndexUtils.lastUpdatedAlertHistoryIndex = targetIndex
            FINDING_HISTORY_WRITE_INDEX -> IndexUtils.lastUpdatedFindingHistoryIndex = targetIndex
        }
    }

    /** Repairs each alias in [aliases] that names more than one index, so a repair the restore missed happens here. Blocks until done. */
    private fun repairWriteAliases(vararg aliases: String) {
        val repair = writeAliasRepair ?: return
        runBlocking { aliases.forEach { repair.repair(it) } }
    }

    private fun rolloverAndDeleteAlertHistoryIndices() {
        repairWriteAliases(ALERT_HISTORY_WRITE_INDEX)
        if (alertHistoryEnabled) rolloverAlertHistoryIndex()
        deleteOldIndices("History", ALERT_HISTORY_ALL, alertHistoryRetentionPeriod, ALERT_HISTORY_WRITE_INDEX, alertHistoryEnabled)
    }

    private fun rolloverAndDeleteFindingHistoryIndices() {
        repairWriteAliases(FINDING_HISTORY_WRITE_INDEX)
        if (findingHistoryEnabled) rolloverFindingHistoryIndex()
        deleteOldIndices("Finding", FINDING_HISTORY_ALL, findingHistoryRetentionPeriod, FINDING_HISTORY_WRITE_INDEX, findingHistoryEnabled)
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
            .settings(
                Settings.builder()
                    .put("index.hidden", true)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put("index.auto_expand_replicas", "1-20")
                    .build()
            )
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

    private fun rolloverAlertHistoryIndex() {
        rolloverIndex(
            alertHistoryIndexInitialized,
            ALERT_HISTORY_WRITE_INDEX,
            ALERT_HISTORY_INDEX_PATTERN,
            alertMapping(),
            alertHistoryMaxDocs,
            alertHistoryMaxAge,
            ALERT_HISTORY_WRITE_INDEX
        )
    }

    private fun rolloverFindingHistoryIndex() {
        rolloverIndex(
            findingHistoryIndexInitialized,
            FINDING_HISTORY_WRITE_INDEX,
            FINDING_HISTORY_INDEX_PATTERN,
            findingMapping(),
            findingHistoryMaxDocs,
            findingHistoryMaxAge,
            FINDING_HISTORY_WRITE_INDEX
        )
    }

    /**
     * Deletes the history indices of a single history type (alerts or findings) that are older than that type's
     * retention period. Each type is evaluated only against its own retention period and its own write alias, so an
     * index of one type is never selected by the other type's retention.
     */
    private fun deleteOldIndices(
        tag: String,
        indices: String,
        retentionPeriod: TimeValue,
        writeIndex: String,
        historyEnabled: Boolean
    ) {
        logger.info("info deleteOldIndices")
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
                            val indicesMetadata = clusterStateResponse.state.metadata.indices.values
                            val indicesToDelete = getIndicesToDelete(
                                indicesMetadata,
                                Instant.now().toEpochMilli(),
                                retentionPeriod.millis,
                                writeIndex,
                                historyEnabled
                            )
                            if (indicesMetadata.any { it.index.name in indicesToDelete && it.aliases.containsKey(writeIndex) }) {
                                // The write index is being deleted because history is disabled; it is recreated on next use
                                if (writeIndex == ALERT_HISTORY_WRITE_INDEX) alertHistoryIndexInitialized = false
                                if (writeIndex == FINDING_HISTORY_WRITE_INDEX) findingHistoryIndexInitialized = false
                            }
                            logger.info("Deleting old $tag indices viz $indicesToDelete")
                            if (indices == ALERT_HISTORY_ALL) {
                                deleteAlertComments(indicesToDelete)
                            }
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

    private fun deleteAllOldHistoryIndices(indicesToDelete: List<String>) {
        if (indicesToDelete.isNotEmpty()) {
            val deleteIndexRequest = DeleteIndexRequest(*indicesToDelete.toTypedArray())
            client.admin().indices().delete(
                deleteIndexRequest,
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(deleteIndicesResponse: AcknowledgedResponse) {
                        if (!deleteIndicesResponse.isAcknowledged) {
                            logger.error(
                                "Could not delete one or more Alerting/Finding history indices: $indicesToDelete. Retrying one by one."
                            )
                            deleteOldHistoryIndex(indicesToDelete)
                        }
                    }
                    override fun onFailure(e: Exception) {
                        logger.error("Delete for Alerting/Finding History Indices $indicesToDelete Failed. Retrying one By one.")
                        deleteOldHistoryIndex(indicesToDelete)
                    }
                }
            )
        }
    }

    private fun deleteOldHistoryIndex(indicesToDelete: List<String>) {
        for (index in indicesToDelete) {
            deleteSingleHistoryIndex(index, 0)
        }
    }

    private fun deleteSingleHistoryIndex(index: String, attempt: Int) {
        // One index per request, so that a single index that cannot be deleted does not fail the others.
        client.admin().indices().delete(
            DeleteIndexRequest(index),
            object : ActionListener<AcknowledgedResponse> {
                override fun onResponse(acknowledgedResponse: AcknowledgedResponse?) {
                    if (acknowledgedResponse != null && !acknowledgedResponse.isAcknowledged) {
                        logger.error("Could not delete Alerting/Finding history index: $index")
                    }
                }
                override fun onFailure(e: Exception) {
                    if (isRetryableHistoryDeleteFailure(e) && attempt < MAX_HISTORY_DELETE_RETRIES) {
                        val delay = historyDeleteRetryDelay(attempt)
                        logger.warn(
                            "Deleting history index [$index] was blocked by [${ExceptionsHelper.unwrapCause(e).javaClass.simpleName}], " +
                                "retry ${attempt + 1}/$MAX_HISTORY_DELETE_RETRIES in $delay"
                        )
                        threadPool.schedule({ deleteSingleHistoryIndex(index, attempt + 1) }, delay, ThreadPool.Names.MANAGEMENT)
                    } else {
                        logger.warn("Failed to delete history index [$index] after ${attempt + 1} attempt(s)", e)
                    }
                }
            }
        )
    }

    private suspend fun deleteAlertComments(alertHistoryIndicesToDelete: List<String>) {
        alertHistoryIndicesToDelete.forEach { alertHistoryIndex ->
            val alertIDs = getAlertIDsFromAlertHistoryIndex(alertHistoryIndex)
            val commentIDsToDelete = CommentsUtils.getCommentIDsByAlertIDs(client, alertIDs)
            CommentsUtils.deleteComments(client, commentIDsToDelete)
        }
    }

    private suspend fun getAlertIDsFromAlertHistoryIndex(indexName: String): List<String> {
        val queryBuilder = QueryBuilders.matchAllQuery()
        val searchSourceBuilder = SearchSourceBuilder()
            .query(queryBuilder)
            .version(true)

        val searchRequest = SearchRequest()
            .indices(indexName)
            .source(searchSourceBuilder)

        val searchResponse: SearchResponse = client.suspendUntil { search(searchRequest, it) }
        val alertIDs = searchResponse.hits.map { hit ->
            val xcp = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                hit.sourceRef,
                XContentType.JSON
            )
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
            val alert = Alert.parse(xcp, hit.id, hit.version)
            alert.id
        }

        return alertIDs.distinct()
    }
}
