/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.alerting.alerts

import org.apache.logging.log4j.LogManager
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.ActionListener
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
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALERT_INDEX
import org.opensearch.alerting.alerts.AlertIndices.Companion.HISTORY_WRITE_INDEX
import org.opensearch.alerting.elasticapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_HISTORY_ENABLED
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_HISTORY_INDEX_MAX_AGE
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_HISTORY_MAX_DOCS
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_HISTORY_ROLLOVER_PERIOD
import org.opensearch.alerting.settings.AlertingSettings.Companion.REQUEST_TIMEOUT
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentType
import org.opensearch.threadpool.Scheduler.Cancellable
import org.opensearch.threadpool.ThreadPool
import java.time.Instant

/**
 * Class to manage the creation and rollover of alert indices and alert history indices.  In progress alerts are stored
 * in [ALERT_INDEX].  Completed alerts are written to [HISTORY_WRITE_INDEX] which is an alias that points at the
 * current index to which completed alerts are written. [HISTORY_WRITE_INDEX] is periodically rolled over to a new
 * date based index. The frequency of rolling over indices is controlled by the `opendistro.alerting.alert_rollover_period` setting.
 *
 * These indexes are created when first used and are then rolled over every `alert_rollover_period`. The rollover is
 * initiated on the master node to ensure only a single node tries to roll it over.  Once we have a curator functionality
 * in Scheduled Jobs we can migrate to using that to rollover the index.
 */
class AlertIndices(
    settings: Settings,
    private val client: Client,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService
) : ClusterStateListener {

    init {
        clusterService.addListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_HISTORY_ENABLED) { historyEnabled = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_HISTORY_MAX_DOCS) { historyMaxDocs = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_HISTORY_INDEX_MAX_AGE) { historyMaxAge = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_HISTORY_ROLLOVER_PERIOD) {
            historyRolloverPeriod = it
            rescheduleRollover()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.ALERT_HISTORY_RETENTION_PERIOD) {
            historyRetentionPeriod = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(REQUEST_TIMEOUT) { requestTimeout = it }
    }

    companion object {

        /** The in progress alert history index. */
        const val ALERT_INDEX = ".opendistro-alerting-alerts"

        /** The Elastic mapping type */
        const val MAPPING_TYPE = "_doc"

        /** The alias of the index in which to write alert history */
        const val HISTORY_WRITE_INDEX = ".opendistro-alerting-alert-history-write"

        /** The index name pattern referring to all alert history indices */
        const val HISTORY_ALL = ".opendistro-alerting-alert-history*"

        /** The index name pattern to create alert history indices */
        const val HISTORY_INDEX_PATTERN = "<.opendistro-alerting-alert-history-{now/d}-1>"

        /** The index name pattern to query all alerts, history and current alerts. */
        const val ALL_INDEX_PATTERN = ".opendistro-alerting-alert*"

        @JvmStatic
        fun alertMapping() =
            AlertIndices::class.java.getResource("alert_mapping.json").readText()

        private val logger = LogManager.getLogger(AlertIndices::class.java)
    }

    @Volatile private var historyEnabled = AlertingSettings.ALERT_HISTORY_ENABLED.get(settings)

    @Volatile private var historyMaxDocs = AlertingSettings.ALERT_HISTORY_MAX_DOCS.get(settings)

    @Volatile private var historyMaxAge = AlertingSettings.ALERT_HISTORY_INDEX_MAX_AGE.get(settings)

    @Volatile private var historyRolloverPeriod = AlertingSettings.ALERT_HISTORY_ROLLOVER_PERIOD.get(settings)

    @Volatile private var historyRetentionPeriod = AlertingSettings.ALERT_HISTORY_RETENTION_PERIOD.get(settings)

    @Volatile private var requestTimeout = AlertingSettings.REQUEST_TIMEOUT.get(settings)

    @Volatile private var isMaster = false

    // for JobsMonitor to report
    var lastRolloverTime: TimeValue? = null

    private var historyIndexInitialized: Boolean = false

    private var alertIndexInitialized: Boolean = false

    private var scheduledRollover: Cancellable? = null

    fun onMaster() {
        try {
            // try to rollover immediately as we might be restarting the cluster
            rolloverHistoryIndex()
            // schedule the next rollover for approx MAX_AGE later
            scheduledRollover = threadPool
                .scheduleWithFixedDelay({ rolloverAndDeleteHistoryIndices() }, historyRolloverPeriod, executorName())
        } catch (e: Exception) {
            // This should be run on cluster startup
            logger.error(
                "Error creating alert indices. " +
                    "Alerts can't be recorded until master node is restarted.",
                e
            )
        }
    }

    fun offMaster() {
        scheduledRollover?.cancel()
    }

    private fun executorName(): String {
        return ThreadPool.Names.MANAGEMENT
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        // Instead of using a LocalNodeMasterListener to track master changes, this service will
        // track them here to avoid conditions where master listener events run after other
        // listeners that depend on what happened in the master listener
        if (this.isMaster != event.localNodeMaster()) {
            this.isMaster = event.localNodeMaster()
            if (this.isMaster) {
                onMaster()
            } else {
                offMaster()
            }
        }

        // if the indexes have been deleted they need to be reinitialized
        alertIndexInitialized = event.state().routingTable().hasIndex(ALERT_INDEX)
        historyIndexInitialized = event.state().metadata().hasAlias(HISTORY_WRITE_INDEX)
    }

    private fun rescheduleRollover() {
        if (clusterService.state().nodes.isLocalNodeElectedMaster) {
            scheduledRollover?.cancel()
            scheduledRollover = threadPool
                .scheduleWithFixedDelay({ rolloverAndDeleteHistoryIndices() }, historyRolloverPeriod, executorName())
        }
    }

    fun isInitialized(): Boolean {
        return alertIndexInitialized && historyIndexInitialized
    }

    fun isHistoryEnabled(): Boolean = historyEnabled

    suspend fun createOrUpdateAlertIndex() {
        if (!alertIndexInitialized) {
            alertIndexInitialized = createIndex(ALERT_INDEX)
            if (alertIndexInitialized) IndexUtils.alertIndexUpdated()
        } else {
            if (!IndexUtils.alertIndexUpdated) updateIndexMapping(ALERT_INDEX)
        }
        alertIndexInitialized
    }

    suspend fun createOrUpdateInitialHistoryIndex() {
        if (!historyIndexInitialized) {
            historyIndexInitialized = createIndex(HISTORY_INDEX_PATTERN, HISTORY_WRITE_INDEX)
            if (historyIndexInitialized)
                IndexUtils.lastUpdatedHistoryIndex = IndexUtils.getIndexNameWithAlias(clusterService.state(), HISTORY_WRITE_INDEX)
        } else {
            updateIndexMapping(HISTORY_WRITE_INDEX, true)
        }
        historyIndexInitialized
    }

    private suspend fun createIndex(index: String, alias: String? = null): Boolean {
        // This should be a fast check of local cluster state. Should be exceedingly rare that the local cluster
        // state does not contain the index and multiple nodes concurrently try to create the index.
        // If it does happen that error is handled we catch the ResourceAlreadyExistsException
        val existsResponse: IndicesExistsResponse = client.admin().indices().suspendUntil {
            exists(IndicesExistsRequest(index).local(true), it)
        }
        if (existsResponse.isExists) return true

        val request = CreateIndexRequest(index)
            .mapping(MAPPING_TYPE, alertMapping(), XContentType.JSON)
            .settings(Settings.builder().put("index.hidden", true).build())

        if (alias != null) request.alias(Alias(alias))
        return try {
            val createIndexResponse: CreateIndexResponse = client.admin().indices().suspendUntil { create(request, it) }
            createIndexResponse.isAcknowledged
        } catch (e: ResourceAlreadyExistsException) {
            true
        }
    }

    private suspend fun updateIndexMapping(index: String, alias: Boolean = false) {
        val clusterState = clusterService.state()
        val mapping = alertMapping()
        var targetIndex = index
        if (alias) {
            targetIndex = IndexUtils.getIndexNameWithAlias(clusterState, index)
        }

        if (targetIndex == IndexUtils.lastUpdatedHistoryIndex) {
            return
        }

        var putMappingRequest: PutMappingRequest = PutMappingRequest(targetIndex).type(MAPPING_TYPE)
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
            HISTORY_WRITE_INDEX -> IndexUtils.lastUpdatedHistoryIndex = targetIndex
        }
    }

    private fun rolloverAndDeleteHistoryIndices() {
        if (historyEnabled) rolloverHistoryIndex()
        deleteOldHistoryIndices()
    }

    private fun rolloverHistoryIndex() {
        if (!historyIndexInitialized) {
            return
        }

        // We have to pass null for newIndexName in order to get Elastic to increment the index count.
        val request = RolloverRequest(HISTORY_WRITE_INDEX, null)
        request.createIndexRequest.index(HISTORY_INDEX_PATTERN)
            .mapping(MAPPING_TYPE, alertMapping(), XContentType.JSON)
            .settings(Settings.builder().put("index.hidden", true).build())
        request.addMaxIndexDocsCondition(historyMaxDocs)
        request.addMaxIndexAgeCondition(historyMaxAge)
        client.admin().indices().rolloverIndex(
            request,
            object : ActionListener<RolloverResponse> {
                override fun onResponse(response: RolloverResponse) {
                    if (!response.isRolledOver) {
                        logger.info("$HISTORY_WRITE_INDEX not rolled over. Conditions were: ${response.conditionStatus}")
                    } else {
                        lastRolloverTime = TimeValue.timeValueMillis(threadPool.absoluteTimeInMillis())
                    }
                }
                override fun onFailure(e: Exception) {
                    logger.error("$HISTORY_WRITE_INDEX not roll over failed.")
                }
            }
        )
    }

    private fun deleteOldHistoryIndices() {

        val clusterStateRequest = ClusterStateRequest()
            .clear()
            .indices(HISTORY_ALL)
            .metadata(true)
            .local(true)
            .indicesOptions(IndicesOptions.strictExpand())

        client.admin().cluster().state(
            clusterStateRequest,
            object : ActionListener<ClusterStateResponse> {
                override fun onResponse(clusterStateResponse: ClusterStateResponse) {
                    if (!clusterStateResponse.state.metadata.indices.isEmpty) {
                        val indicesToDelete = getIndicesToDelete(clusterStateResponse)
                        logger.info("Deleting old history indices viz $indicesToDelete")
                        deleteAllOldHistoryIndices(indicesToDelete)
                    } else {
                        logger.info("No Old History Indices to delete")
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
            val creationTime = indexMetaData.creationDate

            if ((Instant.now().toEpochMilli() - creationTime) > historyRetentionPeriod.millis) {
                val alias = indexMetaData.aliases.firstOrNull { HISTORY_WRITE_INDEX == it.value.alias }
                if (alias != null) {
                    if (historyEnabled) {
                        // If the index has the write alias and history is enabled, don't delete the index
                        continue
                    } else {
                        // Otherwise reset historyIndexInitialized since index will be deleted
                        historyIndexInitialized = false
                    }
                }

                indicesToDelete.add(indexMetaData.index.name)
            }
        }
        return indicesToDelete
    }

    private fun deleteAllOldHistoryIndices(indicesToDelete: List<String>) {
        if (indicesToDelete.isNotEmpty()) {
            val deleteIndexRequest = DeleteIndexRequest(*indicesToDelete.toTypedArray())
            client.admin().indices().delete(
                deleteIndexRequest,
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(deleteIndicesResponse: AcknowledgedResponse) {
                        if (!deleteIndicesResponse.isAcknowledged) {
                            logger.error("Could not delete one or more Alerting history indices: $indicesToDelete. Retrying one by one.")
                            deleteOldHistoryIndex(indicesToDelete)
                        }
                    }
                    override fun onFailure(e: Exception) {
                        logger.error("Delete for Alerting History Indices $indicesToDelete Failed. Retrying one By one.")
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
                                logger.error("Could not delete one or more Alerting history indices: $index")
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
}
