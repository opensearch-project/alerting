package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALERT_HISTORY_WRITE_INDEX
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALERT_INDEX
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALL_ALERT_INDEX_PATTERN
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.AlertV2.Companion.EXPIRATION_TIME_FIELD
import org.opensearch.core.action.ActionListener
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.DeleteByQueryAction
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.time.Instant
import java.util.concurrent.TimeUnit

private val logger = LogManager.getLogger(AlertV2Expirer::class.java)

class AlertV2Expirer(
    private val client: Client,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService,
) : ClusterStateListener {

    init {
        clusterService.addListener(this)
    }

    @Volatile private var isClusterManager = false

    private var alertIndexInitialized = false

    private var alertHistoryIndexInitialized = false

    private var scheduledAlertsV2CheckAndExpire: Scheduler.Cancellable? = null

    private val executorName = ThreadPool.Names.MANAGEMENT

    private val checkForExpirationInterval = TimeValue(1L, TimeUnit.MINUTES)

    override fun clusterChanged(event: ClusterChangedEvent) {
        // Instead of using a LocalNodeClusterManagerListener to track clustermanager changes, this service will
        // track them here to avoid conditions where clustermanager listener events run after other
        // listeners that depend on what happened in the clustermanager listener
        if (this.isClusterManager != event.localNodeClusterManager()) {
            this.isClusterManager = event.localNodeClusterManager()
            if (this.isClusterManager) {
                onManager()
            } else {
                offManager()
            }
        }

        alertIndexInitialized = event.state().routingTable().hasIndex(ALERT_INDEX)
        alertHistoryIndexInitialized = event.state().metadata().hasAlias(ALERT_HISTORY_WRITE_INDEX)
    }

    fun onManager() {
        try {
            // try to sweep current AlertV2s immediately as we might be restarting the cluster
            expireAlertV2s()
            // schedule expiration checks and expirations to happen repeatedly at some interval
            scheduledAlertsV2CheckAndExpire = threadPool
                .scheduleWithFixedDelay({ expireAlertV2s() }, checkForExpirationInterval, executorName)
        } catch (e: Exception) {
            // This should be run on cluster startup
            logger.error(
                "Error creating comments indices. Comments can't be recorded until clustermanager node is restarted.",
                e
            )
        }
    }

    fun offManager() {
        scheduledAlertsV2CheckAndExpire?.cancel()
    }

    private fun expireAlertV2s() {
        if (!areAlertsIndicesInitialized()) {
            // TODO: edge case: what if alert history indices are present but regular alerts index is absent
            return
        }

        try {
            val deleteByQuery = QueryBuilders.rangeQuery(EXPIRATION_TIME_FIELD)
                .lte(Instant.now().toEpochMilli())

            DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                .source(ALL_ALERT_INDEX_PATTERN)
                .filter(deleteByQuery)
                .refresh(true)
                .execute(
                    object : ActionListener<BulkByScrollResponse> {
                        override fun onResponse(response: BulkByScrollResponse) {
                            logger.info("Deleted ${response.deleted} expired alerts")
                        }
                        override fun onFailure(e: Exception) {
                            logger.error("Failed to delete expired alerts", e)
                        }
                    }
                )
        } catch (e: Exception) {
            logger.error("Error during alert cleanup", e)
        }
    }

    private fun areAlertsIndicesInitialized(): Boolean {
        return alertIndexInitialized && alertHistoryIndexInitialized
    }
}
