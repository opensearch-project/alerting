/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.alerts

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.apache.logging.log4j.LogManager
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALERT_INDEX
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.aggregations.bucket.terms.Terms
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client

/**
 * An anti-entropy background job that periodically scans for alerts whose monitors no longer
 * exist in .opendistro-alerting-config ("orphaned alerts") and moves them to the alert history
 * index with state = DELETED.
 *
 * Normally, when a monitor is deleted, the JobSweeper's IndexingOperationListener.postDelete
 * callback fires, which deschedules the monitor and moves its alerts to history via AlertMover.
 * However, postDelete does not always fire reliably — particularly when a monitor deletion
 * occurs while one of its executions is in flight. In this case,
 * alerts remain stranded in the active alerts index indefinitely.
 *
 * This sweeper is not a bug fix for postDelete — it is a safety net. It runs independently on the
 * cluster manager node and cleans up any orphaned alerts that postDelete missed, ensuring the
 * system eventually converges to a consistent state.
 *
 * Runs only on the cluster manager node to avoid duplicate work.
 */
class OrphanedAlertSweeper(
    settings: Settings,
    private val client: Client,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService
) : ClusterStateListener {

    private val logger = LogManager.getLogger(OrphanedAlertSweeper::class.java)
    private val scope = CoroutineScope(Dispatchers.IO)

    @Volatile private var sweepPeriod = SWEEP_PERIOD.get(settings)
    @Volatile private var enabled = ENABLED.get(settings)
    private var scheduledSweep: Scheduler.Cancellable? = null
    private var isClusterManager = false

    init {
        clusterService.addListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(SWEEP_PERIOD) {
            sweepPeriod = it
            if (clusterService.state().nodes.isLocalNodeElectedClusterManager && enabled) reschedule()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ENABLED) {
            enabled = it
            if (clusterService.state().nodes.isLocalNodeElectedClusterManager) {
                if (enabled) reschedule() else cancel()
            }
        }
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        if (this.isClusterManager != event.localNodeClusterManager()) {
            this.isClusterManager = event.localNodeClusterManager()
            if (this.isClusterManager && enabled) {
                reschedule()
            } else {
                cancel()
            }
        }
    }

    private fun reschedule() {
        cancel()
        scheduledSweep = threadPool.scheduleWithFixedDelay(
            { scope.launch { withTimeout(SWEEP_TIMEOUT_MS) { sweepOrphanedAlerts() } } },
            sweepPeriod,
            ThreadPool.Names.MANAGEMENT
        )
        logger.info("Orphaned alert sweeper scheduled with period: $sweepPeriod")
    }

    private fun cancel() {
        scheduledSweep?.cancel()
        scheduledSweep = null
    }

    internal suspend fun sweepOrphanedAlerts() {
        try {
            if (!clusterService.state().routingTable().hasIndex(ALERT_INDEX)) {
                logger.debug("Alert index does not exist, skipping sweep")
                return
            }

            val alertMonitorIds = getAlertMonitorIds()
            if (alertMonitorIds.isEmpty()) {
                logger.debug("No alerts found in active index")
                return
            }

            // Check which monitors still exist in the config index
            val existingIds = if (clusterService.state().routingTable().hasIndex(ScheduledJob.SCHEDULED_JOBS_INDEX)) {
                getExistingMonitorIds(alertMonitorIds)
            } else {
                emptySet()
            }

            val orphanedMonitorIds = alertMonitorIds - existingIds
            if (orphanedMonitorIds.isEmpty()) {
                logger.debug("No orphaned alerts found")
                return
            }

            logger.info("Found ${orphanedMonitorIds.size} orphaned monitor(s) with active alerts: $orphanedMonitorIds")

            // Move orphaned alerts to history
            for (monitorId in orphanedMonitorIds) {
                try {
                    AlertMover.moveAlerts(client, monitorId, null)
                    logger.info("Cleaned up orphaned alerts for monitor [$monitorId]")
                } catch (e: Exception) {
                    logger.error("Failed to clean up orphaned alerts for monitor [$monitorId]", e)
                }
            }
        } catch (e: Exception) {
            logger.error("Error during orphaned alert sweep", e)
        }
    }

    // retrieves the monitor IDs referenced in the .opendistro-alerting-alerts index's alerts
    // the possibility here is that some of these monitor IDs might no longer exist
    private suspend fun getAlertMonitorIds(): Set<String> {
        val agg = TermsAggregationBuilder("monitor_ids")
            .field(Alert.MONITOR_ID_FIELD)
            .size(1000)
        val searchSource = SearchSourceBuilder()
            .size(0)
            .aggregation(agg)
        val searchRequest = SearchRequest(ALERT_INDEX).source(searchSource)

        val response: SearchResponse = client.suspendUntil { search(searchRequest, it) }
        val termsAgg = response.aggregations?.get<Terms>("monitor_ids") ?: return emptySet()
        return termsAgg.buckets.map { it.keyAsString }.toSet()
    }

    // retrieves the monitor IDs that currently exist in .opendistro-alerting-config
    // this is the source of truth for what monitors do and do not exist
    private suspend fun getExistingMonitorIds(monitorIds: Set<String>): Set<String> {
        val searchSource = SearchSourceBuilder()
            .size(monitorIds.size)
            .query(QueryBuilders.idsQuery().addIds(*monitorIds.toTypedArray()))
            .fetchSource(false)
        val searchRequest = SearchRequest(ScheduledJob.SCHEDULED_JOBS_INDEX).source(searchSource)

        val response: SearchResponse = client.suspendUntil { search(searchRequest, it) }
        return response.hits.hits.map { it.id }.toSet()
    }

    companion object {
        val SWEEP_PERIOD = AlertingSettings.ORPHANED_ALERT_SWEEP_PERIOD
        val ENABLED = AlertingSettings.ORPHANED_ALERT_SWEEP_ENABLED
        const val SWEEP_TIMEOUT_MS = 60_000L
    }
}
