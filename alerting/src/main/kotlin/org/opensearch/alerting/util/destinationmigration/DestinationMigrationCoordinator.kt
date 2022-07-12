/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.destinationmigration

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.model.ClusterMetricsDataPoint
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.util.ClusterMetricsVisualizationIndex
import org.opensearch.alerting.util.toMap
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.component.LifecycleListener
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import java.time.Instant
import kotlin.coroutines.CoroutineContext

class DestinationMigrationCoordinator(
    private val client: Client,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool,
    private val scheduledJobIndices: ScheduledJobIndices
) : ClusterStateListener, CoroutineScope, LifecycleListener() {

    private val logger = LogManager.getLogger(javaClass)

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + CoroutineName("DestinationMigrationCoordinator")

    private var scheduledMigration: Scheduler.Cancellable? = null

    @Volatile private var runningLock = false

    init {
        clusterService.addListener(this)
        clusterService.addLifecycleListener(this)
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        logger.info("Detected cluster change event for destination migration")
        val scheduledJobHelper = Runnable {
            launch {
                destinationHelper(client as NodeClient, clusterService)
            }
        }
        threadPool.scheduleWithFixedDelay(scheduledJobHelper, TimeValue.timeValueMinutes(1), ThreadPool.Names.SYSTEM_WRITE)
        if (DestinationMigrationUtilService.finishFlag) {
            logger.info("Reset destination migration process.")
            scheduledMigration?.cancel()
            DestinationMigrationUtilService.finishFlag = false
        }
        if (
            event.localNodeMaster() &&
            !runningLock &&
            (scheduledMigration == null || scheduledMigration!!.isCancelled)
        ) {
            logger.info("richfu destination migration")
            try {
                logger.info("richfu try before")
                runningLock = true
                initMigrateDestinations()
                logger.info("richfu try after")
            } finally {
                logger.info("richfu finally before")
                runningLock = false
                logger.info("richfu finally after")
            }
        } else if (!event.localNodeMaster()) {
            logger.info("richfu no destination migration")
            scheduledMigration?.cancel()
        }
    }

    private fun initMigrateDestinations() {
        logger.info("start of initMigrateDestination")
        logger.info("includes $clusterService")
        if (!scheduledJobIndices.scheduledJobIndexExists()) {
            logger.debug("Alerting config index is not initialized")
            scheduledMigration?.cancel()
            return
        }

        if (!clusterService.state().nodes().isLocalNodeElectedMaster) {
            scheduledMigration?.cancel()
            return
        }

        if (DestinationMigrationUtilService.finishFlag) {
            logger.info("Destination migration is already complete, cancelling migration process.")
            scheduledMigration?.cancel()
            return
        }

        val scheduledJob = Runnable {
            launch {
                try {
                    if (DestinationMigrationUtilService.finishFlag) {
                        logger.info("Cancel background destination migration process.")
                        scheduledMigration?.cancel()
                    }
                    logger.info("richfu calling class")
                    logger.info("richfu after called class")
                    logger.info("Performing migration of destination data.")
                    DestinationMigrationUtilService.migrateDestinations(client as NodeClient)
                } catch (e: Exception) {
                    logger.error("Failed to migrate destination data", e)
                }
            }
        }

        logger.info("richfu before scheduledMigration")
        scheduledMigration = threadPool.scheduleWithFixedDelay(scheduledJob, TimeValue.timeValueMinutes(1), ThreadPool.Names.MANAGEMENT)
        logger.info("richfu after scheduledMigration call")
    }

    suspend fun destinationHelper(client: NodeClient, clusterService: ClusterService) {
        /*
        get time from Instant.now(), use this variable for all 4 documents that I'm going to create
        get clusterHealth API (status + unassigned shards)
            check whether clusterMetricsVisualizationIndex exists
                if not, create
            create document data point:
                parse data from the clusterHealth API to get values
            call index API to index the document
         */
        val current_time = Instant.now().toString()
        // cluster health for unassigned shards
        val cluster_health = client.admin().cluster().health(ClusterHealthRequest()).get().toMap()
        // cluster stats for cluster status (health), CPU usage, JVM pressure
        var cluster_stats = client.admin().cluster().clusterStats(ClusterStatsRequest()).get().toMap()

        ClusterMetricsVisualizationIndex.initFunc(client, clusterService)

        var unassignedShards = cluster_health["unassigned_shards"]
        logger.info("this is unassigned shards $unassignedShards")
        var cluster_status = cluster_health["status"].toString()
        logger.info("this is cluster status $cluster_status")
//        var CPU_usage = cluster_stats.
//        logger.info("this is CPU usage $CPU_usage")
//        var JVM_pressure = cluster_stats

        var cluster_status_data = ClusterMetricsDataPoint(ClusterMetricsDataPoint.MetricType.CLUSTER_STATUS, current_time, cluster_status)

        val indexRequest = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .source(cluster_status_data.toXContent(jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))

        try {
            val indexResponse: IndexResponse = client.suspendUntil { client.index(indexRequest, it) }
            val failureReasons = checkShardsFailure(indexResponse)
//            if (failureReasons != null) {
//                actionListener.onFailure(
//                    AlertingException.wrap(OpenSearchStatusException(failureReasons.toString(), indexResponse.status()))
//                )
//                return
//            }
        } catch (t: Exception) {
            logger.info("richfu CLUSTER METRICS NOT WORK $t")
//            actionListener.onFailure(AlertingException.wrap(t))
        }
    }
    fun checkShardsFailure(response: IndexResponse): String? {
        val failureReasons = StringBuilder()
        if (response.shardInfo.failed > 0) {
            response.shardInfo.failures.forEach {
                    entry ->
                failureReasons.append(entry.reason())
            }
            return failureReasons.toString()
        }
        return null
    }
}
