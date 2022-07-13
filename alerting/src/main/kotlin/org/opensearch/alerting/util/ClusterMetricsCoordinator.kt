package org.opensearch.alerting.util

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.alerting.model.ClusterMetricsDataPoint
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.component.LifecycleListener
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.threadpool.ThreadPool
import java.time.Instant
import kotlin.coroutines.CoroutineContext

private val log = org.apache.logging.log4j.LogManager.getLogger(ClusterMetricsCoordinator::class.java)

class ClusterMetricsCoordinator(
    private val client: Client,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool
) : ClusterStateListener, CoroutineScope, LifecycleListener() {

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + CoroutineName("ClusterMetricsCoordinator")

    init {
        clusterService.addListener(this)
        clusterService.addLifecycleListener(this)
    }

    override fun clusterChanged(event: ClusterChangedEvent?) {
        val scheduledJob = Runnable {
            launch {
                destinationHelper(client as NodeClient, clusterService)
            }
        }
        threadPool.scheduleWithFixedDelay(scheduledJob, TimeValue.timeValueMinutes(1), ThreadPool.Names.SYSTEM_WRITE)
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
        log.info("richfu THIS IS THE TIME $current_time")
        // cluster health for unassigned shards
        val cluster_health = client.admin().cluster().health(ClusterHealthRequest()).get().toMap()
        // cluster stats for cluster status (health), CPU usage, JVM pressure
        var cluster_stats = client.admin().cluster().clusterStats(ClusterStatsRequest()).get().toMap()
        log.info("cluster stats why not work? $cluster_stats")
        ClusterMetricsVisualizationIndex.initFunc(client, clusterService)

        var unassignedShards = cluster_health["unassigned_shards"].toString()
        log.info("this is unassigned shards $unassignedShards")
        var cluster_status = cluster_health["status"].toString()
        log.info("this is cluster status $cluster_status")
        val process_map = cluster_stats.toMap()["process"]
        log.info("this is process map $process_map")

        var cluster_status_data = ClusterMetricsDataPoint(ClusterMetricsDataPoint.MetricType.CLUSTER_STATUS, current_time, cluster_status)
        var unassigned_shards_data = ClusterMetricsDataPoint(
            ClusterMetricsDataPoint.MetricType.UNASSIGNED_SHARDS,
            current_time,
            unassignedShards
        )

        val indexRequest_status = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .source(cluster_status_data.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
        val indexRequest_shards = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .source(unassigned_shards_data.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))

        try {
            val indexResponse: IndexResponse = client.suspendUntil { client.index(indexRequest_status, it) }
            val indexResponse2: IndexResponse = client.suspendUntil { client.index(indexRequest_shards, it) }
            val failureReasons = checkShardsFailure(indexResponse)
            val failureReasons2 = checkShardsFailure(indexResponse2)
            if (failureReasons != null || failureReasons2 != null) {
                log.info("richfu failed because $failureReasons")
                log.info("richfu failed because $failureReasons2")
                return
            }
        } catch (t: Exception) {
            log.info("richfu CLUSTER METRICS NOT WORK $t")
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
