package org.opensearch.alerting.util

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.opensearch.action.ActionListener
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
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.DeleteByQueryAction
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder
import org.opensearch.threadpool.ThreadPool
import java.time.Instant
import kotlin.coroutines.CoroutineContext

private val log = org.apache.logging.log4j.LogManager.getLogger(ClusterMetricsCoordinator::class.java)

class ClusterMetricsCoordinator(
    private val client: Client,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool
) : ClusterStateListener, CoroutineScope, LifecycleListener() {

    companion object {
        @Volatile
        var isRunningFlag = false
            internal set
    }

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
                deleteDocs(client)
            }
        }
        if (event!!.localNodeMaster() && !isRunningFlag) {
            threadPool.scheduleWithFixedDelay(scheduledJob, TimeValue.timeValueMinutes(1), ThreadPool.Names.SYSTEM_WRITE)
            isRunningFlag = true
        }
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
//        val curTime = current_time.
        log.info("richfu THIS IS THE TIME $current_time")
        // cluster health for unassigned shards
        val cluster_health = client.admin().cluster().health(ClusterHealthRequest()).get().toMap()
        // cluster stats for cluster status (health), CPU usage, JVM pressure
        var cluster_stats = client.admin().cluster().clusterStats(ClusterStatsRequest()).get().toMap()

        ClusterMetricsVisualizationIndex.initFunc(client, clusterService)

        var unassignedShards = cluster_health["unassigned_shards"].toString()
        log.info("this is unassigned shards $unassignedShards")
        var cluster_status = cluster_health["status"].toString()
        log.info("this is cluster status $cluster_status")
        val nodes_map = cluster_stats["nodes"] as Map<String, Any>
        val process_map = nodes_map["process"] as Map<String, Any>
        val cpu_map = process_map["cpu"] as Map<String, Any>
        val percent = cpu_map["percent"].toString()
        log.info("THIS IS CPU USAGE $percent")
        val jvm_map = nodes_map["jvm"] as Map<String, Any>
        val mem_map = jvm_map["mem"] as Map<String, Any>
        val mem_used = mem_map["heap_used_in_bytes"]
        val mem_avail = mem_map["heap_max_in_bytes"]
        var jvm_pressure = "0.00"

        if (mem_used is Int && mem_avail is Int) {
            val jvm_pressure_num = ((mem_used.toDouble() / mem_avail.toDouble()) * 100)
            jvm_pressure = String.format("%.2f", jvm_pressure_num)
        }
        log.info("THIS IS JVM PRESSURE $jvm_pressure")

        val cluster_status_data = ClusterMetricsDataPoint(
            ClusterMetricsDataPoint.MetricType.CLUSTER_STATUS,
            current_time,
            cluster_status
        )
        val unassigned_shards_data = ClusterMetricsDataPoint(
            ClusterMetricsDataPoint.MetricType.UNASSIGNED_SHARDS,
            current_time,
            unassignedShards
        )
        val cpu_usage_data = ClusterMetricsDataPoint(
            ClusterMetricsDataPoint.MetricType.CPU_USAGE,
            current_time,
            percent
        )
        val jvm_data = ClusterMetricsDataPoint(
            ClusterMetricsDataPoint.MetricType.JVM_PRESSURE,
            current_time,
            jvm_pressure
        )

        val indexRequest_status = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .source(cluster_status_data.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
        val indexRequest_shards = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .source(unassigned_shards_data.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
        val indexRequest_cpu = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .source(cpu_usage_data.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
        val indexRequest_jvm = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .source(jvm_data.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))

        try {
            val indexResponse: IndexResponse = client.suspendUntil { client.index(indexRequest_status, it) }
            val indexResponse2: IndexResponse = client.suspendUntil { client.index(indexRequest_shards, it) }
            val indexResponse3: IndexResponse = client.suspendUntil { client.index(indexRequest_cpu, it) }
            val indexResponse4: IndexResponse = client.suspendUntil { client.index(indexRequest_jvm, it) }
            val failureReasons = checkShardsFailure(indexResponse)
            val failureReasons2 = checkShardsFailure(indexResponse2)
            val failureReasons3 = checkShardsFailure(indexResponse3)
            val failureReasons4 = checkShardsFailure(indexResponse4)
            if (failureReasons != null || failureReasons2 != null || failureReasons3 != null || failureReasons4 != null) {
                log.info("richfu failed because $failureReasons")
                log.info("richfu failed because $failureReasons2")
                log.info("richfu failed because $failureReasons3")
                log.info("richfu failed because $failureReasons4")
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

    fun deleteDocs(client: NodeClient) {

        DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
            .source(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .filter(QueryBuilders.rangeQuery("cluster_status.timestamp").lte("now-10m/m"))
            .execute(
                object : ActionListener<BulkByScrollResponse> {
                    override fun onResponse(response: BulkByScrollResponse) {
                    }

                    override fun onFailure(t: Exception) {
                    }
                }
            )
        log.info("deleted cluster_status data from 10 minutes ago")

        DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
            .source(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .filter(QueryBuilders.rangeQuery("cpu_usage.timestamp").lte("now-10m/m"))
            .execute(
                object : ActionListener<BulkByScrollResponse> {
                    override fun onResponse(response: BulkByScrollResponse) {
                    }

                    override fun onFailure(t: Exception) {
                    }
                }
            )
        log.info("deleted cpu_usage data from 10 minutes ago")

        DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
            .source(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .filter(QueryBuilders.rangeQuery("jvm_pressure.timestamp").lte("now-10m/m"))
            .execute(
                object : ActionListener<BulkByScrollResponse> {
                    override fun onResponse(response: BulkByScrollResponse) {
                    }

                    override fun onFailure(t: Exception) {
                    }
                }
            )
        log.info("deleted jvm_pressure data from 10 minutes ago")

        DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
            .source(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .filter(QueryBuilders.rangeQuery("unassigned_shards.timestamp").lte("now-10m/m"))
            .execute(
                object : ActionListener<BulkByScrollResponse> {
                    override fun onResponse(response: BulkByScrollResponse) {
                    }

                    override fun onFailure(t: Exception) {
                    }
                }
            )
        log.info("deleted unassigned_shards data from 10 minutes ago")
    }
}
