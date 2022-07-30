/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.alerting.settings.AlertingSettings.Companion.METRICS_EXECUTION_FREQUENCY
import org.opensearch.alerting.settings.AlertingSettings.Companion.METRICS_STORE_TIME
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.component.LifecycleListener
import org.opensearch.common.settings.Settings
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.DeleteByQueryAction
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import java.time.Instant
import java.util.*
import kotlin.coroutines.CoroutineContext

private val log = org.apache.logging.log4j.LogManager.getLogger(ClusterMetricsCoordinator::class.java)

class ClusterMetricsCoordinator(
    private val settings: Settings,
    private val client: Client,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool
) : ClusterStateListener, CoroutineScope, LifecycleListener() {

    @Volatile private var metricsExecutionFrequency = METRICS_EXECUTION_FREQUENCY.get(settings)
    @Volatile private var metricsStoreTime = METRICS_STORE_TIME.get(settings)
    private var dataPointCollectionDeletionJob: Scheduler.Cancellable? = null
    companion object {
        @Volatile
        var isRunningFlag = false
            internal set
        var isDeletionUpdated = false
            internal set
        var isCollectionUpdated = false
            internal set
    }

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + CoroutineName("ClusterMetricsCoordinator")

    init {
        clusterService.addListener(this)
        clusterService.addLifecycleListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(METRICS_EXECUTION_FREQUENCY) {
            metricsExecutionFrequency = it
            isCollectionUpdated = true
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(METRICS_STORE_TIME) {
            metricsStoreTime = it
            isDeletionUpdated = true
        }
    }

    override fun clusterChanged(event: ClusterChangedEvent?) {
        if (metricsStoreTime > metricsExecutionFrequency) {
            val scheduledJobCollection = Runnable {
                launch {
                    createDocs(client as NodeClient, clusterService)
                    deleteDocs(client)
                }
            }
            if (isCollectionUpdated || isDeletionUpdated) {
                dataPointCollectionDeletionJob?.cancel()
                log.info("cancelled data collection and deletion jobs ")
                isRunningFlag = false
                log.info("detected changes to settings, resetting running, deletion and collection flags to false")
                isDeletionUpdated = false
                isCollectionUpdated = false
            }
            // if cluster changed event occurs (if settings are changed)
            // cancel current scheduled job and change runningFlag to false.
            // maybe have individual runningFlags for collection and deletion?
            if (event!!.localNodeMaster() && !isRunningFlag) {
                log.info("cluster changed metricsExecutionFrequency = $metricsExecutionFrequency")
                dataPointCollectionDeletionJob = threadPool.scheduleWithFixedDelay(
                    scheduledJobCollection,
                    metricsExecutionFrequency,
                    ThreadPool.Names.SYSTEM_WRITE
                )
                isRunningFlag = true
            }
        } else {
            throw java.lang.IllegalArgumentException(
                "Execution frequency ($metricsExecutionFrequency) can not be greater than the storage time ($metricsStoreTime). "
            )
        }
    }

    suspend fun createDocs(client: NodeClient, clusterService: ClusterService) {
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
        var nodeStats = client.admin().cluster().nodesStats(NodesStatsRequest().addMetrics("process", "jvm")).get().toMap()

        ClusterMetricsVisualizationIndex.initFunc(client, clusterService)

        val unassignedShards = cluster_health["unassigned_shards"].toString()
        log.info("this is unassigned shards $unassignedShards")
        val clusterStatus = cluster_health["status"].toString()
        log.info("this is cluster status $clusterStatus")
        val numPending = cluster_health["number_of_pending_tasks"].toString()
        log.info("this is number of pending tasks $numPending")
        val activeShards = cluster_health["active_shards"].toString()
        log.info("this is number of active shards $activeShards")
        val relocatingShards = cluster_health["relocating_shards"].toString()
        log.info("This is number of relocatingShards $relocatingShards")

        val nodesMap = nodeStats["nodes"] as Map<String, Any>
        val keys = nodesMap.keys
        log.info("this is nodesMap keys $keys")
        val jvmData = arrayListOf<Int>()
        val cpuData = arrayListOf<Int>()

        for (key in keys) {
            val keyData = nodesMap[key] as Map<String, Any>
            log.info("this is keyData $keyData")
            val processMap = keyData["process"] as Map<String, Any>
            log.info("This is osMap $processMap")
            val cpuMap = processMap["cpu"] as Map<String, Any>
            log.info("This is cpuMap $cpuMap")
            val percent = cpuMap["percent"]
            cpuData.add(percent as Int)

            val jvmMap = keyData["jvm"] as Map<String, Any>
            log.info("this is jvmMap $jvmMap")
            val memMap = jvmMap["mem"] as Map<String, Any>
            log.info("this is memMap $memMap")
            val pressure = memMap["heap_used_percent"]
            jvmData.add(pressure as Int)
        }
        log.info("this is CPU data $cpuData")
        log.info("this is JVM data $jvmData")

        val minimumCPU = Collections.min(cpuData)
        val maximumCPU = Collections.max(cpuData)
        log.info("this is min CPU $minimumCPU")
        log.info("this is max CPU $maximumCPU")

        val minimumJVM = Collections.min(jvmData)
        val maximumJVM = Collections.max(jvmData)
        log.info("this is max JVM $maximumJVM")
        log.info("this is min JVM $minimumJVM")

        var avgCPU = 0
        var avgJVM = 0

        for (i in cpuData.indices) {
            avgCPU += cpuData[i]
            avgJVM += jvmData[i]
        }

        avgCPU /= cpuData.size
        avgJVM /= jvmData.size
        log.info("this is avgCPU $avgCPU")
        log.info("this is avgJVM $avgJVM")
//
//        val clusterStatus_data = ClusterMetricsDataPoint(
//            ClusterMetricsDataPoint.MetricType.CLUSTER_STATUS,
//            current_time,
//            clusterStatusRandom
//        )
//        val unassigned_shards_data = ClusterMetricsDataPoint(
//            ClusterMetricsDataPoint.MetricType.UNASSIGNED_SHARDS,
//            current_time,
//            randomUnassignedShards
//        )
//        val cpu_usage_data = ClusterMetricsDataPoint(
//            ClusterMetricsDataPoint.MetricType.CPU_USAGE,
//            current_time,
//            percent
//        )
//        val jvm_data = ClusterMetricsDataPoint(
//            ClusterMetricsDataPoint.MetricType.JVM_PRESSURE,
//            current_time,
//            jvm_pressure
//        )
//        val pendingTasksData = ClusterMetricsDataPoint(
//            ClusterMetricsDataPoint.MetricType.NUMBER_OF_PENDING_TASKS,
//            current_time,
//            numPending
//        )
//        val activeShardsData = ClusterMetricsDataPoint(
//            ClusterMetricsDataPoint.MetricType.ACTIVE_SHARDS,
//            current_time,
//            activeShards
//        )
//        val relocatingShardsData = ClusterMetricsDataPoint(
//            ClusterMetricsDataPoint.MetricType.RELOCATING_SHARDS,
//            current_time,
//            relocatingShards
//        )
//
//        val indexRequest_status = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
//            .source(clusterStatus_data.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
//        val indexRequest_shards = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
//            .source(unassigned_shards_data.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
//        val indexRequest_cpu = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
//            .source(cpu_usage_data.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
//        val indexRequest_jvm = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
//            .source(jvm_data.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
//        val indexRequest_pending = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
//            .source(pendingTasksData.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
//        val indexRequest_active = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
//            .source(activeShardsData.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
//        val indexRequest_relocating = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
//            .source(relocatingShardsData.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
//
//        try {
//            val indexResponse: IndexResponse = client.suspendUntil { client.index(indexRequest_status, it) }
//            val indexResponse2: IndexResponse = client.suspendUntil { client.index(indexRequest_shards, it) }
//            val indexResponse3: IndexResponse = client.suspendUntil { client.index(indexRequest_cpu, it) }
//            val indexResponse4: IndexResponse = client.suspendUntil { client.index(indexRequest_jvm, it) }
//            val indexResponse5: IndexResponse = client.suspendUntil { client.index(indexRequest_pending, it) }
//            val indexResponse6: IndexResponse = client.suspendUntil { client.index(indexRequest_active, it) }
//            val indexResponse7: IndexResponse = client.suspendUntil { client.index(indexRequest_relocating, it) }
//            val failureReasons = checkShardsFailure(indexResponse)
//            val failureReasons2 = checkShardsFailure(indexResponse2)
//            val failureReasons3 = checkShardsFailure(indexResponse3)
//            val failureReasons4 = checkShardsFailure(indexResponse4)
//            val failureReasons5 = checkShardsFailure(indexResponse5)
//            val failureReasons6 = checkShardsFailure(indexResponse6)
//            val failureReasons7 = checkShardsFailure(indexResponse7)
//            if (
//                failureReasons != null ||
//                failureReasons2 != null ||
//                failureReasons3 != null ||
//                failureReasons4 != null ||
//                failureReasons5 != null ||
//                failureReasons6 != null ||
//                failureReasons7 != null
//            ) {
//                log.info("richfu failed because $failureReasons")
//                log.info("richfu failed because $failureReasons2")
//                log.info("richfu failed because $failureReasons3")
//                log.info("richfu failed because $failureReasons4")
//                log.info("richfu failed because $failureReasons5")
//                log.info("richfu failed because $failureReasons6")
//                log.info("richfu failed because $failureReasons7")
//                return
//            }
//        } catch (t: Exception) {
//            log.info("richfu CLUSTER METRICS NOT WORK $t")
//        }
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
        val documentAge = metricsStoreTime.toString()
        val unitTime = documentAge.last()
        log.info("documentAge returns $documentAge, unitTime returns $unitTime")

        DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
            .source(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .filter(QueryBuilders.rangeQuery("cluster_status.timestamp").lte("now-$documentAge/$unitTime"))
            .execute(
                object : ActionListener<BulkByScrollResponse> {
                    override fun onResponse(response: BulkByScrollResponse) {
                    }

                    override fun onFailure(t: Exception) {
                    }
                }
            )
        log.info("deleted clusterStatus data from $documentAge/$unitTime ago, now-$documentAge")

        DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
            .source(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .filter(QueryBuilders.rangeQuery("cpu_usage.timestamp").lte("now-$documentAge/$unitTime"))
            .execute(
                object : ActionListener<BulkByScrollResponse> {
                    override fun onResponse(response: BulkByScrollResponse) {
                    }

                    override fun onFailure(t: Exception) {
                    }
                }
            )
        log.info("deleted cpu_usage data from $documentAge/$unitTime ago")

        DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
            .source(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .filter(QueryBuilders.rangeQuery("jvm_pressure.timestamp").lte("now-$documentAge/$unitTime"))
            .execute(
                object : ActionListener<BulkByScrollResponse> {
                    override fun onResponse(response: BulkByScrollResponse) {
                    }

                    override fun onFailure(t: Exception) {
                    }
                }
            )
        log.info("deleted jvm_pressure data from $documentAge/$unitTime ago")

        DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
            .source(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .filter(QueryBuilders.rangeQuery("unassigned_shards.timestamp").lte("now-$documentAge/$unitTime"))
            .execute(
                object : ActionListener<BulkByScrollResponse> {
                    override fun onResponse(response: BulkByScrollResponse) {
                    }

                    override fun onFailure(t: Exception) {
                    }
                }
            )
        log.info("deleted unassigned_shards data from $documentAge/$unitTime ago")

        DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
            .source(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .filter(QueryBuilders.rangeQuery("number_of_pending_tasks.timestamp").lte("now-$documentAge/$unitTime"))
            .execute(
                object : ActionListener<BulkByScrollResponse> {
                    override fun onResponse(response: BulkByScrollResponse) {
                    }

                    override fun onFailure(t: Exception) {
                    }
                }
            )
        log.info("deleted number of pending tasks from $documentAge/$unitTime ago")

        DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
            .source(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .filter(QueryBuilders.rangeQuery("active_shards.timestamp").lte("now-$documentAge/$unitTime"))
            .execute(
                object : ActionListener<BulkByScrollResponse> {
                    override fun onResponse(response: BulkByScrollResponse) {
                    }

                    override fun onFailure(t: Exception) {
                    }
                }
            )
        log.info("deleted active shards from $documentAge/$unitTime ago")

        DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
            .source(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
            .filter(QueryBuilders.rangeQuery("relocating_shards.timestamp").lte("now-$documentAge/$unitTime"))
            .execute(
                object : ActionListener<BulkByScrollResponse> {
                    override fun onResponse(response: BulkByScrollResponse) {
                    }

                    override fun onFailure(t: Exception) {
                    }
                }
            )
        log.info("deleted number of relocating shards from $documentAge/$unitTime ago")
    }
}
