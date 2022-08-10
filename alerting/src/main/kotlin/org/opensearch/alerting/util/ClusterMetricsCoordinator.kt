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
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.alerting.model.ClusterMetricsDataPoint
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings.Companion.METRICS_EXECUTION_FREQUENCY
import org.opensearch.alerting.settings.AlertingSettings.Companion.METRICS_STORE_TIME
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.component.LifecycleListener
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
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
        val scheduledJobCollection = Runnable {
            launch {
                createDocs(client as NodeClient, clusterService)
                deleteDocs(client)
            }
        }
        if (isCollectionUpdated || isDeletionUpdated) {
            dataPointCollectionDeletionJob?.cancel()
            log.info("Cancelled data collection and deletion jobs")
            isRunningFlag = false
            log.info("detected changes to settings, resetting running, deletion and collection flags to false")
            isDeletionUpdated = false
            isCollectionUpdated = false
        }
        if (event!!.localNodeMaster() && !isRunningFlag) {
            log.info("cluster changed metricsExecutionFrequency = $metricsExecutionFrequency")
            dataPointCollectionDeletionJob = threadPool.scheduleWithFixedDelay(
                scheduledJobCollection,
                metricsExecutionFrequency,
                ThreadPool.Names.SYSTEM_WRITE
            )
            isRunningFlag = true
        }
    }

    private suspend fun createDocs(client: NodeClient, clusterService: ClusterService) {
        val currentTime = Instant.now().toString()
        log.info("This is the current time: $currentTime")
        val clusterHealth = client.admin().cluster().health(ClusterHealthRequest()).get().toMap()
        val nodeStats = client.admin().cluster().nodesStats(NodesStatsRequest().addMetrics("process", "jvm")).get().toMap()

        ClusterMetricsVisualizationIndex.initFunc(client, clusterService)

        val unassignedShards = clusterHealth["unassigned_shards"].toString()
        log.info("This is unassigned shards value: $unassignedShards")
        val clusterStatus = clusterHealth["status"].toString()
        log.info("This is cluster status value: $clusterStatus")
        val numPending = clusterHealth["number_of_pending_tasks"].toString()
        log.info("This is the number of pending tasks: $numPending")
        val activeShards = clusterHealth["active_shards"].toString()
        log.info("This is active shards $activeShards")
        val relocatingShards = clusterHealth["relocating_shards"].toString()
        log.info("This is relocating shards $relocatingShards")
        val numNodes = clusterHealth["number_of_nodes"].toString()
        log.info("This is number of nodes $numNodes")
        val numDataNodes = clusterHealth["number_of_data_nodes"].toString()
        log.info("this is number of data nodes $numDataNodes")

        val nodesMap = nodeStats["nodes"] as Map<String, Any>
        val keys = nodesMap.keys
        val jvmData = arrayListOf<Int>()
        val cpuData = arrayListOf<Int>()

        for (key in keys) {
            val keyData = nodesMap[key] as Map<String, Any>
            val processMap = keyData["process"] as Map<String, Any>
            val cpuMap = processMap["cpu"] as Map<String, Any>
            val percent = cpuMap["percent"]
            cpuData.add(percent as Int)

            val jvmMap = keyData["jvm"] as Map<String, Any>
            val memMap = jvmMap["mem"] as Map<String, Any>
            val pressure = memMap["heap_used_percent"]
            jvmData.add(pressure as Int)
        }

        val minimumCPU = Collections.min(cpuData).toString()
        val maximumCPU = Collections.max(cpuData).toString()
        log.info("This is minimum CPU Usage, $minimumCPU")
        log.info("This is maximum CPU usage, $maximumCPU")

        val minimumJVM = Collections.min(jvmData).toString()
        val maximumJVM = Collections.max(jvmData).toString()
        log.info("This is minimum JVM, $minimumJVM")
        log.info("This is maximum JVM, $maximumJVM")

        var avgCPUcalc = 0.0
        var avgJVMcalc = 0.0

        for (i in cpuData.indices) {
            avgCPUcalc += cpuData[i]
            avgJVMcalc += jvmData[i]
        }

        avgCPUcalc /= cpuData.size
        avgJVMcalc /= jvmData.size

        val avgCPU = avgCPUcalc.toString()
        val avgJVM = avgJVMcalc.toString()
        log.info("This is average CPU, $avgCPU")
        log.info("This is average JVM, $avgJVM")

        val dataPoints = arrayListOf<ClusterMetricsDataPoint>(
            ClusterMetricsDataPoint(
                ClusterMetricsDataPoint.MetricType.CLUSTER_STATUS,
                currentTime,
                clusterStatus
            ),
            ClusterMetricsDataPoint(
                ClusterMetricsDataPoint.MetricType.UNASSIGNED_SHARDS,
                currentTime,
                unassignedShards
            ),
            ClusterMetricsDataPoint(
                ClusterMetricsDataPoint.MetricType.CPU_USAGE,
                currentTime,
                avgCPU,
                minimumCPU,
                maximumCPU
            ),
            ClusterMetricsDataPoint(
                ClusterMetricsDataPoint.MetricType.JVM_PRESSURE,
                currentTime,
                avgJVM,
                minimumJVM,
                maximumJVM
            ),
            ClusterMetricsDataPoint(
                ClusterMetricsDataPoint.MetricType.NUMBER_OF_PENDING_TASKS,
                currentTime,
                numPending
            ),
            ClusterMetricsDataPoint(
                ClusterMetricsDataPoint.MetricType.ACTIVE_SHARDS,
                currentTime,
                activeShards
            ),
            ClusterMetricsDataPoint(
                ClusterMetricsDataPoint.MetricType.RELOCATING_SHARDS,
                currentTime,
                relocatingShards
            ),
            ClusterMetricsDataPoint(
                ClusterMetricsDataPoint.MetricType.NUMBER_OF_NODES,
                currentTime,
                numNodes
            ),
            ClusterMetricsDataPoint(
                ClusterMetricsDataPoint.MetricType.NUMBER_OF_DATA_NODES,
                currentTime,
                numDataNodes
            )
        )

//        val clusterstatusData = ClusterMetricsDataPoint(
//            ClusterMetricsDataPoint.MetricType.CLUSTER_STATUS,
//            currentTime,
//            clusterStatus
//        )
//        val unassignedShardsData = ClusterMetricsDataPoint(
//            ClusterMetricsDataPoint.MetricType.UNASSIGNED_SHARDS,
//            currentTime,
//            unassignedShards
//        )
//        val cpuUsageData = ClusterMetricsDataPoint(
//            ClusterMetricsDataPoint.MetricType.CPU_USAGE,
//            currentTime,
//            avgCPU,
//            minimumCPU,
//            maximumCPU
//        )
//        val jvmDataPoint = ClusterMetricsDataPoint(
//            ClusterMetricsDataPoint.MetricType.JVM_PRESSURE,
//            currentTime,
//            avgJVM,
//            minimumJVM,
//            maximumJVM
//        )
//        val pendingTasksData = ClusterMetricsDataPoint(
//            ClusterMetricsDataPoint.MetricType.NUMBER_OF_PENDING_TASKS,
//            currentTime,
//            numPending
//        )
//        val activeShardsData = ClusterMetricsDataPoint(
//            ClusterMetricsDataPoint.MetricType.ACTIVE_SHARDS,
//            currentTime,
//            activeShards
//        )
//        val relocatingShardsData = ClusterMetricsDataPoint(
//            ClusterMetricsDataPoint.MetricType.RELOCATING_SHARDS,
//            currentTime,
//            relocatingShards
//        )
//        val nodesData = ClusterMetricsDataPoint(
//            ClusterMetricsDataPoint.MetricType.NUMBER_OF_NODES,
//            currentTime,
//            numNodes
//        )
//        val dataNodesData = ClusterMetricsDataPoint(
//            ClusterMetricsDataPoint.MetricType.NUMBER_OF_DATA_NODES,
//            currentTime,
//            numDataNodes
//        )

        dataPoints.forEach { clusterMetricsDataPoint ->
            try {
                val request = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
                    .source(
                        clusterMetricsDataPoint.toXContent(
                            XContentFactory.jsonBuilder(),
                            ToXContent.MapParams(mapOf("with_type" to "true"))
                        )
                    )
                val indexResponse: IndexResponse = client.suspendUntil { client.index(request, it) }
                val failureReasons = checkShardsFailure(indexResponse)
                if (failureReasons != null) {
                    log.info("failed because $failureReasons")
                }
            } catch (t: Exception) {
                log.info("Unable to get index response,  $t")
            }
        }
//        val indexrequestStatus = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
//            .source(clusterstatusData.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
//        val indexrequestShards = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
//            .source(unassignedShardsData.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
//        val indexrequestCpu = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
//            .source(cpuUsageData.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
//        val indexrequestJvm = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
//            .source(jvmDataPoint.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
//        val indexrequestPending = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
//            .source(pendingTasksData.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
//        val indexrequestActive = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
//            .source(activeShardsData.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
//        val indexrequestRelocating = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
//            .source(relocatingShardsData.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
//        val indexrequestNodes = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
//            .source(nodesData.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
//        val indexrequestDatanodes = IndexRequest(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
//            .source(dataNodesData.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
//
//        try {
//            val indexResponse: IndexResponse = client.suspendUntil { client.index(indexrequestStatus, it) }
//            val indexResponse2: IndexResponse = client.suspendUntil { client.index(indexrequestShards, it) }
//            val indexResponse3: IndexResponse = client.suspendUntil { client.index(indexrequestCpu, it) }
//            val indexResponse4: IndexResponse = client.suspendUntil { client.index(indexrequestJvm, it) }
//            val indexResponse5: IndexResponse = client.suspendUntil { client.index(indexrequestPending, it) }
//            val indexResponse6: IndexResponse = client.suspendUntil { client.index(indexrequestActive, it) }
//            val indexResponse7: IndexResponse = client.suspendUntil { client.index(indexrequestRelocating, it) }
//            val indexResponse8: IndexResponse = client.suspendUntil { client.index(indexrequestNodes, it) }
//            val indexResponse9: IndexResponse = client.suspendUntil { client.index(indexrequestDatanodes, it) }
//            val failureReasons = checkShardsFailure(indexResponse)
//            val failureReasons2 = checkShardsFailure(indexResponse2)
//            val failureReasons3 = checkShardsFailure(indexResponse3)
//            val failureReasons4 = checkShardsFailure(indexResponse4)
//            val failureReasons5 = checkShardsFailure(indexResponse5)
//            val failureReasons6 = checkShardsFailure(indexResponse6)
//            val failureReasons7 = checkShardsFailure(indexResponse7)
//            val failureReasons8 = checkShardsFailure(indexResponse8)
//            val failureReasons9 = checkShardsFailure(indexResponse9)
//            if (
//                failureReasons != null ||
//                failureReasons2 != null ||
//                failureReasons3 != null ||
//                failureReasons4 != null ||
//                failureReasons5 != null ||
//                failureReasons6 != null ||
//                failureReasons7 != null ||
//                failureReasons8 != null ||
//                failureReasons9 != null
//            ) {
//                log.info("failed because $failureReasons")
//                log.info("failed because $failureReasons2")
//                log.info("failed because $failureReasons3")
//                log.info("failed because $failureReasons4")
//                log.info("failed because $failureReasons5")
//                log.info("failed because $failureReasons6")
//                log.info("failed because $failureReasons7")
//                log.info("failed because $failureReasons8")
//                log.info("failed because $failureReasons9")
//                return
//            }
//        } catch (t: Exception) {
//            log.info("Unable to get index response,  $t")
//        }
    }
    private fun checkShardsFailure(response: IndexResponse): String? {
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

    private fun deleteDocs(client: NodeClient) {
        val documentAge = metricsStoreTime.toString()

        ClusterMetricsDataPoint.MetricType.values().forEach {
            DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                .source(ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
                .filter(QueryBuilders.rangeQuery(it.metricName + ".timestamp").lte("now-$documentAge"))
                .execute(
                    object : ActionListener<BulkByScrollResponse> {
                        override fun onResponse(response: BulkByScrollResponse) {
                        }

                        override fun onFailure(t: Exception) {
                        }
                    }
                )
            log.info("deleted ${it.metricName} data from $documentAge ago")
        }
    }
}
