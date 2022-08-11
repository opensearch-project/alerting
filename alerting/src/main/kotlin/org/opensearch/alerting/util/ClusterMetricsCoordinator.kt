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

        val unassignedShards = clusterHealth[ClusterMetricsDataPoint.MetricType.UNASSIGNED_SHARDS.metricName].toString()
        log.info("This is unassigned shards value: $unassignedShards")
        val clusterStatus = clusterHealth["status"].toString()
        log.info("This is cluster status value: $clusterStatus")
        val numPending = clusterHealth[ClusterMetricsDataPoint.MetricType.NUMBER_OF_PENDING_TASKS.metricName].toString()
        log.info("This is the number of pending tasks: $numPending")
        val activeShards = clusterHealth[ClusterMetricsDataPoint.MetricType.ACTIVE_SHARDS.metricName].toString()
        log.info("This is active shards $activeShards")
        val relocatingShards = clusterHealth[ClusterMetricsDataPoint.MetricType.RELOCATING_SHARDS.metricName].toString()
        log.info("This is relocating shards $relocatingShards")
        val numNodes = clusterHealth[ClusterMetricsDataPoint.MetricType.NUMBER_OF_NODES.metricName].toString()
        log.info("This is number of nodes $numNodes")
        val numDataNodes = clusterHealth[ClusterMetricsDataPoint.MetricType.NUMBER_OF_DATA_NODES.metricName].toString()
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
                    log.error("Failed to index ${clusterMetricsDataPoint.metric}.", failureReasons)
                }
            } catch (t: Exception) {
                log.error("Failed to index ${clusterMetricsDataPoint.metric}.", t)
            }
        }
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
            log.info("Deleted ${it.metricName} data from $documentAge ago.")
        }
    }
}
