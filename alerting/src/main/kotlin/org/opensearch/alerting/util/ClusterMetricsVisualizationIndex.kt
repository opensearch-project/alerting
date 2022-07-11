package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.action.IndexMonitorAction
import org.opensearch.alerting.action.IndexMonitorRequest
import org.opensearch.alerting.core.model.CronSchedule
import org.opensearch.alerting.core.model.IntervalSchedule
import org.opensearch.alerting.model.Monitor
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.component.LifecycleListener
import org.opensearch.common.settings.Setting
import org.opensearch.rest.RestRequest
import org.opensearch.threadpool.ThreadPool
import java.time.Instant
import java.time.ZoneId
import java.time.temporal.ChronoUnit

private val log = LogManager.getLogger(ClusterMetricsVisualizationIndex::class.java)

class ClusterMetricsVisualizationIndex(
    private val client: Client,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool
) : ClusterStateListener, LifecycleListener() {
    /** The index name pattern for all cluster metric visualizations indices */
    val CLUSTER_METRIC_VISUALIZATION_INDEX = ".opendistro-alerting-cluster-metrics"
    val METRICS_HISTORY_ENABLED = Setting.boolSetting("opendistro.alerting.metrics_history_enabled", true)

    companion object {
        @JvmStatic
        fun clusterMetricsVisualizationsMappings(): String {
            return ClusterMetricsVisualizationIndex::class.java.classLoader.getResource("mappings/metrics-visualizations.json").readText()
        }
    }

    init {
        clusterService.addListener(this)
        clusterService.addLifecycleListener(this)
    }
    override fun clusterChanged(p0: ClusterChangedEvent) {
        log.info("THIS CLASS IS BEING CALLED")
        // threadPool.schedule({ helper() }, TimeValue.timeValueMinutes(1), ThreadPool.Names.MANAGEMENT)
    }

    suspend fun helper() {
        val cronSchedule = CronSchedule("*/15 * * * *", ZoneId.of("US/Pacific"))
        // index is already being created error, maybe check index is created and THEN run?
        val monitor = Monitor(
            id = "12345",
            version = 1L,
            name = "test_pls_work",
            enabled = true,
            user = null,
            schedule = IntervalSchedule(interval = 15, unit = ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            enabledTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            monitorType = Monitor.MonitorType.CLUSTER_METRICS_MONITOR,
            inputs = mutableListOf(),
            triggers = mutableListOf(),
            uiMetadata = mapOf()
        )
        val monitorRequest = IndexMonitorRequest(
            monitorId = "111111",
            seqNo = 1L,
            primaryTerm = 2L,
            refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE,
            RestRequest.Method.POST,
            monitor
        )
        val response = client.execute(IndexMonitorAction.INSTANCE, monitorRequest).get()
        log.info("YEP $response")
        // response.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS)
    }

//    init {
//        if (!clusterMetricsVisualizationIndexExists()) {
//            val indexRequest = CreateIndexRequest(CLUSTER_METRIC_VISUALIZATION_INDEX)
//                .mapping(clusterMetricsVisualizationsMappings())
//                .settings(
//                    Settings.builder().put("index.hidden", true)
//                        .build()
//                )
//            val createIndexResponse: CreateIndexResponse = client.admin().indices().create(indexRequest).get()
//            createIndexResponse.isAcknowledged
//        }
//        clusterService.addListener(this)
//        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.ALERT_HISTORY_ENABLED) { alertHistoryEnabled = it }
//        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.ALERT_HISTORY_MAX_DOCS) { alertHistoryMaxDocs = it }
//        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.ALERT_HISTORY_INDEX_MAX_AGE) { alertHistoryMaxAge = it }
//        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.ALERT_HISTORY_ROLLOVER_PERIOD) {
//            alertHistoryRolloverPeriod = it
//            rescheduleAlertRollover()
//        }
//    }
//    suspend fun initFunc() {
//        if (!clusterMetricsVisualizationIndexExists()) {
//            val indexRequest = CreateIndexRequest(CLUSTER_METRIC_VISUALIZATION_INDEX)
//                .mapping(clusterMetricsVisualizationsMappings())
//                .settings(
//                    Settings.builder().put("index.hidden", true)
//                        .build()
//                )
//            val createIndexResponse: CreateIndexResponse = client.suspendUntil { client.admin().indices().create(indexRequest, it) }
//            createIndexResponse.isAcknowledged
//        }
//    }
//    fun clusterMetricsVisualizationIndexExists(): Boolean {
//        val clusterState = clusterService.state()
//        return clusterState.routingTable.hasIndex(CLUSTER_METRIC_VISUALIZATION_INDEX)
//    }
}
