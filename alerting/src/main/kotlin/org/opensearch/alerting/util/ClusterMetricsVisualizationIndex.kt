package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings

private val log = LogManager.getLogger(ClusterMetricsVisualizationIndex::class.java)

class ClusterMetricsVisualizationIndex(private val client: Client, private val clusterService: ClusterService) {
    /** The index name pattern for all cluster metric visualizations indices */
    val CLUSTER_METRIC_VISUALIZATION_INDEX = ".opendistro-alerting-cluster-metrics"

    companion object {
        @JvmStatic
        fun clusterMetricsVisualizationsMappings(): String {
            return ClusterMetricsVisualizationIndex::class.java.classLoader.getResource("mappings/metrics-visualizations.json").readText()
        }
    }

    init {
        if (!clusterMetricsVisualizationIndexExists()) {
            val indexRequest = CreateIndexRequest(CLUSTER_METRIC_VISUALIZATION_INDEX)
                .mapping(clusterMetricsVisualizationsMappings())
                .settings(
                    Settings.builder().put("index.hidden", true)
                        .build()
                )
            val createIndexResponse: CreateIndexResponse = client.admin().indices().create(indexRequest).get()
            createIndexResponse.isAcknowledged
        }
    }
    // potentially want to use suspendUntil, tomorrow do similar to ClusterMetricsInput.kt class

    fun clusterMetricsVisualizationIndexExists(): Boolean {
        val clusterState = clusterService.state()
        return clusterState.routingTable.hasIndex(CLUSTER_METRIC_VISUALIZATION_INDEX)
    }
}
