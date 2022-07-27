/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.threadpool.ThreadPool

private val log = LogManager.getLogger(ClusterMetricsVisualizationIndex::class.java)

class ClusterMetricsVisualizationIndex(
    private val client: Client,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool
) {

    companion object {
        /** The index name pattern for all cluster metric visualizations indices */
        val CLUSTER_METRIC_VISUALIZATION_INDEX = ".opendistro-alerting-cluster-metrics"

        @JvmStatic
        fun clusterMetricsVisualizationsMappings(): String {
            return ClusterMetricsVisualizationIndex::class.java.classLoader.getResource("mappings/metrics-visualizations.json").readText()
        }
        suspend fun initFunc(client: Client, clusterService: ClusterService) {
            if (!clusterMetricsVisualizationIndexExists(clusterService)) {
                val indexRequest = CreateIndexRequest(CLUSTER_METRIC_VISUALIZATION_INDEX)
                    .mapping(clusterMetricsVisualizationsMappings())
                    .settings(
                        Settings.builder().put("index.hidden", true)
                            .build()
                    )
                try {
                    val createIndexResponse: CreateIndexResponse = client.suspendUntil { client.admin().indices().create(indexRequest, it) }
                    createIndexResponse.isAcknowledged
                } catch (e: ResourceAlreadyExistsException) {
                    log.info("Index already exists.")
                    true
                }
            }
        }
        fun clusterMetricsVisualizationIndexExists(clusterService: ClusterService): Boolean {
            val clusterState = clusterService.state()
            return clusterState.routingTable.hasIndex(CLUSTER_METRIC_VISUALIZATION_INDEX)
        }
    }
}
