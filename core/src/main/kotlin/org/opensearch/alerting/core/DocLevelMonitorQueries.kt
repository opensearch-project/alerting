package org.opensearch.alerting.core

import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.client.AdminClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings

class DocLevelMonitorQueries(private val client: AdminClient, private val clusterService: ClusterService) {
    companion object {
        @JvmStatic
        fun docLevelQueriesMappings(): String {
            return DocLevelMonitorQueries::class.java.classLoader.getResource("mappings/doc-level-queries.json").readText()
        }
    }

    fun initDocLevelQueryIndex() {
        if (!docLevelQueryIndexExists()) {
            var indexRequest = CreateIndexRequest(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)
                .mapping(docLevelQueriesMappings())
                .settings(
                    Settings.builder().put("index.hidden", true)
                        .build()
                )
            client.indices().create(indexRequest).actionGet()
        }
    }

    fun docLevelQueryIndexExists(): Boolean {
        val clusterState = clusterService.state()
        return clusterState.routingTable.hasIndex(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)
    }
}
