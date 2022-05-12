/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.admin.indices.get.GetIndexRequest
import org.opensearch.action.admin.indices.get.GetIndexResponse
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.alerting.core.model.DocLevelMonitorInput
import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue

private val log = LogManager.getLogger(DocLevelMonitorQueries::class.java)

class DocLevelMonitorQueries(private val client: Client, private val clusterService: ClusterService) {
    companion object {
        @JvmStatic
        fun docLevelQueriesMappings(): String {
            return DocLevelMonitorQueries::class.java.classLoader.getResource("mappings/doc-level-queries.json").readText()
        }
    }

    suspend fun initDocLevelQueryIndex(): Boolean {
        if (!docLevelQueryIndexExists()) {
            val indexRequest = CreateIndexRequest(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)
                .mapping(docLevelQueriesMappings())
                .settings(
                    Settings.builder().put("index.hidden", true)
                        .build()
                )
            return try {
                val createIndexResponse: CreateIndexResponse = client.suspendUntil { client.admin().indices().create(indexRequest, it) }
                createIndexResponse.isAcknowledged
            } catch (t: ResourceAlreadyExistsException) {
                if (t.message?.contains("already exists") == true) {
                    true
                } else {
                    throw t
                }
            }
        }
        return true
    }

    fun docLevelQueryIndexExists(): Boolean {
        val clusterState = clusterService.state()
        return clusterState.routingTable.hasIndex(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)
    }

    suspend fun indexDocLevelQueries(
        monitor: Monitor,
        monitorId: String,
        refreshPolicy: RefreshPolicy = RefreshPolicy.IMMEDIATE,
        indexTimeout: TimeValue
    ) {
        val docLevelMonitorInput = monitor.inputs[0] as DocLevelMonitorInput
        val index = docLevelMonitorInput.indices[0]
        val queries: List<DocLevelQuery> = docLevelMonitorInput.queries

        val clusterState = clusterService.state()

        val getIndexRequest = GetIndexRequest().indices(index)
        val getIndexResponse: GetIndexResponse = client.suspendUntil {
            client.admin().indices().getIndex(getIndexRequest, it)
        }
        val indices = getIndexResponse.indices()

        indices?.forEach { indexName ->
            if (clusterState.routingTable.hasIndex(indexName)) {
                val indexMetadata = clusterState.metadata.index(indexName)
                if (indexMetadata.mapping()?.sourceAsMap?.get("properties") != null) {
                    val properties = (
                        (indexMetadata.mapping()?.sourceAsMap?.get("properties"))
                            as Map<String, Map<String, Any>>
                        )

                    val updatedProperties = properties.entries.associate {
                        if (it.value.containsKey("path")) {
                            val newVal = it.value.toMutableMap()
                            newVal["path"] = "${it.value["path"]}_${indexName}_$monitorId"
                            "${it.key}_${indexName}_$monitorId" to newVal
                        } else {
                            "${it.key}_${indexName}_$monitorId" to it.value
                        }
                    }

                    val updateMappingRequest = PutMappingRequest(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)
                    updateMappingRequest.source(mapOf<String, Any>("properties" to updatedProperties))
                    val updateMappingResponse: AcknowledgedResponse = client.suspendUntil {
                        client.admin().indices().putMapping(updateMappingRequest, it)
                    }

                    if (updateMappingResponse.isAcknowledged) {
                        val indexRequests = mutableListOf<IndexRequest>()
                        queries.forEach {
                            var query = it.query
                            properties.forEach { prop ->
                                query = query.replace("${prop.key}:", "${prop.key}_${indexName}_$monitorId:")
                            }
                            val indexRequest = IndexRequest(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)
                                .id(it.id + "_${indexName}_$monitorId")
                                .source(
                                    mapOf(
                                        "query" to mapOf("query_string" to mapOf("query" to query)),
                                        "monitor_id" to monitorId,
                                        "index" to indexName
                                    )
                                )
                            indexRequests.add(indexRequest)
                        }
                        if (indexRequests.isNotEmpty()) {
                            val bulkResponse: BulkResponse = client.suspendUntil {
                                client.bulk(
                                    BulkRequest().setRefreshPolicy(refreshPolicy).timeout(indexTimeout).add(indexRequests), it
                                )
                            }
                        }
                    }
                }
            }
        }
    }
}
