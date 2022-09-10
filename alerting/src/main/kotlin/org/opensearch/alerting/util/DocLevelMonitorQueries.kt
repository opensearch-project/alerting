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
import org.opensearch.alerting.model.DataSources
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
    suspend fun initDocLevelQueryIndex(dataSources: DataSources): Boolean {
        if (dataSources.queryIndex == ScheduledJob.DOC_LEVEL_QUERIES_INDEX) {
            return initDocLevelQueryIndex()
        }
        val queryIndex = dataSources.queryIndex
        if (!clusterService.state().routingTable.hasIndex(queryIndex)) {
            val indexRequest = CreateIndexRequest(queryIndex)
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

    fun docLevelQueryIndexExists(dataSources: DataSources): Boolean {
        val clusterState = clusterService.state()
        return clusterState.routingTable.hasIndex(dataSources.queryIndex)
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
                        val newVal = it.value.toMutableMap()
                        if (monitor.dataSources.queryIndexMappingsByType.isNotEmpty()) {
                            val mappingsByType = monitor.dataSources.queryIndexMappingsByType
                            if (it.value.containsKey("type") && mappingsByType.containsKey(it.value["type"]!!)) {
                                mappingsByType[it.value["type"]]?.entries?.forEach { iter: Map.Entry<String, String> ->
                                    newVal[iter.key] = iter.value
                                }
                            }
                        }
                        if (it.value.containsKey("path")) newVal["path"] = "${it.value["path"]}_${indexName}_$monitorId"
                        "${it.key}_${indexName}_$monitorId" to newVal
                    }
                    val queryIndex = monitor.dataSources.queryIndex

                    val updateMappingRequest = PutMappingRequest(queryIndex)
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
                            val indexRequest = IndexRequest(queryIndex)
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
