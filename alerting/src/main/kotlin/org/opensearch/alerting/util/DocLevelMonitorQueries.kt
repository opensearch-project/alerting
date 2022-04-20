/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.alerting.action.ExecuteMonitorResponse
import org.opensearch.alerting.action.IndexMonitorResponse
import org.opensearch.alerting.core.model.DocLevelMonitorInput
import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.model.Monitor
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue

private val log = LogManager.getLogger(DocLevelMonitorQueries::class.java)

class DocLevelMonitorQueries(private val client: AdminClient, private val clusterService: ClusterService) {
    companion object {
        @JvmStatic
        fun docLevelQueriesMappings(): String {
            return DocLevelMonitorQueries::class.java.classLoader.getResource("mappings/doc-level-queries.json").readText()
        }
    }

    fun initDocLevelQueryIndex(actionListener: ActionListener<CreateIndexResponse>) {
        if (!docLevelQueryIndexExists()) {
            var indexRequest = CreateIndexRequest(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)
                .mapping(docLevelQueriesMappings())
                .settings(
                    Settings.builder().put("index.hidden", true)
                        .build()
                )
            client.indices().create(indexRequest, actionListener)
        }
    }

    fun docLevelQueryIndexExists(): Boolean {
        val clusterState = clusterService.state()
        return clusterState.routingTable.hasIndex(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)
    }

    fun indexDocLevelQueries(
        queryClient: Client,
        monitor: Monitor,
        monitorId: String,
        refreshPolicy: RefreshPolicy,
        indexTimeout: TimeValue,
        indexMonitorActionListener: ActionListener<IndexMonitorResponse>?,
        executeMonitorActionListener: ActionListener<ExecuteMonitorResponse>?,
        docLevelQueryIndexListener: ActionListener<BulkResponse>
    ) {
        val docLevelMonitorInput = monitor.inputs[0] as DocLevelMonitorInput
        val index = docLevelMonitorInput.indices[0]
        val queries: List<DocLevelQuery> = docLevelMonitorInput.queries

        val clusterState = clusterService.state()

        val getAliasesRequest = GetAliasesRequest(index)
        queryClient.admin().indices().getAliases(
            getAliasesRequest,
            object : ActionListener<GetAliasesResponse> {
                override fun onResponse(getAliasesResponse: GetAliasesResponse?) {
                    val aliasIndices = getAliasesResponse?.aliases?.keys()?.map { it.value }
                    val isAlias = aliasIndices != null && aliasIndices.isNotEmpty()
                    val indices = if (isAlias) getAliasesResponse?.aliases?.keys()?.map { it.value } else listOf(index)
                    val indexRequests = mutableListOf<IndexRequest>()
                    log.info("indices: $indices")
                    indices?.forEach { indexName ->
                        if (clusterState.routingTable.hasIndex(indexName)) {
                            val indexMetadata = clusterState.metadata.index(indexName)

                            if (indexMetadata.mapping() != null) {
                                log.info("Index name: $indexName")
                                val properties = (
                                    (indexMetadata.mapping()?.sourceAsMap?.get("properties"))
                                        as Map<String, Map<String, Any>>
                                    )
                                val updatedProperties = properties.entries.associate { "${it.key}_$monitorId" to it.value }.toMutableMap()

                                val updateMappingRequest = PutMappingRequest(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)
                                updateMappingRequest.source(mapOf<String, Any>("properties" to updatedProperties))

                                queryClient.admin().indices().putMapping(
                                    updateMappingRequest,
                                    object : ActionListener<AcknowledgedResponse> {
                                        override fun onResponse(response: AcknowledgedResponse) {

                                            queries.forEach {
                                                var query = it.query

                                                properties.forEach { prop ->
                                                    query = query.replace("${prop.key}:", "${prop.key}_$monitorId:")
                                                }
                                                val indexRequest = IndexRequest(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)
                                                    .id(it.id + "_$monitorId")
                                                    .source(
                                                        mapOf(
                                                            "query" to mapOf("query_string" to mapOf("query" to query)),
                                                            "monitor_id" to monitorId
                                                        )
                                                    )
                                                indexRequests.add(indexRequest)
                                            }
                                            if (indexRequests.isNotEmpty()) {
                                                queryClient.bulk(
                                                    BulkRequest().setRefreshPolicy(refreshPolicy).timeout(indexTimeout).add(indexRequests),
                                                    docLevelQueryIndexListener
                                                )
                                            }
                                            return
                                        }

                                        override fun onFailure(e: Exception) {
                                            log.error("This is a failure", e)
                                            if (indexMonitorActionListener != null) {
                                                indexMonitorActionListener.onFailure(AlertingException.wrap(e))
                                            } else executeMonitorActionListener?.onFailure(AlertingException.wrap(e))
                                            return
                                        }
                                    }
                                )
                            }
                        }
                    }
                }

                override fun onFailure(e: Exception) {
                    if (indexMonitorActionListener != null) {
                        indexMonitorActionListener.onFailure(AlertingException.wrap(e))
                    } else executeMonitorActionListener?.onFailure(AlertingException.wrap(e))
                }
            }
        )
    }
}
