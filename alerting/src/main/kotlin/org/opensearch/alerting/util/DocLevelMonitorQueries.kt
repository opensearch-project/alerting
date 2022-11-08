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
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob

private val log = LogManager.getLogger(DocLevelMonitorQueries::class.java)

class DocLevelMonitorQueries(private val client: Client, private val clusterService: ClusterService) {
    companion object {

        val PROPERTIES = "properties"
        val NESTED = "nested"
        val TYPE = "type"

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

    /**
     * From given index mapping node, extracts fieldName -> fieldProperties pair
     */
    fun extractField(
        node: MutableMap<String, Any>,
        parent: MutableMap<String, Any>?,
        currentPath: String
    ): Pair<String, MutableMap<String, Any>> {
        if (node.containsKey(PROPERTIES)) {
            return extractField(node.get(PROPERTIES) as MutableMap<String, Any>, null, currentPath)
        } else if (node.containsKey(NESTED)) {
            return extractField(node.get(NESTED) as MutableMap<String, Any>, null, currentPath)
        } else if (node.containsKey(TYPE) == false) {
            val iter = node.iterator().next()
            return extractField(iter.value as MutableMap<String, Any>, node, currentPath + "." + iter.key)
        } else {
            return Pair(currentPath, node)
        }
    }

    fun traverseMappingsAndUpdate(
        inNode: MutableMap<String, Any>,
        currentPath: String,
        processNodeFn: (String, MutableMap<String, Any>) -> Triple<String, String, MutableMap<String, Any>>
    ) {
        if (inNode.containsKey(PROPERTIES)) {
            return traverseMappingsAndUpdate(inNode.get(PROPERTIES) as MutableMap<String, Any>, currentPath, processNodeFn)
        } else if (inNode.containsKey(TYPE) && inNode[TYPE] == NESTED) {
            return traverseMappingsAndUpdate(inNode.get(NESTED) as MutableMap<String, Any>, currentPath, processNodeFn)
        } else if (inNode.containsKey(TYPE) == false) {
            var newNodes = ArrayList<Triple<String, String, Any>>(inNode.size)
            inNode.entries.forEach {
                val nodeProps = it.value as MutableMap<String, Any>
                if (nodeProps.containsKey(TYPE) && nodeProps[TYPE] != NESTED) {
                    val (oldName, newName, props) = processNodeFn(it.key, it.value as MutableMap<String, Any>)
                    newNodes.add(Triple(oldName, newName, props))
                } else {
                    traverseMappingsAndUpdate(nodeProps[PROPERTIES] as MutableMap<String, Any>, it.key, processNodeFn)
                }
            }
            newNodes.forEach {
                if (it.first != it.second) {
                    inNode.remove(it.first)
                }
                inNode.put(it.second, it.third)
            }
        }
    }

    fun processNode(
        fieldName: String,
        props: MutableMap<String, Any>,
    ): Triple<String, String, MutableMap<String, Any>> {
        val newProps = LinkedHashMap<String, Any>(props)
        var newFieldName = fieldName
/*
        if (monitor.dataSources.queryIndexMappingsByType.isNotEmpty()) {
            val mappingsByType = monitor.dataSources.queryIndexMappingsByType
            if (it.value.containsKey("type") && mappingsByType.containsKey(it.value["type"]!!)) {
                mappingsByType[it.value["type"]]?.entries?.forEach { iter: Map.Entry<String, String> ->
                    newProps[iter.key] = iter.value
                }
            }
        }

        if (fieldProps.containsKey("path")) newProps["path"] = "${fieldProps["path"]}_${indexName}_$monitorId"
        "${fieldName}_${indexName}_$monitorId" to newProps
*/
        return Triple(fieldName, newFieldName, newProps)
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
                            as MutableMap<String, Any>
                        )

                    val nodeProcessor =
                        fun(fieldName: String, props: MutableMap<String, Any>): Triple<String, String, MutableMap<String, Any>> {
                            var newFieldName = fieldName
                            val newProps = props.toMutableMap()
                            if (monitor.dataSources.queryIndexMappingsByType.isNotEmpty()) {
                                val mappingsByType = monitor.dataSources.queryIndexMappingsByType
                                if (props.containsKey("type") && mappingsByType.containsKey(props["type"]!!)) {
                                    mappingsByType[props["type"]]?.entries?.forEach { iter: Map.Entry<String, String> ->
                                        newProps[iter.key] = iter.value
                                    }
                                }
                            }
                            if (props.containsKey("path")) {
                                newProps["path"] = "${props["path"]}_${indexName}_$monitorId"
                            }
                            return Triple(fieldName, "${fieldName}_${indexName}_$monitorId", newProps)
                        }
                    traverseMappingsAndUpdate(properties, "", nodeProcessor)

                    val updatedProperties = properties

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
