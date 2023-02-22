/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchStatusException
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.admin.indices.alias.Alias
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.admin.indices.rollover.RolloverRequest
import org.opensearch.action.admin.indices.rollover.RolloverResponse
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.alerting.MonitorRunnerService.monitorCtx
import org.opensearch.alerting.model.MonitorMetadata
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.index.mapper.MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING
import org.opensearch.rest.RestStatus

private val log = LogManager.getLogger(DocLevelMonitorQueries::class.java)

class DocLevelMonitorQueries(private val client: Client, private val clusterService: ClusterService) {
    companion object {

        const val PROPERTIES = "properties"
        const val NESTED = "nested"
        const val TYPE = "type"
        const val INDEX_PATTERN_SUFFIX = "-000001"
        const val QUERY_INDEX_BASE_FIELDS_COUNT = 8 // 3 fields we defined and 5 builtin additional metadata fields
        @JvmStatic
        fun docLevelQueriesMappings(): String {
            return DocLevelMonitorQueries::class.java.classLoader.getResource("mappings/doc-level-queries.json").readText()
        }
        fun docLevelQueriesSettings(): Settings {
            return Settings.builder().loadFromSource(
                DocLevelMonitorQueries::class.java.classLoader.getResource("settings/doc-level-queries.json").readText(),
                XContentType.JSON
            ).build()
        }
    }

    suspend fun initDocLevelQueryIndex(): Boolean {
        if (!docLevelQueryIndexExists()) {
            // Since we changed queryIndex to be alias now, for backwards compatibility, we have to delete index with same name
            // as our alias, to avoid name clash.
            if (clusterService.state().metadata.hasIndex(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)) {
                val acknowledgedResponse: AcknowledgedResponse = client.suspendUntil {
                    admin().indices().delete(DeleteIndexRequest(ScheduledJob.DOC_LEVEL_QUERIES_INDEX), it)
                }
                if (!acknowledgedResponse.isAcknowledged) {
                    val errorMessage = "Deletion of old queryIndex [${ScheduledJob.DOC_LEVEL_QUERIES_INDEX}] index is not acknowledged!"
                    log.error(errorMessage)
                    throw AlertingException.wrap(OpenSearchStatusException(errorMessage, RestStatus.INTERNAL_SERVER_ERROR))
                }
            }
            val alias = ScheduledJob.DOC_LEVEL_QUERIES_INDEX
            val indexPattern = ScheduledJob.DOC_LEVEL_QUERIES_INDEX + INDEX_PATTERN_SUFFIX
            val indexRequest = CreateIndexRequest(indexPattern)
                .mapping(docLevelQueriesMappings())
                .alias(Alias(alias))
                .settings(docLevelQueriesSettings())
            return try {
                val createIndexResponse: CreateIndexResponse = client.suspendUntil { client.admin().indices().create(indexRequest, it) }
                createIndexResponse.isAcknowledged
            } catch (t: Exception) {
                if (ExceptionsHelper.unwrapCause(t) is ResourceAlreadyExistsException) {
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
        // Since we changed queryIndex to be alias now, for backwards compatibility, we have to delete index with same name
        // as our alias, to avoid name clash.
        if (clusterService.state().metadata.hasIndex(dataSources.queryIndex)) {
            val acknowledgedResponse: AcknowledgedResponse = client.suspendUntil {
                admin().indices().delete(DeleteIndexRequest(dataSources.queryIndex), it)
            }
            if (!acknowledgedResponse.isAcknowledged) {
                log.warn("Deletion of old queryIndex [${dataSources.queryIndex}] index is not acknowledged!")
            }
        }
        val alias = dataSources.queryIndex
        val indexPattern = dataSources.queryIndex + INDEX_PATTERN_SUFFIX
        if (!clusterService.state().metadata.hasAlias(alias)) {
            val indexRequest = CreateIndexRequest(indexPattern)
                .mapping(docLevelQueriesMappings())
                .alias(Alias(alias))
                .settings(
                    Settings.builder().put("index.hidden", true)
                        .build()
                )
            return try {
                val createIndexResponse: CreateIndexResponse = client.suspendUntil { client.admin().indices().create(indexRequest, it) }
                createIndexResponse.isAcknowledged
            } catch (t: Exception) {
                if (ExceptionsHelper.unwrapCause(t) is ResourceAlreadyExistsException) {
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
        return clusterState.metadata.hasAlias(dataSources.queryIndex)
    }

    fun docLevelQueryIndexExists(): Boolean {
        val clusterState = clusterService.state()
        return clusterState.metadata.hasAlias(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)
    }

    /**
     * Does a DFS traversal of index mappings tree.
     * Calls processLeafFn on every leaf node.
     * Populates flattenPaths list with full paths of leaf nodes
     * @param node current node which we're visiting
     * @param currentPath current node path from root node
     * @param processLeafFn leaf processor function which is called on every leaf discovered
     * @param flattenPaths list of full paths of all leaf nodes relative to root
     */
    fun traverseMappingsAndUpdate(
        node: MutableMap<String, Any>,
        currentPath: String,
        processLeafFn: (String, MutableMap<String, Any>) -> Triple<String, String, MutableMap<String, Any>>,
        flattenPaths: MutableList<String>
    ) {
        // If node contains "properties" property then it is internal(non-leaf) node
        log.debug("Node in traverse: $node")
        if (node.containsKey(PROPERTIES) &&
            (node.get(PROPERTIES) as Map<String, Any>).containsKey(PROPERTIES) == false // Make sure that "properties" isn't interim node
        ) { // Make sure that "properties" isn't interim node
            return traverseMappingsAndUpdate(node.get(PROPERTIES) as MutableMap<String, Any>, currentPath, processLeafFn, flattenPaths)
        } else {
            // newNodes will hold list of updated leaf properties
            var newNodes = ArrayList<Triple<String, String, Any>>(node.size)
            node.entries.forEach {
                // Compute full path relative to root
                val fullPath = if (currentPath.isEmpty()) it.key
                else "$currentPath.${it.key}"
                val nodeProps = it.value as MutableMap<String, Any>
                // If it has type property and type is not "nested" then this is a leaf
                if (nodeProps.containsKey(TYPE) && nodeProps[TYPE] != NESTED) {
                    // At this point we know full path of node, so we add it to output array
                    flattenPaths.add(fullPath)
                    // Calls processLeafFn and gets old node name, new node name and new properties of node.
                    // This is all information we need to update this node
                    val (oldName, newName, props) = processLeafFn(it.key, it.value as MutableMap<String, Any>)
                    newNodes.add(Triple(oldName, newName, props))
                } else {
                    // Internal(non-leaf) node - visit children
                    traverseMappingsAndUpdate(nodeProps[PROPERTIES] as MutableMap<String, Any>, fullPath, processLeafFn, flattenPaths)
                }
            }
            // Here we can update all processed leaves in tree
            newNodes.forEach {
                // If we renamed leaf, we have to remove it first
                if (it.first != it.second) {
                    node.remove(it.first)
                }
                // Put new properties of leaf
                log.info("REWRITTNG ${it.first} to ${it.second} ${it.third}")
                node.put(it.second, it.third)
            }
        }
    }

    suspend fun indexDocLevelQueries(
        monitor: Monitor,
        monitorId: String,
        monitorMetadata: MonitorMetadata,
        refreshPolicy: RefreshPolicy = RefreshPolicy.IMMEDIATE,
        indexTimeout: TimeValue
    ) {
        val docLevelMonitorInput = monitor.inputs[0] as DocLevelMonitorInput
        val queries: List<DocLevelQuery> = docLevelMonitorInput.queries

        val indices = IndexUtils.resolveAllIndices(
            docLevelMonitorInput.indices,
            monitorCtx.clusterService!!,
            monitorCtx.indexNameExpressionResolver!!
        )

        val clusterState = clusterService.state()

        // Run through each backing index and apply appropriate mappings to query index
        indices?.forEach { indexName ->
            if (clusterState.routingTable.hasIndex(indexName)) {
                val indexMetadata = clusterState.metadata.index(indexName)
                if (indexMetadata.mapping()?.sourceAsMap?.get("properties") != null) {
                    val properties = (
                        (indexMetadata.mapping()?.sourceAsMap?.get("properties"))
                            as MutableMap<String, Any>
                        )
                    // Node processor function is used to process leaves of index mappings tree
                    //
                    val leafNodeProcessor =
                        fun(fieldName: String, props: MutableMap<String, Any>): Triple<String, String, MutableMap<String, Any>> {
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
                                newProps["path"] = "${props["path"]}_${IndexUtils.sanitizeDotsInIndexName(indexName)}_$monitorId"
                            }
                            return Triple(fieldName, "${fieldName}_${IndexUtils.sanitizeDotsInIndexName(indexName)}_$monitorId", newProps)
                        }
                    // Traverse and update index mappings here while extracting flatten field paths
                    val flattenPaths = mutableListOf<String>()
                    traverseMappingsAndUpdate(properties, "", leafNodeProcessor, flattenPaths)
                    // Updated mappings ready to be applied on queryIndex
                    val updatedProperties = properties
                    // Updates mappings of concrete queryIndex. This can rollover queryIndex if field mapping limit is reached.
                    var (updateMappingResponse, concreteQueryIndex) = updateQueryIndexMappings(
                        monitor,
                        monitorMetadata,
                        indexName,
                        updatedProperties
                    )

                    if (updateMappingResponse.isAcknowledged) {
                        doIndexAllQueries(concreteQueryIndex, indexName, monitorId, queries, flattenPaths, refreshPolicy, indexTimeout)
                    }
                }
            }
        }
    }

    private suspend fun doIndexAllQueries(
        concreteQueryIndex: String,
        sourceIndex: String,
        monitorId: String,
        queries: List<DocLevelQuery>,
        flattenPaths: MutableList<String>,
        refreshPolicy: RefreshPolicy,
        indexTimeout: TimeValue
    ) {
        val indexRequests = mutableListOf<IndexRequest>()
        queries.forEach {
            var query = it.query
            flattenPaths.forEach { fieldPath ->
                query = query.replace("$fieldPath:", "${fieldPath}_${IndexUtils.sanitizeDotsInIndexName(sourceIndex)}_$monitorId:")
            }
            val indexRequest = IndexRequest(concreteQueryIndex)
                .id(it.id + "_${sourceIndex}_$monitorId")
                .source(
                    mapOf(
                        "query" to mapOf("query_string" to mapOf("query" to query)),
                        "monitor_id" to monitorId,
                        "index" to sourceIndex
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
            bulkResponse.forEach { bulkItemResponse ->
                if (bulkItemResponse.isFailed) {
                    log.info(bulkItemResponse.failureMessage)
                }
            }
        }
    }

    private suspend fun updateQueryIndexMappings(
        monitor: Monitor,
        monitorMetadata: MonitorMetadata,
        sourceIndex: String,
        updatedProperties: MutableMap<String, Any>
    ): Pair<AcknowledgedResponse, String> {
        var targetQueryIndex = monitorMetadata.getMappedQueryIndex(sourceIndex)
        if (targetQueryIndex == null) {
            // queryIndex is alias which will always have only 1 backing index which is writeIndex
            // This is due to a fact that that _rollover API would maintain only single index under alias
            // if you don't add is_write_index setting when creating index initially
            targetQueryIndex = getWriteIndexNameForAlias(monitor.dataSources.queryIndex)
            if (targetQueryIndex == null) {
                val message = "Failed to get write index for queryIndex alias:${monitor.dataSources.queryIndex}"
                log.error(message)
                throw AlertingException.wrap(
                    OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR)
                )
            }
            monitorMetadata.addQueryIndexToMapping(sourceIndex, targetQueryIndex)
        }
        val updateMappingRequest = PutMappingRequest(targetQueryIndex)
        updateMappingRequest.source(mapOf<String, Any>("properties" to updatedProperties))
        var updateMappingResponse = AcknowledgedResponse(false)
        try {
            // Adjust max field limit in mappings for query index, if needed.
            checkAndAdjustMaxFieldLimit(sourceIndex, targetQueryIndex)
            updateMappingResponse = client.suspendUntil {
                client.admin().indices().putMapping(updateMappingRequest, it)
            }
            return Pair(updateMappingResponse, targetQueryIndex)
        } catch (e: Exception) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            log.debug("exception after rollover queryIndex index: $targetQueryIndex exception: ${unwrappedException.message}")
            // If we reached limit for total number of fields in mappings, do a rollover here
            if (unwrappedException.message?.contains("Limit of total fields") == true) {
                try {
                    // Do queryIndex rollover
                    targetQueryIndex = rolloverQueryIndex(monitor)
                    // Adjust max field limit in mappings for new index.
                    checkAndAdjustMaxFieldLimit(sourceIndex, targetQueryIndex)
                    // PUT mappings to newly created index
                    val updateMappingRequest = PutMappingRequest(targetQueryIndex)
                    updateMappingRequest.source(mapOf<String, Any>("properties" to updatedProperties))
                    updateMappingResponse = client.suspendUntil {
                        client.admin().indices().putMapping(updateMappingRequest, it)
                    }
                } catch (e: Exception) {
                    // If we reached limit for total number of fields in mappings after rollover
                    // it means that source index has more then (FIELD_LIMIT - 3) fields (every query index has 3 fields defined)
                    // TODO maybe split queries/mappings between multiple query indices?
                    val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
                    log.debug("exception after rollover queryIndex index: $targetQueryIndex exception: ${unwrappedException.message}")
                    if (unwrappedException.message?.contains("Limit of total fields") == true) {
                        val errorMessage =
                            "Monitor [${monitorMetadata.monitorId}] can't process index [$sourceIndex] due to field mapping limit"
                        log.error(errorMessage)
                        throw AlertingException(errorMessage, RestStatus.INTERNAL_SERVER_ERROR, e)
                    } else {
                        throw AlertingException.wrap(e)
                    }
                }
            } else {
                log.debug("unknown exception during PUT mapping on queryIndex: $targetQueryIndex")
                val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
                throw AlertingException.wrap(unwrappedException)
            }
        }
        // We did rollover, so try to apply mappings again on new targetQueryIndex
        if (targetQueryIndex.isNotEmpty()) {
            // add newly created index to monitor's metadata object so that we can fetch it later on, when either applying mappings or running queries
            monitorMetadata.addQueryIndexToMapping(sourceIndex, targetQueryIndex)
        } else {
            val failureMessage = "Failed to resolve targetQueryIndex!"
            log.error(failureMessage)
            throw AlertingException(failureMessage, RestStatus.INTERNAL_SERVER_ERROR, IllegalStateException(failureMessage))
        }
        return Pair(updateMappingResponse, targetQueryIndex)
    }

    /**
     * Adjusts max field limit index setting for query index if source index has higher limit.
     * This will prevent max field limit exception, when source index has more fields then query index limit
     */
    private suspend fun checkAndAdjustMaxFieldLimit(sourceIndex: String, concreteQueryIndex: String) {
        val getSettingsResponse: GetSettingsResponse = client.suspendUntil {
            admin().indices().getSettings(GetSettingsRequest().indices(sourceIndex, concreteQueryIndex), it)
        }
        val sourceIndexLimit =
            getSettingsResponse.getSetting(sourceIndex, INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.key)?.toLong() ?: 1000L
        val queryIndexLimit =
            getSettingsResponse.getSetting(concreteQueryIndex, INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.key)?.toLong() ?: 1000L
        // Our query index initially has 3 fields we defined and 5 more builtin metadata fields in mappings so we have to account for that
        if (sourceIndexLimit > (queryIndexLimit - QUERY_INDEX_BASE_FIELDS_COUNT)) {
            val updateSettingsResponse: AcknowledgedResponse = client.suspendUntil {
                admin().indices().updateSettings(
                    UpdateSettingsRequest(concreteQueryIndex).settings(
                        Settings.builder().put(
                            INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.key, sourceIndexLimit + QUERY_INDEX_BASE_FIELDS_COUNT
                        )
                    ),
                    it
                )
            }
        }
    }

    private suspend fun rolloverQueryIndex(monitor: Monitor): String {
        val queryIndex = monitor.dataSources.queryIndex
        val queryIndexPattern = monitor.dataSources.queryIndex + INDEX_PATTERN_SUFFIX

        val request = RolloverRequest(queryIndex, null)
        request.createIndexRequest.index(queryIndexPattern)
            .mapping(docLevelQueriesMappings())
            .settings(docLevelQueriesSettings())
        val response: RolloverResponse = client.suspendUntil {
            client.admin().indices().rolloverIndex(request, it)
        }
        if (response.isRolledOver == false) {
            val message = "failed to rollover queryIndex:$queryIndex queryIndexPattern:$queryIndexPattern"
            log.error(message)
            throw AlertingException.wrap(
                OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR)
            )
        }
        return response.newIndex
    }

    private fun getWriteIndexNameForAlias(alias: String): String? {
        return this.clusterService.state().metadata().indicesLookup?.get(alias)?.writeIndex?.index?.name
    }
}
