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
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse
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
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorMetadata
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.mapper.MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.DeleteByQueryAction
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

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

    suspend fun deleteDocLevelQueriesOnDryRun(monitorMetadata: MonitorMetadata) {
        try {
            monitorMetadata.sourceToQueryIndexMapping.forEach { (_, queryIndex) ->
                val indicesExistsResponse: IndicesExistsResponse =
                    client.suspendUntil {
                        client.admin().indices().exists(IndicesExistsRequest(queryIndex), it)
                    }
                if (indicesExistsResponse.isExists == false) {
                    return
                }

                val queryBuilder = QueryBuilders.boolQuery()
                    .must(QueryBuilders.existsQuery("monitor_id"))
                    .mustNot(QueryBuilders.wildcardQuery("monitor_id", "*"))

                val response: BulkByScrollResponse = suspendCoroutine { cont ->
                    DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                        .source(queryIndex)
                        .filter(queryBuilder)
                        .refresh(true)
                        .execute(
                            object : ActionListener<BulkByScrollResponse> {
                                override fun onResponse(response: BulkByScrollResponse) = cont.resume(response)
                                override fun onFailure(t: Exception) = cont.resumeWithException(t)
                            }
                        )
                }
                response.bulkFailures.forEach {
                    log.error("Failed deleting queries while removing dry run queries: [${it.id}] cause: [${it.cause}] ")
                }
            }
        } catch (e: Exception) {
            log.error("Failed to delete doc level queries on dry run", e)
        }
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
        processLeafFn: (String, String, MutableMap<String, Any>) -> Triple<String, String, MutableMap<String, Any>>,
        flattenPaths: MutableMap<String, MutableMap<String, Any>>
    ) {
        // If node contains "properties" property then it is internal(non-leaf) node
        log.debug("Node in traverse: $node")
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
                flattenPaths.put(fullPath, nodeProps)
                // Calls processLeafFn and gets old node name, new node name and new properties of node.
                // This is all information we need to update this node
                val (oldName, newName, props) = processLeafFn(it.key, fullPath, it.value as MutableMap<String, Any>)
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
            node.put(it.second, it.third)
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

        val indices = docLevelMonitorInput.indices
        val clusterState = clusterService.state()

        // Run through each backing index and apply appropriate mappings to query index
        indices.forEach { indexName ->
            var concreteIndices = IndexUtils.resolveAllIndices(
                listOf(indexName),
                monitorCtx.clusterService!!,
                monitorCtx.indexNameExpressionResolver!!
            )
            if (IndexUtils.isAlias(indexName, monitorCtx.clusterService!!.state()) ||
                IndexUtils.isDataStream(indexName, monitorCtx.clusterService!!.state())
            ) {
                val lastWriteIndex = concreteIndices.find { monitorMetadata.lastRunContext.containsKey(it) }
                if (lastWriteIndex != null) {
                    val lastWriteIndexCreationDate =
                        IndexUtils.getCreationDateForIndex(lastWriteIndex, monitorCtx.clusterService!!.state())
                    concreteIndices = IndexUtils.getNewestIndicesByCreationDate(
                        concreteIndices,
                        monitorCtx.clusterService!!.state(),
                        lastWriteIndexCreationDate
                    )
                }
            }
            val updatedIndexName = indexName.replace("*", "_")
            val updatedProperties = mutableMapOf<String, Any>()
            val allFlattenPaths = mutableSetOf<Pair<String, String>>()
            var sourceIndexFieldLimit = 0L
            val conflictingFields = getAllConflictingFields(clusterState, concreteIndices)

            concreteIndices.forEach { concreteIndexName ->
                if (clusterState.routingTable.hasIndex(concreteIndexName)) {
                    val indexMetadata = clusterState.metadata.index(concreteIndexName)
                    if (indexMetadata.mapping()?.sourceAsMap?.get("properties") != null) {
                        val properties = (
                            (indexMetadata.mapping()?.sourceAsMap?.get("properties"))
                                as MutableMap<String, Any>
                            )
                        // Node processor function is used to process leaves of index mappings tree
                        //
                        val leafNodeProcessor =
                            fun(fieldName: String, fullPath: String, props: MutableMap<String, Any>):
                                Triple<String, String, MutableMap<String, Any>> {
                                val newProps = props.toMutableMap()
                                if (monitor.dataSources.queryIndexMappingsByType.isNotEmpty()) {
                                    val mappingsByType = monitor.dataSources.queryIndexMappingsByType
                                    if (props.containsKey("type") && mappingsByType.containsKey(props["type"]!!)) {
                                        mappingsByType[props["type"]]?.entries?.forEach { iter: Map.Entry<String, String> ->
                                            newProps[iter.key] = iter.value
                                        }
                                    }
                                }

                                return if (conflictingFields.contains(fullPath)) {
                                    if (props.containsKey("path")) {
                                        newProps["path"] = "${props["path"]}_${concreteIndexName}_$monitorId"
                                    }
                                    Triple(fieldName, "${fieldName}_${concreteIndexName}_$monitorId", newProps)
                                } else {
                                    if (props.containsKey("path")) {
                                        newProps["path"] = "${props["path"]}_${updatedIndexName}_$monitorId"
                                    }
                                    Triple(fieldName, "${fieldName}_${updatedIndexName}_$monitorId", newProps)
                                }
                            }
                        // Traverse and update index mappings here while extracting flatten field paths
                        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()
                        traverseMappingsAndUpdate(properties, "", leafNodeProcessor, flattenPaths)
                        flattenPaths.keys.forEach { allFlattenPaths.add(Pair(it, concreteIndexName)) }
                        // Updated mappings ready to be applied on queryIndex
                        properties.forEach {
                            if (
                                it.value is Map<*, *> &&
                                (it.value as Map<String, Any>).containsKey("type") &&
                                (it.value as Map<String, Any>)["type"] == NESTED
                            ) {
                            } else {
                                if (updatedProperties.containsKey(it.key) && updatedProperties[it.key] != it.value) {
                                    val mergedField = mergeConflictingFields(
                                        updatedProperties[it.key] as Map<String, Any>,
                                        it.value as Map<String, Any>
                                    )
                                    updatedProperties[it.key] = mergedField
                                } else {
                                    updatedProperties[it.key] = it.value
                                }
                            }
                        }
                        sourceIndexFieldLimit += checkMaxFieldLimit(concreteIndexName)
                    }
                }
            }
            // Updates mappings of concrete queryIndex. This can rollover queryIndex if field mapping limit is reached.
            val (updateMappingResponse, concreteQueryIndex) = updateQueryIndexMappings(
                monitor,
                monitorMetadata,
                updatedIndexName,
                sourceIndexFieldLimit,
                updatedProperties
            )

            if (updateMappingResponse.isAcknowledged) {
                doIndexAllQueries(
                    concreteQueryIndex,
                    updatedIndexName,
                    monitorId,
                    queries,
                    allFlattenPaths,
                    conflictingFields,
                    refreshPolicy,
                    indexTimeout
                )
            }
        }
    }

    private suspend fun doIndexAllQueries(
        concreteQueryIndex: String,
        sourceIndex: String,
        monitorId: String,
        queries: List<DocLevelQuery>,
        flattenPaths: MutableSet<Pair<String, String>>,
        conflictingPaths: Set<String>,
        refreshPolicy: RefreshPolicy,
        indexTimeout: TimeValue
    ) {
        val indexRequests = mutableListOf<IndexRequest>()
        val conflictingPathToConcreteIndices = mutableMapOf<String, MutableSet<String>>()
        flattenPaths.forEach { fieldPath ->
            if (conflictingPaths.contains(fieldPath.first)) {
                if (conflictingPathToConcreteIndices.containsKey(fieldPath.first)) {
                    val concreteIndexSet = conflictingPathToConcreteIndices[fieldPath.first]
                    concreteIndexSet!!.add(fieldPath.second)
                    conflictingPathToConcreteIndices[fieldPath.first] = concreteIndexSet
                } else {
                    val concreteIndexSet = mutableSetOf<String>()
                    concreteIndexSet.add(fieldPath.second)
                    conflictingPathToConcreteIndices[fieldPath.first] = concreteIndexSet
                }
            }
        }

        val newQueries = mutableListOf<DocLevelQuery>()
        queries.forEach {
            val filteredConcreteIndices = mutableSetOf<String>()
            var query = it.query
            conflictingPaths.forEach { conflictingPath ->
                if (query.contains(conflictingPath)) {
                    query = transformExistsQuery(query, conflictingPath, "<index>", monitorId)
                    query = query.replace("$conflictingPath:", "${conflictingPath}_<index>_$monitorId:")
                    filteredConcreteIndices.addAll(conflictingPathToConcreteIndices[conflictingPath]!!)
                }
            }

            if (filteredConcreteIndices.isNotEmpty()) {
                filteredConcreteIndices.forEach { filteredConcreteIndex ->
                    val newQuery = it.copy(
                        id = "${it.id}_$filteredConcreteIndex",
                        query = query.replace("<index>", filteredConcreteIndex)
                    )
                    newQueries.add(newQuery)
                }
            } else {
                newQueries.add(it.copy(id = "${it.id}_$sourceIndex"))
            }
        }

        newQueries.forEach {
            var query = it.query
            flattenPaths.forEach { fieldPath ->
                if (!conflictingPaths.contains(fieldPath.first)) {
                    query = transformExistsQuery(query, fieldPath.first, sourceIndex, monitorId)
                    query = query.replace("${fieldPath.first}:", "${fieldPath.first}_${sourceIndex}_$monitorId:")
                }
            }
            val indexRequest = IndexRequest(concreteQueryIndex)
                .id(it.id + "_$monitorId")
                .source(
                    mapOf(
                        "query" to mapOf("query_string" to mapOf("query" to query, "fields" to it.fields)),
                        "monitor_id" to monitorId,
                        "index" to sourceIndex
                    )
                )
            indexRequests.add(indexRequest)
        }
        log.debug("bulk inserting percolate [${queries.size}] queries")
        if (indexRequests.isNotEmpty()) {
            val bulkResponse: BulkResponse = client.suspendUntil {
                client.bulk(
                    BulkRequest().setRefreshPolicy(refreshPolicy).timeout(indexTimeout).add(indexRequests),
                    it
                )
            }
            bulkResponse.forEach { bulkItemResponse ->
                if (bulkItemResponse.isFailed) {
                    log.debug(bulkItemResponse.failureMessage)
                }
            }
        }
    }

    /**
     * Transforms the query if it includes an _exists_ clause to append the index name and the monitor id to the field value
     */
    private fun transformExistsQuery(query: String, conflictingPath: String, indexName: String, monitorId: String): String {
        return query
            .replace("_exists_: ", "_exists_:") // remove space to read exists query as one string
            .split("\\s+".toRegex())
            .joinToString(separator = " ") { segment ->
                if (segment.contains("_exists_:")) {
                    val trimSegement = segment.trim { it == '(' || it == ')' } // remove any delimiters from ends
                    val (_, value) = trimSegement.split(":", limit = 2) // split into key and value
                    val newString = if (value == conflictingPath)
                        segment.replace(conflictingPath, "${conflictingPath}_${indexName}_$monitorId") else segment
                    newString
                } else {
                    segment
                }
            }
    }

    private suspend fun updateQueryIndexMappings(
        monitor: Monitor,
        monitorMetadata: MonitorMetadata,
        sourceIndex: String,
        sourceIndexFieldLimit: Long,
        updatedProperties: MutableMap<String, Any>
    ): Pair<AcknowledgedResponse, String> {
        var targetQueryIndex = monitorMetadata.sourceToQueryIndexMapping[sourceIndex + monitor.id]
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
            monitorMetadata.sourceToQueryIndexMapping[sourceIndex + monitor.id] = targetQueryIndex
        }
        val updateMappingRequest = PutMappingRequest(targetQueryIndex)
        updateMappingRequest.source(mapOf<String, Any>("properties" to updatedProperties))
        var updateMappingResponse = AcknowledgedResponse(false)
        try {
            // Adjust max field limit in mappings for query index, if needed.
            adjustMaxFieldLimitForQueryIndex(sourceIndexFieldLimit, targetQueryIndex)
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
                    adjustMaxFieldLimitForQueryIndex(sourceIndexFieldLimit, targetQueryIndex)
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
            monitorMetadata.sourceToQueryIndexMapping[sourceIndex + monitor.id] = targetQueryIndex
        } else {
            val failureMessage = "Failed to resolve targetQueryIndex!"
            log.error(failureMessage)
            throw AlertingException(failureMessage, RestStatus.INTERNAL_SERVER_ERROR, IllegalStateException(failureMessage))
        }
        return Pair(updateMappingResponse, targetQueryIndex)
    }

    /**
     * merge conflicting leaf fields in the mapping tree
     */
    private fun mergeConflictingFields(oldField: Map<String, Any>, newField: Map<String, Any>): Map<String, Any> {
        val mergedField = mutableMapOf<String, Any>()
        oldField.entries.forEach {
            if (newField.containsKey(it.key)) {
                if (it.value is Map<*, *> && newField[it.key] is Map<*, *>) {
                    mergedField[it.key] =
                        mergeConflictingFields(it.value as Map<String, Any>, newField[it.key] as Map<String, Any>)
                } else {
                    mergedField[it.key] = it.value
                }
            } else {
                mergedField[it.key] = it.value
            }
        }

        newField.entries.forEach {
            if (!oldField.containsKey(it.key)) {
                mergedField[it.key] = it.value
            }
        }
        return mergedField
    }

    /**
     * get all fields which have same name but different mappings belonging to an index pattern
     */
    fun getAllConflictingFields(clusterState: ClusterState, concreteIndices: List<String>): Set<String> {
        val conflictingFields = mutableSetOf<String>()
        val allFlattenPaths = mutableMapOf<String, MutableMap<String, Any>>()
        concreteIndices.forEach { concreteIndexName ->
            if (clusterState.routingTable.hasIndex(concreteIndexName)) {
                val indexMetadata = clusterState.metadata.index(concreteIndexName)
                if (indexMetadata.mapping()?.sourceAsMap?.get("properties") != null) {
                    val properties = (
                        (indexMetadata.mapping()?.sourceAsMap?.get("properties"))
                            as MutableMap<String, Any>
                        )
                    // Node processor function is used to process leaves of index mappings tree
                    //
                    val leafNodeProcessor =
                        fun(fieldName: String, _: String, props: MutableMap<String, Any>): Triple<String, String, MutableMap<String, Any>> {
                            return Triple(fieldName, fieldName, props)
                        }
                    // Traverse and update index mappings here while extracting flatten field paths
                    val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()
                    traverseMappingsAndUpdate(properties, "", leafNodeProcessor, flattenPaths)

                    flattenPaths.forEach {
                        if (allFlattenPaths.containsKey(it.key) && allFlattenPaths[it.key]!! != it.value) {
                            conflictingFields.add(it.key)
                        }
                        allFlattenPaths.putIfAbsent(it.key, it.value)
                    }
                }
            }
        }
        return conflictingFields
    }

    /**
     * checks the max field limit for a concrete index
     */
    private suspend fun checkMaxFieldLimit(sourceIndex: String): Long {
        val getSettingsResponse: GetSettingsResponse = client.suspendUntil {
            admin().indices().getSettings(GetSettingsRequest().indices(sourceIndex), it)
        }
        return getSettingsResponse.getSetting(sourceIndex, INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.key)?.toLong() ?: 1000L
    }

    /**
     * Adjusts max field limit index setting for query index if source index has higher limit.
     * This will prevent max field limit exception, when source index has more fields then query index limit
     */
    private suspend fun adjustMaxFieldLimitForQueryIndex(sourceIndexFieldLimit: Long, concreteQueryIndex: String) {
        val getSettingsResponse: GetSettingsResponse = client.suspendUntil {
            admin().indices().getSettings(GetSettingsRequest().indices(concreteQueryIndex), it)
        }
        val queryIndexLimit =
            getSettingsResponse.getSetting(concreteQueryIndex, INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.key)?.toLong() ?: 1000L
        // Our query index initially has 3 fields we defined and 5 more builtin metadata fields in mappings so we have to account for that
        if (sourceIndexFieldLimit > (queryIndexLimit - QUERY_INDEX_BASE_FIELDS_COUNT)) {
            val updateSettingsResponse: AcknowledgedResponse = client.suspendUntil {
                admin().indices().updateSettings(
                    UpdateSettingsRequest(concreteQueryIndex).settings(
                        Settings.builder().put(
                            INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.key,
                            sourceIndexFieldLimit + QUERY_INDEX_BASE_FIELDS_COUNT
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
