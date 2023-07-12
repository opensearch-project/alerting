/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.clusterMetricsMonitorHelpers

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ActionResponse
import org.opensearch.action.ValidateActions
import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.action.admin.cluster.state.ClusterStateResponse
import org.opensearch.action.admin.indices.stats.CommonStats
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.admin.indices.stats.ShardStats
import org.opensearch.alerting.util.IndexUtils.Companion.VALID_INDEX_NAME_REGEX
import org.opensearch.cluster.routing.UnassignedInfo
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.index.cache.query.QueryCacheStats
import org.opensearch.index.engine.CommitStats
import org.opensearch.index.engine.Engine
import org.opensearch.index.engine.SegmentsStats
import org.opensearch.index.fielddata.FieldDataStats
import org.opensearch.index.flush.FlushStats
import org.opensearch.index.get.GetStats
import org.opensearch.index.merge.MergeStats
import org.opensearch.index.refresh.RefreshStats
import org.opensearch.index.search.stats.SearchStats
import org.opensearch.index.seqno.SeqNoStats
import org.opensearch.index.shard.DocsStats
import org.opensearch.index.store.StoreStats
import org.opensearch.search.suggest.completion.CompletionStats
import java.time.Instant
import java.util.Locale
import java.util.function.Function

class CatShardsRequestWrapper(val pathParams: String = "") : ActionRequest() {
    var clusterStateRequest: ClusterStateRequest =
        ClusterStateRequest().clear().nodes(true).routingTable(true)
    var indicesStatsRequest: IndicesStatsRequest =
        IndicesStatsRequest().all()
    var indicesList = arrayOf<String>()

    init {
        if (pathParams.isNotBlank()) {
            indicesList = pathParams.split(",").toTypedArray()

            require(validate() == null) { "The path parameters do not form a valid, comma-separated list of data streams, indices, or index aliases." }

            clusterStateRequest = clusterStateRequest.indices(*indicesList)
            indicesStatsRequest = indicesStatsRequest.indices(*indicesList)
        }
    }

    override fun validate(): ActionRequestValidationException? {
        var exception: ActionRequestValidationException? = null
        if (pathParams.isNotBlank() && indicesList.any { !VALID_INDEX_NAME_REGEX.containsMatchIn(it) })
            exception = ValidateActions.addValidationError(
                "The path parameters do not form a valid, comma-separated list of data streams, indices, or index aliases.",
                exception
            )
        return exception
    }
}

class CatShardsResponseWrapper(
    stateResp: ClusterStateResponse,
    indicesResp: IndicesStatsResponse
) : ActionResponse(), ToXContentObject {
    var shardInfoList: List<ShardInfo> = listOf()

    init {
        shardInfoList = compileShardInfo(stateResp, indicesResp)
    }

    companion object {
        const val WRAPPER_FIELD = "shards"
    }

    override fun writeTo(out: StreamOutput) {
        out.writeList(shardInfoList)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.startArray(WRAPPER_FIELD)
        shardInfoList.forEach { it.toXContent(builder, params) }
        builder.endArray()
        return builder.endObject()
    }

    private fun <S, T> getOrNull(stats: S?, accessor: Function<S, T>, func: Function<T, Any>): Any? {
        if (stats != null) {
            val t: T? = accessor.apply(stats)
            if (t != null) {
                return func.apply(t)
            }
        }
        return null
    }

    private fun compileShardInfo(
        stateResp: ClusterStateResponse,
        indicesResp: IndicesStatsResponse
    ): List<ShardInfo> {
        val list = mutableListOf<ShardInfo>()

        for (shard in stateResp.state.routingTable.allShards()) {
            val shardStats = indicesResp.asMap()[shard]
            var commonStats: CommonStats? = null
            var commitStats: CommitStats? = null
            if (shardStats != null) {
                commonStats = shardStats.stats
                commitStats = shardStats.commitStats
            }

            var shardInfo = ShardInfo(
                index = shard.indexName,
                shard = "${shard.id}",
                primaryOrReplica = if (shard.primary()) "p" else "r",
                state = shard.state().name,
                docs = getOrNull(commonStats, CommonStats::getDocs, DocsStats::getCount)?.toString(),
                store = getOrNull(commonStats, CommonStats::getStore, StoreStats::getSize)?.toString(),
                id = null, // Added below
                node = null, // Added below
                completionSize = getOrNull(commonStats, CommonStats::getCompletion, CompletionStats::getSize)?.toString(),
                fieldDataMemory = getOrNull(commonStats, CommonStats::getFieldData, FieldDataStats::getMemorySize)?.toString(),
                fieldDataEvictions = getOrNull(commonStats, CommonStats::getFieldData, FieldDataStats::getEvictions)?.toString(),
                flushTotal = getOrNull(commonStats, CommonStats::getFlush, FlushStats::getTotal)?.toString(),
                flushTotalTime = getOrNull(commonStats, CommonStats::getFlush, FlushStats::getTotalTime)?.toString(),
                getCurrent = getOrNull(commonStats, CommonStats::getGet, GetStats::current)?.toString(),
                getTime = getOrNull(commonStats, CommonStats::getGet, GetStats::getTime)?.toString(),
                getTotal = getOrNull(commonStats, CommonStats::getGet, GetStats::getCount)?.toString(),
                getExistsTime = getOrNull(commonStats, CommonStats::getGet, GetStats::getExistsTime)?.toString(),
                getExistsTotal = getOrNull(commonStats, CommonStats::getGet, GetStats::getExistsCount)?.toString(),
                getMissingTime = getOrNull(commonStats, CommonStats::getGet, GetStats::getMissingTime)?.toString(),
                getMissingTotal = getOrNull(commonStats, CommonStats::getGet, GetStats::getMissingCount)?.toString(),
                indexingDeleteCurrent = getOrNull(commonStats, CommonStats::getIndexing, { it.total.deleteCurrent })?.toString(),
                indexingDeleteTime = getOrNull(commonStats, CommonStats::getIndexing, { it.total.deleteTime })?.toString(),
                indexingDeleteTotal = getOrNull(commonStats, CommonStats::getIndexing, { it.total.deleteCount })?.toString(),
                indexingIndexCurrent = getOrNull(commonStats, CommonStats::getIndexing, { it.total.indexCurrent })?.toString(),
                indexingIndexTime = getOrNull(commonStats, CommonStats::getIndexing, { it.total.indexTime })?.toString(),
                indexingIndexTotal = getOrNull(commonStats, CommonStats::getIndexing, { it.total.indexCount })?.toString(),
                indexingIndexFailed = getOrNull(commonStats, CommonStats::getIndexing, { it.total.indexFailedCount })?.toString(),
                mergesCurrent = getOrNull(commonStats, CommonStats::getMerge, MergeStats::getCurrent)?.toString(),
                mergesCurrentDocs = getOrNull(commonStats, CommonStats::getMerge, MergeStats::getCurrentNumDocs)?.toString(),
                mergesCurrentSize = getOrNull(commonStats, CommonStats::getMerge, MergeStats::getCurrentSize)?.toString(),
                mergesTotal = getOrNull(commonStats, CommonStats::getMerge, MergeStats::getTotal)?.toString(),
                mergesTotalDocs = getOrNull(commonStats, CommonStats::getMerge, MergeStats::getTotalNumDocs)?.toString(),
                mergesTotalSize = getOrNull(commonStats, CommonStats::getMerge, MergeStats::getTotalSize)?.toString(),
                mergesTotalTime = getOrNull(commonStats, CommonStats::getMerge, MergeStats::getTotalTime)?.toString(),
                queryCacheMemory = getOrNull(commonStats, CommonStats::getQueryCache, QueryCacheStats::getMemorySize)?.toString(),
                queryCacheEvictions = getOrNull(commonStats, CommonStats::getQueryCache, QueryCacheStats::getEvictions)?.toString(),
                recoverySourceType = null, // Added below
                refreshTotal = getOrNull(commonStats, CommonStats::getRefresh, RefreshStats::getTotal)?.toString(),
                refreshTime = getOrNull(commonStats, CommonStats::getRefresh, RefreshStats::getTotalTime)?.toString(),
                searchFetchCurrent = getOrNull(commonStats, CommonStats::getSearch, { it.total.fetchCurrent })?.toString(),
                searchFetchTime = getOrNull(commonStats, CommonStats::getSearch, { it.total.fetchTime })?.toString(),
                searchFetchTotal = getOrNull(commonStats, CommonStats::getSearch, { it.total.fetchCount })?.toString(),
                searchOpenContexts = getOrNull(commonStats, CommonStats::getSearch, SearchStats::getOpenContexts)?.toString(),
                searchQueryCurrent = getOrNull(commonStats, CommonStats::getSearch, { it.total.queryCurrent })?.toString(),
                searchQueryTime = getOrNull(commonStats, CommonStats::getSearch, { it.total.queryTime })?.toString(),
                searchQueryTotal = getOrNull(commonStats, CommonStats::getSearch, { it.total.queryCount })?.toString(),
                searchScrollCurrent = getOrNull(commonStats, CommonStats::getSearch, { it.total.scrollCurrent })?.toString(),
                searchScrollTime = getOrNull(commonStats, CommonStats::getSearch, { it.total.scrollTime })?.toString(),
                searchScrollTotal = getOrNull(commonStats, CommonStats::getSearch, { it.total.scrollCount })?.toString(),
                segmentsCount = getOrNull(commonStats, CommonStats::getSegments, SegmentsStats::getCount)?.toString(),
                segmentsMemory = getOrNull(commonStats, CommonStats::getSegments, SegmentsStats::getZeroMemory)?.toString(),
                segmentsIndexWriterMemory = getOrNull(commonStats, CommonStats::getSegments, SegmentsStats::getIndexWriterMemory)?.toString(),
                segmentsVersionMapMemory = getOrNull(commonStats, CommonStats::getSegments, SegmentsStats::getVersionMapMemory)?.toString(),
                fixedBitsetMemory = getOrNull(commonStats, CommonStats::getSegments, SegmentsStats::getBitsetMemory)?.toString(),
                globalCheckpoint = getOrNull(shardStats, ShardStats::getSeqNoStats, SeqNoStats::getGlobalCheckpoint)?.toString(),
                localCheckpoint = getOrNull(shardStats, ShardStats::getSeqNoStats, SeqNoStats::getLocalCheckpoint)?.toString(),
                maxSeqNo = getOrNull(shardStats, ShardStats::getSeqNoStats, SeqNoStats::getMaxSeqNo)?.toString(),
                syncId = commitStats?.userData?.get(Engine.SYNC_COMMIT_ID),
                unassignedAt = null, // Added below
                unassignedDetails = null, // Added below
                unassignedFor = null, // Added below
                unassignedReason = null // Added below
            )

            if (shard.assignedToNode()) {
                val id = shard.currentNodeId()
                val node = StringBuilder()
                node.append(stateResp.state.nodes().get(id).name)

                if (shard.relocating()) {
                    val reloNodeId = shard.relocatingNodeId()
                    val reloName = stateResp.state.nodes().get(reloNodeId).name
                    node.append(" -> ")
                    node.append(reloNodeId)
                    node.append(" ")
                    node.append(reloName)
                }

                shardInfo = shardInfo.copy(
                    id = id,
                    node = node.toString()
                )
            }

            if (shard.unassignedInfo() != null) {
                val unassignedTime = Instant.ofEpochMilli(shard.unassignedInfo().unassignedTimeInMillis)
                shardInfo = shardInfo.copy(
                    unassignedReason = shard.unassignedInfo().reason.name,
                    unassignedAt = UnassignedInfo.DATE_TIME_FORMATTER.format(unassignedTime),
                    unassignedFor = TimeValue.timeValueMillis(System.currentTimeMillis() - shard.unassignedInfo().unassignedTimeInMillis).stringRep,
                    unassignedDetails = shard.unassignedInfo().details
                )
            }

            if (shard.recoverySource() != null) {
                shardInfo = shardInfo.copy(
                    recoverySourceType = shard.recoverySource().type.toString().lowercase(Locale.ROOT)
                )
            }

            list.add(shardInfo)
        }
        return list
    }

    data class ShardInfo(
        val index: String?,
        val shard: String?,
        val primaryOrReplica: String?,
        val state: String?,
        val docs: String?,
        val store: String?,
        val id: String?,
        val node: String?,
        val completionSize: String?,
        val fieldDataMemory: String?,
        val fieldDataEvictions: String?,
        val flushTotal: String?,
        val flushTotalTime: String?,
        val getCurrent: String?,
        val getTime: String?,
        val getTotal: String?,
        val getExistsTime: String?,
        val getExistsTotal: String?,
        val getMissingTime: String?,
        val getMissingTotal: String?,
        val indexingDeleteCurrent: String?,
        val indexingDeleteTime: String?,
        val indexingDeleteTotal: String?,
        val indexingIndexCurrent: String?,
        val indexingIndexTime: String?,
        val indexingIndexTotal: String?,
        val indexingIndexFailed: String?,
        val mergesCurrent: String?,
        val mergesCurrentDocs: String?,
        val mergesCurrentSize: String?,
        val mergesTotal: String?,
        val mergesTotalDocs: String?,
        val mergesTotalSize: String?,
        val mergesTotalTime: String?,
        val queryCacheMemory: String?,
        val queryCacheEvictions: String?,
        val recoverySourceType: String?,
        val refreshTotal: String?,
        val refreshTime: String?,
        val searchFetchCurrent: String?,
        val searchFetchTime: String?,
        val searchFetchTotal: String?,
        val searchOpenContexts: String?,
        val searchQueryCurrent: String?,
        val searchQueryTime: String?,
        val searchQueryTotal: String?,
        val searchScrollCurrent: String?,
        val searchScrollTime: String?,
        val searchScrollTotal: String?,
        val segmentsCount: String?,
        val segmentsMemory: String?,
        val segmentsIndexWriterMemory: String?,
        val segmentsVersionMapMemory: String?,
        val fixedBitsetMemory: String?,
        val globalCheckpoint: String?,
        val localCheckpoint: String?,
        val maxSeqNo: String?,
        val syncId: String?,
        val unassignedAt: String?,
        val unassignedDetails: String?,
        val unassignedFor: String?,
        val unassignedReason: String?
    ) : ToXContentObject, Writeable {
        companion object {
            const val INDEX_FIELD = "index"
            const val SHARD_FIELD = "shard"
            const val PRIMARY_OR_REPLICA_FIELD = "primaryOrReplica"
            const val STATE_FIELD = "state"
            const val DOCS_FIELD = "docs"
            const val STORE_FIELD = "store"
            const val ID_FIELD = "id"
            const val NODE_FIELD = "node"
            const val COMPLETION_SIZE_FIELD = "completionSize"
            const val FIELD_DATA_MEMORY_FIELD = "fielddataMemory"
            const val FIELD_DATA_EVICTIONS_FIELD = "fielddataEvictions"
            const val FLUSH_TOTAL_FIELD = "flushTotal"
            const val FLUSH_TOTAL_TIME_FIELD = "flushTotalTime"
            const val GET_CURRENT_FIELD = "getCurrent"
            const val GET_TIME_FIELD = "getTime"
            const val GET_TOTAL_FIELD = "getTotal"
            const val GET_EXISTS_TIME_FIELD = "getExistsTime"
            const val GET_EXISTS_TOTAL_FIELD = "getExistsTotal"
            const val GET_MISSING_TIME_FIELD = "getMissingTime"
            const val GET_MISSING_TOTAL_FIELD = "getMissingTotal"
            const val INDEXING_DELETE_CURRENT_FIELD = "indexingDeleteCurrent"
            const val INDEXING_DELETE_TIME_FIELD = "indexingDeleteTime"
            const val INDEXING_DELETE_TOTAL_FIELD = "indexingDeleteTotal"
            const val INDEXING_INDEX_CURRENT_FIELD = "indexingIndexCurrent"
            const val INDEXING_INDEX_TIME_FIELD = "indexingIndexTime"
            const val INDEXING_INDEX_TOTAL_FIELD = "indexingIndexTotal"
            const val INDEXING_INDEX_FAILED_FIELD = "indexingIndexFailed"
            const val MERGES_CURRENT_FIELD = "mergesCurrent"
            const val MERGES_CURRENT_DOCS_FIELD = "mergesCurrentDocs"
            const val MERGES_CURRENT_SIZE_FIELD = "mergesCurrentSize"
            const val MERGES_TOTAL_FIELD = "mergesTotal"
            const val MERGES_TOTAL_DOCS_FIELD = "mergesTotalDocs"
            const val MERGES_TOTAL_SIZE_FIELD = "mergesTotalSize"
            const val MERGES_TOTAL_TIME_FIELD = "mergesTotalTime"
            const val QUERY_CACHE_MEMORY_FIELD = "queryCacheMemory"
            const val QUERY_CACHE_EVICTIONS_FIELD = "queryCacheEvictions"
            const val RECOVERY_SOURCE_TYPE_FIELD = "recoverysource.type"
            const val REFRESH_TOTAL_FIELD = "refreshTotal"
            const val REFRESH_TIME_FIELD = "refreshTime"
            const val SEARCH_FETCH_CURRENT_FIELD = "searchFetchCurrent"
            const val SEARCH_FETCH_TIME_FIELD = "searchFetchTime"
            const val SEARCH_FETCH_TOTAL_FIELD = "searchFetchTotal"
            const val SEARCH_OPEN_CONTEXTS_FIELD = "searchOpenContexts"
            const val SEARCH_QUERY_CURRENT_FIELD = "searchQueryCurrent"
            const val SEARCH_QUERY_TIME_FIELD = "searchQueryTime"
            const val SEARCH_QUERY_TOTAL_FIELD = "searchQueryTotal"
            const val SEARCH_SCROLL_CURRENT_FIELD = "searchScrollCurrent"
            const val SEARCH_SCROLL_TIME_FIELD = "searchScrollTime"
            const val SEARCH_SCROLL_TOTAL_FIELD = "searchScrollTotal"
            const val SEGMENTS_COUNT_FIELD = "segmentsCount"
            const val SEGMENTS_MEMORY_FIELD = "segmentsMemory"
            const val SEGMENTS_INDEX_WRITER_MEMORY_FIELD = "segmentsIndexWriterMemory"
            const val SEGMENTS_VERSION_MAP_MEMORY_FIELD = "segmentsVersionMapMemory"
            const val FIXED_BITSET_MEMORY_FIELD = "fixedBitsetMemory"
            const val GLOBAL_CHECKPOINT_FIELD = "globalCheckpoint"
            const val LOCAL_CHECKPOINT_FIELD = "localCheckpoint"
            const val MAX_SEQ_NO_FIELD = "maxSeqNo"
            const val SYNC_ID_FIELD = "sync_id"
            const val UNASSIGNED_AT_FIELD = "unassigned.at"
            const val UNASSIGNED_DETAILS_FIELD = "unassigned.details"
            const val UNASSIGNED_FOR_FIELD = "unassigned.for"
            const val UNASSIGNED_REASON_FIELD = "unassigned.reason"
        }

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            builder.startObject()
                .field(INDEX_FIELD, index)
                .field(SHARD_FIELD, shard)
                .field(PRIMARY_OR_REPLICA_FIELD, primaryOrReplica)
                .field(STATE_FIELD, state)
                .field(DOCS_FIELD, docs)
                .field(STORE_FIELD, store)
                .field(ID_FIELD, id)
                .field(NODE_FIELD, node)
                .field(COMPLETION_SIZE_FIELD, completionSize)
                .field(FIELD_DATA_MEMORY_FIELD, fieldDataMemory)
                .field(FIELD_DATA_EVICTIONS_FIELD, fieldDataEvictions)
                .field(FLUSH_TOTAL_FIELD, flushTotal)
                .field(FLUSH_TOTAL_TIME_FIELD, flushTotalTime)
                .field(GET_CURRENT_FIELD, getCurrent)
                .field(GET_TIME_FIELD, getTime)
                .field(GET_TOTAL_FIELD, getTotal)
                .field(GET_EXISTS_TIME_FIELD, getExistsTime)
                .field(GET_EXISTS_TOTAL_FIELD, getExistsTotal)
                .field(GET_MISSING_TIME_FIELD, getMissingTime)
                .field(GET_MISSING_TOTAL_FIELD, getMissingTotal)
                .field(INDEXING_DELETE_CURRENT_FIELD, indexingDeleteCurrent)
                .field(INDEXING_DELETE_TIME_FIELD, indexingDeleteTime)
                .field(INDEXING_DELETE_TOTAL_FIELD, indexingDeleteTotal)
                .field(INDEXING_INDEX_CURRENT_FIELD, indexingIndexCurrent)
                .field(INDEXING_INDEX_TIME_FIELD, indexingIndexTime)
                .field(INDEXING_INDEX_TOTAL_FIELD, indexingIndexTotal)
                .field(INDEXING_INDEX_FAILED_FIELD, indexingIndexFailed)
                .field(MERGES_CURRENT_FIELD, mergesCurrent)
                .field(MERGES_CURRENT_DOCS_FIELD, mergesCurrentDocs)
                .field(MERGES_CURRENT_SIZE_FIELD, mergesCurrentSize)
                .field(MERGES_TOTAL_FIELD, mergesTotal)
                .field(MERGES_TOTAL_DOCS_FIELD, mergesTotalDocs)
                .field(MERGES_TOTAL_SIZE_FIELD, mergesTotalSize)
                .field(MERGES_TOTAL_TIME_FIELD, mergesTotalTime)
                .field(QUERY_CACHE_MEMORY_FIELD, queryCacheMemory)
                .field(QUERY_CACHE_EVICTIONS_FIELD, queryCacheEvictions)
                .field(RECOVERY_SOURCE_TYPE_FIELD, recoverySourceType)
                .field(REFRESH_TOTAL_FIELD, refreshTotal)
                .field(REFRESH_TIME_FIELD, refreshTime)
                .field(SEARCH_FETCH_CURRENT_FIELD, searchFetchCurrent)
                .field(SEARCH_FETCH_TIME_FIELD, searchFetchTime)
                .field(SEARCH_FETCH_TOTAL_FIELD, searchFetchTotal)
                .field(SEARCH_OPEN_CONTEXTS_FIELD, searchOpenContexts)
                .field(SEARCH_QUERY_CURRENT_FIELD, searchQueryCurrent)
                .field(SEARCH_QUERY_TIME_FIELD, searchQueryTime)
                .field(SEARCH_QUERY_TOTAL_FIELD, searchQueryTotal)
                .field(SEARCH_SCROLL_CURRENT_FIELD, searchScrollCurrent)
                .field(SEARCH_SCROLL_TIME_FIELD, searchScrollTime)
                .field(SEARCH_SCROLL_TOTAL_FIELD, searchScrollTotal)
                .field(SEGMENTS_COUNT_FIELD, segmentsCount)
                .field(SEGMENTS_MEMORY_FIELD, segmentsMemory)
                .field(SEGMENTS_INDEX_WRITER_MEMORY_FIELD, segmentsIndexWriterMemory)
                .field(SEGMENTS_VERSION_MAP_MEMORY_FIELD, segmentsVersionMapMemory)
                .field(FIXED_BITSET_MEMORY_FIELD, fixedBitsetMemory)
                .field(GLOBAL_CHECKPOINT_FIELD, globalCheckpoint)
                .field(LOCAL_CHECKPOINT_FIELD, localCheckpoint)
                .field(MAX_SEQ_NO_FIELD, maxSeqNo)
                .field(SYNC_ID_FIELD, syncId)
                .field(UNASSIGNED_AT_FIELD, unassignedAt)
                .field(UNASSIGNED_DETAILS_FIELD, unassignedDetails)
                .field(UNASSIGNED_FOR_FIELD, unassignedFor)
                .field(UNASSIGNED_REASON_FIELD, unassignedReason)
            return builder.endObject()
        }

        override fun writeTo(out: StreamOutput) {
            out.writeString(index)
            out.writeString(shard)
            out.writeString(primaryOrReplica)
            out.writeString(state)
            out.writeString(docs)
            out.writeString(store)
            out.writeString(id)
            out.writeString(node)
            out.writeString(completionSize)
            out.writeString(fieldDataMemory)
            out.writeString(fieldDataEvictions)
            out.writeString(flushTotal)
            out.writeString(flushTotalTime)
            out.writeString(getCurrent)
            out.writeString(getTime)
            out.writeString(getTotal)
            out.writeString(getExistsTime)
            out.writeString(getExistsTotal)
            out.writeString(getMissingTime)
            out.writeString(getMissingTotal)
            out.writeString(indexingDeleteCurrent)
            out.writeString(indexingDeleteTime)
            out.writeString(indexingDeleteTotal)
            out.writeString(indexingIndexCurrent)
            out.writeString(indexingIndexTime)
            out.writeString(indexingIndexTotal)
            out.writeString(indexingIndexFailed)
            out.writeString(mergesCurrent)
            out.writeString(mergesCurrentDocs)
            out.writeString(mergesCurrentSize)
            out.writeString(mergesTotal)
            out.writeString(mergesTotalDocs)
            out.writeString(mergesTotalSize)
            out.writeString(mergesTotalTime)
            out.writeString(queryCacheMemory)
            out.writeString(queryCacheEvictions)
            out.writeString(recoverySourceType)
            out.writeString(refreshTotal)
            out.writeString(refreshTime)
            out.writeString(searchFetchCurrent)
            out.writeString(searchFetchTime)
            out.writeString(searchFetchTotal)
            out.writeString(searchOpenContexts)
            out.writeString(searchQueryCurrent)
            out.writeString(searchQueryTime)
            out.writeString(searchQueryTotal)
            out.writeString(searchScrollCurrent)
            out.writeString(searchScrollTime)
            out.writeString(searchScrollTotal)
            out.writeString(segmentsCount)
            out.writeString(segmentsMemory)
            out.writeString(segmentsIndexWriterMemory)
            out.writeString(segmentsVersionMapMemory)
            out.writeString(fixedBitsetMemory)
            out.writeString(globalCheckpoint)
            out.writeString(localCheckpoint)
            out.writeString(maxSeqNo)
            out.writeString(syncId)
            out.writeString(unassignedAt)
            out.writeString(unassignedDetails)
            out.writeString(unassignedFor)
            out.writeString(unassignedReason)
        }
    }
}
