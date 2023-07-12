/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.clusterMetricsMonitorHelpers

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ActionResponse
import org.opensearch.action.ValidateActions
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse
import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.action.admin.cluster.state.ClusterStateResponse
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse
import org.opensearch.action.admin.indices.stats.CommonStats
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.support.IndicesOptions
import org.opensearch.alerting.util.IndexUtils.Companion.VALID_INDEX_NAME_REGEX
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.time.DateFormatter
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.index.IndexSettings
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.Locale

class CatIndicesRequestWrapper(val pathParams: String = "") : ActionRequest() {
    val log = LogManager.getLogger(CatIndicesRequestWrapper::class.java)

    var clusterHealthRequest: ClusterHealthRequest =
        ClusterHealthRequest().indicesOptions(IndicesOptions.lenientExpandHidden())
    var clusterStateRequest: ClusterStateRequest =
        ClusterStateRequest().indicesOptions(IndicesOptions.lenientExpandHidden())
    var indexSettingsRequest: GetSettingsRequest =
        GetSettingsRequest()
            .indicesOptions(IndicesOptions.lenientExpandHidden())
            .names(IndexSettings.INDEX_SEARCH_THROTTLED.key)
    var indicesStatsRequest: IndicesStatsRequest =
        IndicesStatsRequest().all().indicesOptions(IndicesOptions.lenientExpandHidden())
    var indicesList = arrayOf<String>()

    init {
        if (pathParams.isNotBlank()) {
            indicesList = pathParams.split(",").toTypedArray()

            require(validate() == null) { "The path parameters do not form a valid, comma-separated list of data streams, indices, or index aliases." }

            clusterHealthRequest = clusterHealthRequest.indices(*indicesList)
            clusterStateRequest = clusterStateRequest.indices(*indicesList)
            indexSettingsRequest = indexSettingsRequest.indices(*indicesList)
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

class CatIndicesResponseWrapper(
    clusterHealthResponse: ClusterHealthResponse,
    clusterStateResponse: ClusterStateResponse,
    indexSettingsResponse: GetSettingsResponse,
    indicesStatsResponse: IndicesStatsResponse
) : ActionResponse(), ToXContentObject {
    var indexInfoList: List<IndexInfo> = listOf()

    init {
        indexInfoList = compileIndexInfo(
            clusterHealthResponse,
            clusterStateResponse,
            indexSettingsResponse,
            indicesStatsResponse
        )
    }

    companion object {
        const val WRAPPER_FIELD = "indices"
    }

    override fun writeTo(out: StreamOutput) {
        out.writeList(indexInfoList)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.startArray(WRAPPER_FIELD)
        indexInfoList.forEach { it.toXContent(builder, params) }
        builder.endArray()
        return builder.endObject()
    }

    private fun compileIndexInfo(
        clusterHealthResponse: ClusterHealthResponse,
        clusterStateResponse: ClusterStateResponse,
        indexSettingsResponse: GetSettingsResponse,
        indicesStatsResponse: IndicesStatsResponse
    ): List<IndexInfo> {
        val list = mutableListOf<IndexInfo>()

        val indicesSettings = indexSettingsResponse.indexToSettings
        val indicesHealths = clusterHealthResponse.indices
        val indicesStats = indicesStatsResponse.indices
        val indicesMetadatas = hashMapOf<String, IndexMetadata>()
        clusterStateResponse.state.metadata.forEach { indicesMetadatas[it.index.name] = it }

        indicesSettings.forEach { (indexName, settings) ->
            if (!indicesMetadatas.containsKey(indexName)) return@forEach

            val indexMetadata = indicesMetadatas[indexName]
            val indexState = indexMetadata?.state
            val indexStats = indicesStats[indexName]
            val searchThrottled = IndexSettings.INDEX_SEARCH_THROTTLED.get(settings)
            val indexHealth = indicesHealths[indexName]

            var health = ""
            if (indexHealth != null) {
                health = indexHealth.status.toString().lowercase(Locale.ROOT)
            } else if (indexStats != null) {
                health = "red*"
            }

            val primaryStats: CommonStats?
            val totalStats: CommonStats?
            if (indexStats == null || indexState == IndexMetadata.State.CLOSE) {
                primaryStats = CommonStats()
                totalStats = CommonStats()
            } else {
                primaryStats = indexStats.primaries
                totalStats = indexStats.total
            }

            list.add(
                IndexInfo(
                    health = health,
                    status = indexState.toString().lowercase(Locale.ROOT),
                    index = indexName,
                    uuid = indexMetadata?.indexUUID,
                    pri = "${indexHealth?.numberOfShards}",
                    rep = "${indexHealth?.numberOfReplicas}",
                    docsCount = "${primaryStats?.getDocs()?.count}",
                    docsDeleted = "${primaryStats?.getDocs()?.deleted}",
                    creationDate = "${indexMetadata?.creationDate}",
                    creationDateString = DateFormatter.forPattern("strict_date_time")
                        .format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(indexMetadata!!.creationDate), ZoneOffset.UTC)),
                    storeSize = "${totalStats?.store?.size}",
                    priStoreSize = "${primaryStats?.store?.size}",
                    completionSize = "${totalStats?.completion?.size}",
                    priCompletionSize = "${primaryStats?.completion?.size}",
                    fieldDataMemorySize = "${totalStats?.fieldData?.memorySize}",
                    priFieldDataMemorySize = "${primaryStats?.fieldData?.memorySize}",
                    fieldDataEvictions = "${totalStats?.fieldData?.evictions}",
                    priFieldDataEvictions = "${primaryStats?.fieldData?.evictions}",
                    queryCacheMemorySize = "${totalStats?.queryCache?.memorySize}",
                    priQueryCacheMemorySize = "${primaryStats?.queryCache?.memorySize}",
                    queryCacheEvictions = "${totalStats?.queryCache?.evictions}",
                    priQueryCacheEvictions = "${primaryStats?.queryCache?.evictions}",
                    requestCacheMemorySize = "${totalStats?.requestCache?.memorySize}",
                    priRequestCacheMemorySize = "${primaryStats?.requestCache?.memorySize}",
                    requestCacheEvictions = "${totalStats?.requestCache?.evictions}",
                    priRequestCacheEvictions = "${primaryStats?.requestCache?.evictions}",
                    requestCacheHitCount = "${totalStats?.requestCache?.hitCount}",
                    priRequestCacheHitCount = "${primaryStats?.requestCache?.hitCount}",
                    requestCacheMissCount = "${totalStats?.requestCache?.missCount}",
                    priRequestCacheMissCount = "${primaryStats?.requestCache?.missCount}",
                    flushTotal = "${totalStats?.flush?.total}",
                    priFlushTotal = "${primaryStats?.flush?.total}",
                    flushTotalTime = "${totalStats?.flush?.totalTime}",
                    priFlushTotalTime = "${primaryStats?.flush?.totalTime}",
                    getCurrent = "${totalStats?.get?.current()}",
                    priGetCurrent = "${primaryStats?.get?.current()}",
                    getTime = "${totalStats?.get?.time}",
                    priGetTime = "${primaryStats?.get?.time}",
                    getTotal = "${totalStats?.get?.count}",
                    priGetTotal = "${primaryStats?.get?.count}",
                    getExistsTime = "${totalStats?.get?.existsTime}",
                    priGetExistsTime = "${primaryStats?.get?.existsTime}",
                    getExistsTotal = "${totalStats?.get?.existsCount}",
                    priGetExistsTotal = "${primaryStats?.get?.existsCount}",
                    getMissingTime = "${totalStats?.get?.missingTime}",
                    priGetMissingTime = "${primaryStats?.get?.missingTime}",
                    getMissingTotal = "${totalStats?.get?.missingCount}",
                    priGetMissingTotal = "${primaryStats?.get?.missingCount}",
                    indexingDeleteCurrent = "${totalStats?.indexing?.total?.deleteCurrent}",
                    priIndexingDeleteCurrent = "${primaryStats?.indexing?.total?.deleteCurrent}",
                    indexingDeleteTime = "${totalStats?.indexing?.total?.deleteTime}",
                    priIndexingDeleteTime = "${primaryStats?.indexing?.total?.deleteTime}",
                    indexingDeleteTotal = "${totalStats?.indexing?.total?.deleteCount}",
                    priIndexingDeleteTotal = "${primaryStats?.indexing?.total?.deleteCount}",
                    indexingIndexCurrent = "${totalStats?.indexing?.total?.indexCurrent}",
                    priIndexingIndexCurrent = "${primaryStats?.indexing?.total?.indexCurrent}",
                    indexingIndexTime = "${totalStats?.indexing?.total?.indexTime}",
                    priIndexingIndexTime = "${primaryStats?.indexing?.total?.indexTime}",
                    indexingIndexTotal = "${totalStats?.indexing?.total?.indexCount}",
                    priIndexingIndexTotal = "${primaryStats?.indexing?.total?.indexCount}",
                    indexingIndexFailed = "${totalStats?.indexing?.total?.indexFailedCount}",
                    priIndexingIndexFailed = "${primaryStats?.indexing?.total?.indexFailedCount}",
                    mergesCurrent = "${totalStats?.merge?.current}",
                    priMergesCurrent = "${primaryStats?.merge?.current}",
                    mergesCurrentDocs = "${totalStats?.merge?.currentNumDocs}",
                    priMergesCurrentDocs = "${primaryStats?.merge?.currentNumDocs}",
                    mergesCurrentSize = "${totalStats?.merge?.currentSize}",
                    priMergesCurrentSize = "${primaryStats?.merge?.currentSize}",
                    mergesTotal = "${totalStats?.merge?.total}",
                    priMergesTotal = "${primaryStats?.merge?.total}",
                    mergesTotalDocs = "${totalStats?.merge?.totalNumDocs}",
                    priMergesTotalDocs = "${primaryStats?.merge?.totalNumDocs}",
                    mergesTotalSize = "${totalStats?.merge?.totalSize}",
                    priMergesTotalSize = "${primaryStats?.merge?.totalSize}",
                    mergesTotalTime = "${totalStats?.merge?.totalTime}",
                    priMergesTotalTime = "${primaryStats?.merge?.totalTime}",
                    refreshTotal = "${totalStats?.refresh?.total}",
                    priRefreshTotal = "${primaryStats?.refresh?.total}",
                    refreshTime = "${totalStats?.refresh?.totalTime}",
                    priRefreshTime = "${primaryStats?.refresh?.totalTime}",
                    refreshExternalTotal = "${totalStats?.refresh?.externalTotal}",
                    priRefreshExternalTotal = "${primaryStats?.refresh?.externalTotal}",
                    refreshExternalTime = "${totalStats?.refresh?.externalTotalTime}",
                    priRefreshExternalTime = "${primaryStats?.refresh?.externalTotalTime}",
                    refreshListeners = "${totalStats?.refresh?.listeners}",
                    priRefreshListeners = "${primaryStats?.refresh?.listeners}",
                    searchFetchCurrent = "${totalStats?.search?.total?.fetchCurrent}",
                    priSearchFetchCurrent = "${primaryStats?.search?.total?.fetchCurrent}",
                    searchFetchTime = "${totalStats?.search?.total?.fetchTime}",
                    priSearchFetchTime = "${primaryStats?.search?.total?.fetchTime}",
                    searchFetchTotal = "${totalStats?.search?.total?.fetchCount}",
                    priSearchFetchTotal = "${primaryStats?.search?.total?.fetchCount}",
                    searchOpenContexts = "${totalStats?.search?.openContexts}",
                    priSearchOpenContexts = "${primaryStats?.search?.openContexts}",
                    searchQueryCurrent = "${totalStats?.search?.total?.queryCurrent}",
                    priSearchQueryCurrent = "${primaryStats?.search?.total?.queryCurrent}",
                    searchQueryTime = "${totalStats?.search?.total?.queryTime}",
                    priSearchQueryTime = "${primaryStats?.search?.total?.queryTime}",
                    searchQueryTotal = "${totalStats?.search?.total?.queryCount}",
                    priSearchQueryTotal = "${primaryStats?.search?.total?.queryCount}",
                    searchScrollCurrent = "${totalStats?.search?.total?.scrollCurrent}",
                    priSearchScrollCurrent = "${primaryStats?.search?.total?.scrollCurrent}",
                    searchScrollTime = "${totalStats?.search?.total?.scrollTime}",
                    priSearchScrollTime = "${primaryStats?.search?.total?.scrollTime}",
                    searchScrollTotal = "${totalStats?.search?.total?.scrollCount}",
                    priSearchScrollTotal = "${primaryStats?.search?.total?.scrollCount}",
                    searchPointInTimeCurrent = "${totalStats?.search?.total?.pitCurrent}",
                    priSearchPointInTimeCurrent = "${primaryStats?.search?.total?.pitCurrent}",
                    searchPointInTimeTime = "${totalStats?.search?.total?.pitTime}",
                    priSearchPointInTimeTime = "${primaryStats?.search?.total?.pitTime}",
                    searchPointInTimeTotal = "${totalStats?.search?.total?.pitCount}",
                    priSearchPointInTimeTotal = "${primaryStats?.search?.total?.pitCount}",
                    segmentsCount = "${totalStats?.segments?.count}",
                    priSegmentsCount = "${primaryStats?.segments?.count}",
                    segmentsMemory = "${totalStats?.segments?.zeroMemory}",
                    priSegmentsMemory = "${primaryStats?.segments?.zeroMemory}",
                    segmentsIndexWriterMemory = "${totalStats?.segments?.indexWriterMemory}",
                    priSegmentsIndexWriterMemory = "${primaryStats?.segments?.indexWriterMemory}",
                    segmentsVersionMapMemory = "${totalStats?.segments?.versionMapMemory}",
                    priSegmentsVersionMapMemory = "${primaryStats?.segments?.versionMapMemory}",
                    segmentsFixedBitsetMemory = "${totalStats?.segments?.bitsetMemory}",
                    priSegmentsFixedBitsetMemory = "${primaryStats?.segments?.bitsetMemory}",
                    warmerCurrent = "${totalStats?.warmer?.current()}",
                    priWarmerCurrent = "${primaryStats?.warmer?.current()}",
                    warmerTotal = "${totalStats?.warmer?.total()}",
                    priWarmerTotal = "${primaryStats?.warmer?.total()}",
                    warmerTotalTime = "${totalStats?.warmer?.totalTime()}",
                    priWarmerTotalTime = "${primaryStats?.warmer?.totalTime()}",
                    suggestCurrent = "${totalStats?.search?.total?.suggestCurrent}",
                    priSuggestCurrent = "${primaryStats?.search?.total?.suggestCurrent}",
                    suggestTime = "${totalStats?.search?.total?.suggestTime}",
                    priSuggestTime = "${primaryStats?.search?.total?.suggestTime}",
                    suggestTotal = "${totalStats?.search?.total?.suggestCount}",
                    priSuggestTotal = "${primaryStats?.search?.total?.suggestCount}",
                    memoryTotal = "${totalStats?.totalMemory}",
                    priMemoryTotal = "${primaryStats?.totalMemory}",
                    searchThrottled = "$searchThrottled",
                )
            )
        }

        return list
    }

    data class IndexInfo(
        val health: String?,
        val status: String?,
        val index: String?,
        val uuid: String?,
        val pri: String?,
        val rep: String?,
        val docsCount: String?,
        val docsDeleted: String?,
        val creationDate: String?,
        val creationDateString: String?,
        val storeSize: String?,
        val priStoreSize: String?,
        val completionSize: String?,
        val priCompletionSize: String?,
        val fieldDataMemorySize: String?,
        val priFieldDataMemorySize: String?,
        val fieldDataEvictions: String?,
        val priFieldDataEvictions: String?,
        val queryCacheMemorySize: String?,
        val priQueryCacheMemorySize: String?,
        val queryCacheEvictions: String?,
        val priQueryCacheEvictions: String?,
        val requestCacheMemorySize: String?,
        val priRequestCacheMemorySize: String?,
        val requestCacheEvictions: String?,
        val priRequestCacheEvictions: String?,
        val requestCacheHitCount: String?,
        val priRequestCacheHitCount: String?,
        val requestCacheMissCount: String?,
        val priRequestCacheMissCount: String?,
        val flushTotal: String?,
        val priFlushTotal: String?,
        val flushTotalTime: String?,
        val priFlushTotalTime: String?,
        val getCurrent: String?,
        val priGetCurrent: String?,
        val getTime: String?,
        val priGetTime: String?,
        val getTotal: String?,
        val priGetTotal: String?,
        val getExistsTime: String?,
        val priGetExistsTime: String?,
        val getExistsTotal: String?,
        val priGetExistsTotal: String?,
        val getMissingTime: String?,
        val priGetMissingTime: String?,
        val getMissingTotal: String?,
        val priGetMissingTotal: String?,
        val indexingDeleteCurrent: String?,
        val priIndexingDeleteCurrent: String?,
        val indexingDeleteTime: String?,
        val priIndexingDeleteTime: String?,
        val indexingDeleteTotal: String?,
        val priIndexingDeleteTotal: String?,
        val indexingIndexCurrent: String?,
        val priIndexingIndexCurrent: String?,
        val indexingIndexTime: String?,
        val priIndexingIndexTime: String?,
        val indexingIndexTotal: String?,
        val priIndexingIndexTotal: String?,
        val indexingIndexFailed: String?,
        val priIndexingIndexFailed: String?,
        val mergesCurrent: String?,
        val priMergesCurrent: String?,
        val mergesCurrentDocs: String?,
        val priMergesCurrentDocs: String?,
        val mergesCurrentSize: String?,
        val priMergesCurrentSize: String?,
        val mergesTotal: String?,
        val priMergesTotal: String?,
        val mergesTotalDocs: String?,
        val priMergesTotalDocs: String?,
        val mergesTotalSize: String?,
        val priMergesTotalSize: String?,
        val mergesTotalTime: String?,
        val priMergesTotalTime: String?,
        val refreshTotal: String?,
        val priRefreshTotal: String?,
        val refreshTime: String?,
        val priRefreshTime: String?,
        val refreshExternalTotal: String?,
        val priRefreshExternalTotal: String?,
        val refreshExternalTime: String?,
        val priRefreshExternalTime: String?,
        val refreshListeners: String?,
        val priRefreshListeners: String?,
        val searchFetchCurrent: String?,
        val priSearchFetchCurrent: String?,
        val searchFetchTime: String?,
        val priSearchFetchTime: String?,
        val searchFetchTotal: String?,
        val priSearchFetchTotal: String?,
        val searchOpenContexts: String?,
        val priSearchOpenContexts: String?,
        val searchQueryCurrent: String?,
        val priSearchQueryCurrent: String?,
        val searchQueryTime: String?,
        val priSearchQueryTime: String?,
        val searchQueryTotal: String?,
        val priSearchQueryTotal: String?,
        val searchScrollCurrent: String?,
        val priSearchScrollCurrent: String?,
        val searchScrollTime: String?,
        val priSearchScrollTime: String?,
        val searchScrollTotal: String?,
        val priSearchScrollTotal: String?,
        val searchPointInTimeCurrent: String?,
        val priSearchPointInTimeCurrent: String?,
        val searchPointInTimeTime: String?,
        val priSearchPointInTimeTime: String?,
        val searchPointInTimeTotal: String?,
        val priSearchPointInTimeTotal: String?,
        val segmentsCount: String?,
        val priSegmentsCount: String?,
        val segmentsMemory: String?,
        val priSegmentsMemory: String?,
        val segmentsIndexWriterMemory: String?,
        val priSegmentsIndexWriterMemory: String?,
        val segmentsVersionMapMemory: String?,
        val priSegmentsVersionMapMemory: String?,
        val segmentsFixedBitsetMemory: String?,
        val priSegmentsFixedBitsetMemory: String?,
        val warmerCurrent: String?,
        val priWarmerCurrent: String?,
        val warmerTotal: String?,
        val priWarmerTotal: String?,
        val warmerTotalTime: String?,
        val priWarmerTotalTime: String?,
        val suggestCurrent: String?,
        val priSuggestCurrent: String?,
        val suggestTime: String?,
        val priSuggestTime: String?,
        val suggestTotal: String?,
        val priSuggestTotal: String?,
        val memoryTotal: String?,
        val priMemoryTotal: String?,
        val searchThrottled: String?
    ) : ToXContentObject, Writeable {
        companion object {
            const val HEALTH_FIELD = "health"
            const val STATUS_FIELD = "status"
            const val INDEX_FIELD = "index"
            const val UUID_FIELD = "uuid"
            const val PRI_FIELD = "pri"
            const val REP_FIELD = "rep"
            const val DOCS_COUNT_FIELD = "docs.count"
            const val DOCS_DELETED_FIELD = "docs.deleted"
            const val CREATION_DATE_FIELD = "creation.date"
            const val CREATION_DATE_STRING_FIELD = "creation.date.string"
            const val STORE_SIZE_FIELD = "store.size"
            const val PRI_STORE_SIZE_FIELD = "pri.store.size"
            const val COMPLETION_SIZE_FIELD = "completion.size"
            const val PRI_COMPLETION_SIZE_FIELD = "pri.completion.size"
            const val FIELD_DATA_MEMORY_SIZE_FIELD = "fielddata.memory_size"
            const val PRI_FIELD_DATA_MEMORY_SIZE_FIELD = "pri.fielddata.memory_size"
            const val FIELD_DATA_EVICTIONS_FIELD = "fielddata.evictions"
            const val PRI_FIELD_DATA_EVICTIONS_FIELD = "pri.fielddata.evictions"
            const val QUERY_CACHE_MEMORY_SIZE_FIELD = "query_cache.memory_size"
            const val PRI_QUERY_CACHE_MEMORY_SIZE_FIELD = "pri.query_cache.memory_size"
            const val QUERY_CACHE_EVICTIONS_FIELD = "query_cache.evictions"
            const val PRI_QUERY_CACHE_EVICTIONS_FIELD = "pri.query_cache.evictions"
            const val REQUEST_CACHE_MEMORY_SIZE_FIELD = "request_cache.memory_size"
            const val PRI_REQUEST_CACHE_MEMORY_SIZE_FIELD = "pri.request_cache.memory_size"
            const val REQUEST_CACHE_EVICTIONS_FIELD = "request_cache.evictions"
            const val PRI_REQUEST_CACHE_EVICTIONS_FIELD = "pri.request_cache.evictions"
            const val REQUEST_CACHE_HIT_COUNT_FIELD = "request_cache.hit_count"
            const val PRI_REQUEST_CACHE_HIT_COUNT_FIELD = "pri.request_cache.hit_count"
            const val REQUEST_CACHE_MISS_COUNT_FIELD = "request_cache.miss_count"
            const val PRI_REQUEST_CACHE_MISS_COUNT_FIELD = "pri.request_cache.miss_count"
            const val FLUSH_TOTAL_FIELD = "flush.total"
            const val PRI_FLUSH_TOTAL_FIELD = "pri.flush.total"
            const val FLUSH_TOTAL_TIME_FIELD = "flush.total_time"
            const val PRI_FLUSH_TOTAL_TIME_FIELD = "pri.flush.total_time"
            const val GET_CURRENT_FIELD = "get.current"
            const val PRI_GET_CURRENT_FIELD = "pri.get.current"
            const val GET_TIME_FIELD = "get.time"
            const val PRI_GET_TIME_FIELD = "pri.get.time"
            const val GET_TOTAL_FIELD = "get.total"
            const val PRI_GET_TOTAL_FIELD = "pri.get.total"
            const val GET_EXISTS_TIME_FIELD = "get.exists_time"
            const val PRI_GET_EXISTS_TIME_FIELD = "pri.get.exists_time"
            const val GET_EXISTS_TOTAL_FIELD = "get.exists_total"
            const val PRI_GET_EXISTS_TOTAL_FIELD = "pri.get.exists_total"
            const val GET_MISSING_TIME_FIELD = "get.missing_time"
            const val PRI_GET_MISSING_TIME_FIELD = "pri.get.missing_time"
            const val GET_MISSING_TOTAL_FIELD = "get.missing_total"
            const val PRI_GET_MISSING_TOTAL_FIELD = "pri.get.missing_total"
            const val INDEXING_DELETE_CURRENT_FIELD = "indexing.delete_current"
            const val PRI_INDEXING_DELETE_CURRENT_FIELD = "pri.indexing.delete_current"
            const val INDEXING_DELETE_TIME_FIELD = "indexing.delete_time"
            const val PRI_INDEXING_DELETE_TIME_FIELD = "pri.indexing.delete_time"
            const val INDEXING_DELETE_TOTAL_FIELD = "indexing.delete_total"
            const val PRI_INDEXING_DELETE_TOTAL_FIELD = "pri.indexing.delete_total"
            const val INDEXING_INDEX_CURRENT_FIELD = "indexing.index_current"
            const val PRI_INDEXING_INDEX_CURRENT_FIELD = "pri.indexing.index_current"
            const val INDEXING_INDEX_TIME_FIELD = "indexing.index_time"
            const val PRI_INDEXING_INDEX_TIME_FIELD = "pri.indexing.index_time"
            const val INDEXING_INDEX_TOTAL_FIELD = "indexing.index_total"
            const val PRI_INDEXING_INDEX_TOTAL_FIELD = "pri.indexing.index_total"
            const val INDEXING_INDEX_FAILED_FIELD = "indexing.index_failed"
            const val PRI_INDEXING_INDEX_FAILED_FIELD = "pri.indexing.index_failed"
            const val MERGES_CURRENT_FIELD = "merges.current"
            const val PRI_MERGES_CURRENT_FIELD = "pri.merges.current"
            const val MERGES_CURRENT_DOCS_FIELD = "merges.current_docs"
            const val PRI_MERGES_CURRENT_DOCS_FIELD = "pri.merges.current_docs"
            const val MERGES_CURRENT_SIZE_FIELD = "merges.current_size"
            const val PRI_MERGES_CURRENT_SIZE_FIELD = "pri.merges.current_size"
            const val MERGES_TOTAL_FIELD = "merges.total"
            const val PRI_MERGES_TOTAL_FIELD = "pri.merges.total"
            const val MERGES_TOTAL_DOCS_FIELD = "merges.total_docs"
            const val PRI_MERGES_TOTAL_DOCS_FIELD = "pri.merges.total_docs"
            const val MERGES_TOTAL_SIZE_FIELD = "merges.total_size"
            const val PRI_MERGES_TOTAL_SIZE_FIELD = "pri.merges.total_size"
            const val MERGES_TOTAL_TIME_FIELD = "merges.total_time"
            const val PRI_MERGES_TOTAL_TIME_FIELD = "pri.merges.total_time"
            const val REFRESH_TOTAL_FIELD = "refresh.total"
            const val PRI_REFRESH_TOTAL_FIELD = "pri.refresh.total"
            const val REFRESH_TIME_FIELD = "refresh.time"
            const val PRI_REFRESH_TIME_FIELD = "pri.refresh.time"
            const val REFRESH_EXTERNAL_TOTAL_FIELD = "refresh.external_total"
            const val PRI_REFRESH_EXTERNAL_TOTAL_FIELD = "pri.refresh.external_total"
            const val REFRESH_EXTERNAL_TIME_FIELD = "refresh.external_time"
            const val PRI_REFRESH_EXTERNAL_TIME_FIELD = "pri.refresh.external_time"
            const val REFRESH_LISTENERS_FIELD = "refresh.listeners"
            const val PRI_REFRESH_LISTENERS_FIELD = "pri.refresh.listeners"
            const val SEARCH_FETCH_CURRENT_FIELD = "search.fetch_current"
            const val PRI_SEARCH_FETCH_CURRENT_FIELD = "pri.search.fetch_current"
            const val SEARCH_FETCH_TIME_FIELD = "search.fetch_time"
            const val PRI_SEARCH_FETCH_TIME_FIELD = "pri.search.fetch_time"
            const val SEARCH_FETCH_TOTAL_FIELD = "search.fetch_total"
            const val PRI_SEARCH_FETCH_TOTAL_FIELD = "pri.search.fetch_total"
            const val SEARCH_OPEN_CONTEXTS_FIELD = "search.open_contexts"
            const val PRI_SEARCH_OPEN_CONTEXTS_FIELD = "pri.search.open_contexts"
            const val SEARCH_QUERY_CURRENT_FIELD = "search.query_current"
            const val PRI_SEARCH_QUERY_CURRENT_FIELD = "pri.search.query_current"
            const val SEARCH_QUERY_TIME_FIELD = "search.query_time"
            const val PRI_SEARCH_QUERY_TIME_FIELD = "pri.search.query_time"
            const val SEARCH_QUERY_TOTAL_FIELD = "search.query_total"
            const val PRI_SEARCH_QUERY_TOTAL_FIELD = "pri.search.query_total"
            const val SEARCH_SCROLL_CURRENT_FIELD = "search.scroll_current"
            const val PRI_SEARCH_SCROLL_CURRENT_FIELD = "pri.search.scroll_current"
            const val SEARCH_SCROLL_TIME_FIELD = "search.scroll_time"
            const val PRI_SEARCH_SCROLL_TIME_FIELD = "pri.search.scroll_time"
            const val SEARCH_SCROLL_TOTAL_FIELD = "search.scroll_total"
            const val PRI_SEARCH_SCROLL_TOTAL_FIELD = "pri.search.scroll_total"
            const val SEARCH_POINT_IN_TIME_CURRENT_FIELD = "search.point_in_time_current"
            const val PRI_SEARCH_POINT_IN_TIME_CURRENT_FIELD = "pri.search.point_in_time_current"
            const val SEARCH_POINT_IN_TIME_TIME_FIELD = "search.point_in_time_time"
            const val PRI_SEARCH_POINT_IN_TIME_TIME_FIELD = "pri.search.point_in_time_time"
            const val SEARCH_POINT_IN_TIME_TOTAL_FIELD = "search.point_in_time_total"
            const val PRI_SEARCH_POINT_IN_TIME_TOTAL_FIELD = "pri.search.point_in_time_total"
            const val SEGMENTS_COUNT_FIELD = "segments.count"
            const val PRI_SEGMENTS_COUNT_FIELD = "pri.segments.count"
            const val SEGMENTS_MEMORY_FIELD = "segments.memory"
            const val PRI_SEGMENTS_MEMORY_FIELD = "pri.segments.memory"
            const val SEGMENTS_INDEX_WRITER_MEMORY_FIELD = "segments.index_writer_memory"
            const val PRI_SEGMENTS_INDEX_WRITER_MEMORY_FIELD = "pri.segments.index_writer_memory"
            const val SEGMENTS_VERSION_MAP_MEMORY_FIELD = "segments.version_map_memory"
            const val PRI_SEGMENTS_VERSION_MAP_MEMORY_FIELD = "pri.segments.version_map_memory"
            const val SEGMENTS_FIXED_BITSET_MEMORY_FIELD = "segments.fixed_bitset_memory"
            const val PRI_SEGMENTS_FIXED_BITSET_MEMORY_FIELD = "pri.segments.fixed_bitset_memory"
            const val WARMER_CURRENT_FIELD = "warmer.current"
            const val PRI_WARMER_CURRENT_FIELD = "pri.warmer.current"
            const val WARMER_TOTAL_FIELD = "warmer.total"
            const val PRI_WARMER_TOTAL_FIELD = "pri.warmer.total"
            const val WARMER_TOTAL_TIME_FIELD = "warmer.total_time"
            const val PRI_WARMER_TOTAL_TIME_FIELD = "pri.warmer.total_time"
            const val SUGGEST_CURRENT_FIELD = "suggest.current"
            const val PRI_SUGGEST_CURRENT_FIELD = "pri.suggest.current"
            const val SUGGEST_TIME_FIELD = "suggest.time"
            const val PRI_SUGGEST_TIME_FIELD = "pri.suggest.time"
            const val SUGGEST_TOTAL_FIELD = "suggest.total"
            const val PRI_SUGGEST_TOTAL_FIELD = "pri.suggest.total"
            const val MEMORY_TOTAL_FIELD = "memory.total"
            const val PRI_MEMORY_TOTAL_FIELD = "pri.memory.total"
            const val SEARCH_THROTTLED_FIELD = "search.throttled"
        }

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            builder.startObject()
                .field(HEALTH_FIELD, health)
                .field(STATUS_FIELD, status)
                .field(INDEX_FIELD, index)
                .field(UUID_FIELD, uuid)
                .field(PRI_FIELD, pri)
                .field(REP_FIELD, rep)
                .field(DOCS_COUNT_FIELD, docsCount)
                .field(DOCS_DELETED_FIELD, docsDeleted)
                .field(CREATION_DATE_FIELD, creationDate)
                .field(CREATION_DATE_STRING_FIELD, creationDateString)
                .field(STORE_SIZE_FIELD, storeSize)
                .field(PRI_STORE_SIZE_FIELD, priStoreSize)
                .field(COMPLETION_SIZE_FIELD, completionSize)
                .field(PRI_COMPLETION_SIZE_FIELD, priCompletionSize)
                .field(FIELD_DATA_MEMORY_SIZE_FIELD, fieldDataMemorySize)
                .field(PRI_FIELD_DATA_MEMORY_SIZE_FIELD, priFieldDataMemorySize)
                .field(FIELD_DATA_EVICTIONS_FIELD, fieldDataEvictions)
                .field(PRI_FIELD_DATA_EVICTIONS_FIELD, priFieldDataEvictions)
                .field(QUERY_CACHE_MEMORY_SIZE_FIELD, queryCacheMemorySize)
                .field(PRI_QUERY_CACHE_MEMORY_SIZE_FIELD, priQueryCacheMemorySize)
                .field(QUERY_CACHE_EVICTIONS_FIELD, queryCacheEvictions)
                .field(PRI_QUERY_CACHE_EVICTIONS_FIELD, priQueryCacheEvictions)
                .field(REQUEST_CACHE_MEMORY_SIZE_FIELD, requestCacheMemorySize)
                .field(PRI_REQUEST_CACHE_MEMORY_SIZE_FIELD, priRequestCacheMemorySize)
                .field(REQUEST_CACHE_EVICTIONS_FIELD, requestCacheEvictions)
                .field(PRI_REQUEST_CACHE_EVICTIONS_FIELD, priRequestCacheEvictions)
                .field(REQUEST_CACHE_HIT_COUNT_FIELD, requestCacheHitCount)
                .field(PRI_REQUEST_CACHE_HIT_COUNT_FIELD, priRequestCacheHitCount)
                .field(REQUEST_CACHE_MISS_COUNT_FIELD, requestCacheMissCount)
                .field(PRI_REQUEST_CACHE_MISS_COUNT_FIELD, priRequestCacheMissCount)
                .field(FLUSH_TOTAL_FIELD, flushTotal)
                .field(PRI_FLUSH_TOTAL_FIELD, priFlushTotal)
                .field(FLUSH_TOTAL_TIME_FIELD, flushTotalTime)
                .field(PRI_FLUSH_TOTAL_TIME_FIELD, priFlushTotalTime)
                .field(GET_CURRENT_FIELD, getCurrent)
                .field(PRI_GET_CURRENT_FIELD, priGetCurrent)
                .field(GET_TIME_FIELD, getTime)
                .field(PRI_GET_TIME_FIELD, priGetTime)
                .field(GET_TOTAL_FIELD, getTotal)
                .field(PRI_GET_TOTAL_FIELD, priGetTotal)
                .field(GET_EXISTS_TIME_FIELD, getExistsTime)
                .field(PRI_GET_EXISTS_TIME_FIELD, priGetExistsTime)
                .field(GET_EXISTS_TOTAL_FIELD, getExistsTotal)
                .field(PRI_GET_EXISTS_TOTAL_FIELD, priGetExistsTotal)
                .field(GET_MISSING_TIME_FIELD, getMissingTime)
                .field(PRI_GET_MISSING_TIME_FIELD, priGetMissingTime)
                .field(GET_MISSING_TOTAL_FIELD, getMissingTotal)
                .field(PRI_GET_MISSING_TOTAL_FIELD, priGetMissingTotal)
                .field(INDEXING_DELETE_CURRENT_FIELD, indexingDeleteCurrent)
                .field(PRI_INDEXING_DELETE_CURRENT_FIELD, priIndexingDeleteCurrent)
                .field(INDEXING_DELETE_TIME_FIELD, indexingDeleteTime)
                .field(PRI_INDEXING_DELETE_TIME_FIELD, priIndexingDeleteTime)
                .field(INDEXING_DELETE_TOTAL_FIELD, indexingDeleteTotal)
                .field(PRI_INDEXING_DELETE_TOTAL_FIELD, priIndexingDeleteTotal)
                .field(INDEXING_INDEX_CURRENT_FIELD, indexingIndexCurrent)
                .field(PRI_INDEXING_INDEX_CURRENT_FIELD, priIndexingIndexCurrent)
                .field(INDEXING_INDEX_TIME_FIELD, indexingIndexTime)
                .field(PRI_INDEXING_INDEX_TIME_FIELD, priIndexingIndexTime)
                .field(INDEXING_INDEX_TOTAL_FIELD, indexingIndexTotal)
                .field(PRI_INDEXING_INDEX_TOTAL_FIELD, priIndexingIndexTotal)
                .field(INDEXING_INDEX_FAILED_FIELD, indexingIndexFailed)
                .field(PRI_INDEXING_INDEX_FAILED_FIELD, priIndexingIndexFailed)
                .field(MERGES_CURRENT_FIELD, mergesCurrent)
                .field(PRI_MERGES_CURRENT_FIELD, priMergesCurrent)
                .field(MERGES_CURRENT_DOCS_FIELD, mergesCurrentDocs)
                .field(PRI_MERGES_CURRENT_DOCS_FIELD, priMergesCurrentDocs)
                .field(MERGES_CURRENT_SIZE_FIELD, mergesCurrentSize)
                .field(PRI_MERGES_CURRENT_SIZE_FIELD, priMergesCurrentSize)
                .field(MERGES_TOTAL_FIELD, mergesTotal)
                .field(PRI_MERGES_TOTAL_FIELD, priMergesTotal)
                .field(MERGES_TOTAL_DOCS_FIELD, mergesTotalDocs)
                .field(PRI_MERGES_TOTAL_DOCS_FIELD, priMergesTotalDocs)
                .field(MERGES_TOTAL_SIZE_FIELD, mergesTotalSize)
                .field(PRI_MERGES_TOTAL_SIZE_FIELD, priMergesTotalSize)
                .field(MERGES_TOTAL_TIME_FIELD, mergesTotalTime)
                .field(PRI_MERGES_TOTAL_TIME_FIELD, priMergesTotalTime)
                .field(REFRESH_TOTAL_FIELD, refreshTotal)
                .field(PRI_REFRESH_TOTAL_FIELD, priRefreshTotal)
                .field(REFRESH_TIME_FIELD, refreshTime)
                .field(PRI_REFRESH_TIME_FIELD, priRefreshTime)
                .field(REFRESH_EXTERNAL_TOTAL_FIELD, refreshExternalTotal)
                .field(PRI_REFRESH_EXTERNAL_TOTAL_FIELD, priRefreshExternalTotal)
                .field(REFRESH_EXTERNAL_TIME_FIELD, refreshExternalTime)
                .field(PRI_REFRESH_EXTERNAL_TIME_FIELD, priRefreshExternalTime)
                .field(REFRESH_LISTENERS_FIELD, refreshListeners)
                .field(PRI_REFRESH_LISTENERS_FIELD, priRefreshListeners)
                .field(SEARCH_FETCH_CURRENT_FIELD, searchFetchCurrent)
                .field(PRI_SEARCH_FETCH_CURRENT_FIELD, priSearchFetchCurrent)
                .field(SEARCH_FETCH_TIME_FIELD, searchFetchTime)
                .field(PRI_SEARCH_FETCH_TIME_FIELD, priSearchFetchTime)
                .field(SEARCH_FETCH_TOTAL_FIELD, searchFetchTotal)
                .field(PRI_SEARCH_FETCH_TOTAL_FIELD, priSearchFetchTotal)
                .field(SEARCH_OPEN_CONTEXTS_FIELD, searchOpenContexts)
                .field(PRI_SEARCH_OPEN_CONTEXTS_FIELD, priSearchOpenContexts)
                .field(SEARCH_QUERY_CURRENT_FIELD, searchQueryCurrent)
                .field(PRI_SEARCH_QUERY_CURRENT_FIELD, priSearchQueryCurrent)
                .field(SEARCH_QUERY_TIME_FIELD, searchQueryTime)
                .field(PRI_SEARCH_QUERY_TIME_FIELD, priSearchQueryTime)
                .field(SEARCH_QUERY_TOTAL_FIELD, searchQueryTotal)
                .field(PRI_SEARCH_QUERY_TOTAL_FIELD, priSearchQueryTotal)
                .field(SEARCH_SCROLL_CURRENT_FIELD, searchScrollCurrent)
                .field(PRI_SEARCH_SCROLL_CURRENT_FIELD, priSearchScrollCurrent)
                .field(SEARCH_SCROLL_TIME_FIELD, searchScrollTime)
                .field(PRI_SEARCH_SCROLL_TIME_FIELD, priSearchScrollTime)
                .field(SEARCH_SCROLL_TOTAL_FIELD, searchScrollTotal)
                .field(PRI_SEARCH_SCROLL_TOTAL_FIELD, priSearchScrollTotal)
                .field(SEARCH_POINT_IN_TIME_CURRENT_FIELD, searchPointInTimeCurrent)
                .field(PRI_SEARCH_POINT_IN_TIME_CURRENT_FIELD, priSearchPointInTimeCurrent)
                .field(SEARCH_POINT_IN_TIME_TIME_FIELD, searchPointInTimeTime)
                .field(PRI_SEARCH_POINT_IN_TIME_TIME_FIELD, priSearchPointInTimeTime)
                .field(SEARCH_POINT_IN_TIME_TOTAL_FIELD, searchPointInTimeTotal)
                .field(PRI_SEARCH_POINT_IN_TIME_TOTAL_FIELD, priSearchPointInTimeTotal)
                .field(SEGMENTS_COUNT_FIELD, segmentsCount)
                .field(PRI_SEGMENTS_COUNT_FIELD, priSegmentsCount)
                .field(SEGMENTS_MEMORY_FIELD, segmentsMemory)
                .field(PRI_SEGMENTS_MEMORY_FIELD, priSegmentsMemory)
                .field(SEGMENTS_INDEX_WRITER_MEMORY_FIELD, segmentsIndexWriterMemory)
                .field(PRI_SEGMENTS_INDEX_WRITER_MEMORY_FIELD, priSegmentsIndexWriterMemory)
                .field(SEGMENTS_VERSION_MAP_MEMORY_FIELD, segmentsVersionMapMemory)
                .field(PRI_SEGMENTS_VERSION_MAP_MEMORY_FIELD, priSegmentsVersionMapMemory)
                .field(SEGMENTS_FIXED_BITSET_MEMORY_FIELD, segmentsFixedBitsetMemory)
                .field(PRI_SEGMENTS_FIXED_BITSET_MEMORY_FIELD, priSegmentsFixedBitsetMemory)
                .field(WARMER_CURRENT_FIELD, warmerCurrent)
                .field(PRI_WARMER_CURRENT_FIELD, priWarmerCurrent)
                .field(WARMER_TOTAL_FIELD, warmerTotal)
                .field(PRI_WARMER_TOTAL_FIELD, priWarmerTotal)
                .field(WARMER_TOTAL_TIME_FIELD, warmerTotalTime)
                .field(PRI_WARMER_TOTAL_TIME_FIELD, priWarmerTotalTime)
                .field(SUGGEST_CURRENT_FIELD, suggestCurrent)
                .field(PRI_SUGGEST_CURRENT_FIELD, priSuggestCurrent)
                .field(SUGGEST_TIME_FIELD, suggestTime)
                .field(PRI_SUGGEST_TIME_FIELD, priSuggestTime)
                .field(SUGGEST_TOTAL_FIELD, suggestTotal)
                .field(PRI_SUGGEST_TOTAL_FIELD, priSuggestTotal)
                .field(MEMORY_TOTAL_FIELD, memoryTotal)
                .field(PRI_MEMORY_TOTAL_FIELD, priMemoryTotal)
                .field(SEARCH_THROTTLED_FIELD, searchThrottled)
            return builder.endObject()
        }

        override fun writeTo(out: StreamOutput) {
            out.writeString(health)
            out.writeString(status)
            out.writeString(index)
            out.writeString(uuid)
            out.writeString(pri)
            out.writeString(rep)
            out.writeString(docsCount)
            out.writeString(docsDeleted)
            out.writeString(creationDate)
            out.writeString(creationDateString)
            out.writeString(storeSize)
            out.writeString(priStoreSize)
            out.writeString(completionSize)
            out.writeString(priCompletionSize)
            out.writeString(fieldDataMemorySize)
            out.writeString(priFieldDataMemorySize)
            out.writeString(fieldDataEvictions)
            out.writeString(priFieldDataEvictions)
            out.writeString(queryCacheMemorySize)
            out.writeString(priQueryCacheMemorySize)
            out.writeString(queryCacheEvictions)
            out.writeString(priQueryCacheEvictions)
            out.writeString(requestCacheMemorySize)
            out.writeString(priRequestCacheMemorySize)
            out.writeString(requestCacheEvictions)
            out.writeString(priRequestCacheEvictions)
            out.writeString(requestCacheHitCount)
            out.writeString(priRequestCacheHitCount)
            out.writeString(requestCacheMissCount)
            out.writeString(priRequestCacheMissCount)
            out.writeString(flushTotal)
            out.writeString(priFlushTotal)
            out.writeString(flushTotalTime)
            out.writeString(priFlushTotalTime)
            out.writeString(getCurrent)
            out.writeString(priGetCurrent)
            out.writeString(getTime)
            out.writeString(priGetTime)
            out.writeString(getTotal)
            out.writeString(priGetTotal)
            out.writeString(getExistsTime)
            out.writeString(priGetExistsTime)
            out.writeString(getExistsTotal)
            out.writeString(priGetExistsTotal)
            out.writeString(getMissingTime)
            out.writeString(priGetMissingTime)
            out.writeString(getMissingTotal)
            out.writeString(priGetMissingTotal)
            out.writeString(indexingDeleteCurrent)
            out.writeString(priIndexingDeleteCurrent)
            out.writeString(indexingDeleteTime)
            out.writeString(priIndexingDeleteTime)
            out.writeString(indexingDeleteTotal)
            out.writeString(priIndexingDeleteTotal)
            out.writeString(indexingIndexCurrent)
            out.writeString(priIndexingIndexCurrent)
            out.writeString(indexingIndexTime)
            out.writeString(priIndexingIndexTime)
            out.writeString(indexingIndexTotal)
            out.writeString(priIndexingIndexTotal)
            out.writeString(indexingIndexFailed)
            out.writeString(priIndexingIndexFailed)
            out.writeString(mergesCurrent)
            out.writeString(priMergesCurrent)
            out.writeString(mergesCurrentDocs)
            out.writeString(priMergesCurrentDocs)
            out.writeString(mergesCurrentSize)
            out.writeString(priMergesCurrentSize)
            out.writeString(mergesTotal)
            out.writeString(priMergesTotal)
            out.writeString(mergesTotalDocs)
            out.writeString(priMergesTotalDocs)
            out.writeString(mergesTotalSize)
            out.writeString(priMergesTotalSize)
            out.writeString(mergesTotalTime)
            out.writeString(priMergesTotalTime)
            out.writeString(refreshTotal)
            out.writeString(priRefreshTotal)
            out.writeString(refreshTime)
            out.writeString(priRefreshTime)
            out.writeString(refreshExternalTotal)
            out.writeString(priRefreshExternalTotal)
            out.writeString(refreshExternalTime)
            out.writeString(priRefreshExternalTime)
            out.writeString(refreshListeners)
            out.writeString(priRefreshListeners)
            out.writeString(searchFetchCurrent)
            out.writeString(priSearchFetchCurrent)
            out.writeString(searchFetchTime)
            out.writeString(priSearchFetchTime)
            out.writeString(searchFetchTotal)
            out.writeString(priSearchFetchTotal)
            out.writeString(searchOpenContexts)
            out.writeString(priSearchOpenContexts)
            out.writeString(searchQueryCurrent)
            out.writeString(priSearchQueryCurrent)
            out.writeString(searchQueryTime)
            out.writeString(priSearchQueryTime)
            out.writeString(searchQueryTotal)
            out.writeString(priSearchQueryTotal)
            out.writeString(searchScrollCurrent)
            out.writeString(priSearchScrollCurrent)
            out.writeString(searchScrollTime)
            out.writeString(priSearchScrollTime)
            out.writeString(searchScrollTotal)
            out.writeString(priSearchScrollTotal)
            out.writeString(searchPointInTimeCurrent)
            out.writeString(priSearchPointInTimeCurrent)
            out.writeString(searchPointInTimeTime)
            out.writeString(priSearchPointInTimeTime)
            out.writeString(searchPointInTimeTotal)
            out.writeString(priSearchPointInTimeTotal)
            out.writeString(segmentsCount)
            out.writeString(priSegmentsCount)
            out.writeString(segmentsMemory)
            out.writeString(priSegmentsMemory)
            out.writeString(segmentsIndexWriterMemory)
            out.writeString(priSegmentsIndexWriterMemory)
            out.writeString(segmentsVersionMapMemory)
            out.writeString(priSegmentsVersionMapMemory)
            out.writeString(segmentsFixedBitsetMemory)
            out.writeString(priSegmentsFixedBitsetMemory)
            out.writeString(warmerCurrent)
            out.writeString(priWarmerCurrent)
            out.writeString(warmerTotal)
            out.writeString(priWarmerTotal)
            out.writeString(warmerTotalTime)
            out.writeString(priWarmerTotalTime)
            out.writeString(suggestCurrent)
            out.writeString(priSuggestCurrent)
            out.writeString(suggestTime)
            out.writeString(priSuggestTime)
            out.writeString(suggestTotal)
            out.writeString(priSuggestTotal)
            out.writeString(memoryTotal)
            out.writeString(priMemoryTotal)
            out.writeString(searchThrottled)
        }
    }
}
