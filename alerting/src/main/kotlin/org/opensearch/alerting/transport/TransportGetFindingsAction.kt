/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.apache.lucene.search.join.ScoreMode
import org.opensearch.action.ActionListener
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.GetFindingsAction
import org.opensearch.alerting.action.GetFindingsRequest
import org.opensearch.alerting.action.GetFindingsResponse
import org.opensearch.alerting.action.GetMonitorAction
import org.opensearch.alerting.action.GetMonitorRequest
import org.opensearch.alerting.action.GetMonitorResponse
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALL_FINDING_INDEX_PATTERN
import org.opensearch.alerting.model.Finding
import org.opensearch.alerting.model.FindingDocument
import org.opensearch.alerting.model.FindingWithDocs
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.Strings
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.query.Operator
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestRequest
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.search.sort.SortBuilders
import org.opensearch.search.sort.SortOrder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportGetFindingsSearchAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportGetFindingsSearchAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    clusterService: ClusterService,
    actionFilters: ActionFilters,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetFindingsRequest, GetFindingsResponse> (
    GetFindingsAction.NAME, transportService, actionFilters, ::GetFindingsRequest
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(
        task: Task,
        getFindingsRequest: GetFindingsRequest,
        actionListener: ActionListener<GetFindingsResponse>
    ) {
        val tableProp = getFindingsRequest.table

        val sortBuilder = SortBuilders
            .fieldSort(tableProp.sortString)
            .order(SortOrder.fromString(tableProp.sortOrder))
        if (!tableProp.missing.isNullOrBlank()) {
            sortBuilder.missing(tableProp.missing)
        }

        val searchSourceBuilder = SearchSourceBuilder()
            .sort(sortBuilder)
            .size(tableProp.size)
            .from(tableProp.startIndex)
            .fetchSource(FetchSourceContext(true, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY))
            .seqNoAndPrimaryTerm(true)
            .version(true)

        val queryBuilder = QueryBuilders.boolQuery()

        if (!getFindingsRequest.findingId.isNullOrBlank())
            queryBuilder.filter(QueryBuilders.termQuery("_id", getFindingsRequest.findingId))

        if (!tableProp.searchString.isNullOrBlank()) {
            queryBuilder
                .should(
                    QueryBuilders
                        .queryStringQuery(tableProp.searchString)
                )
                .should(
                    QueryBuilders.nestedQuery(
                        "queries",
                        QueryBuilders.boolQuery()
                            .must(
                                QueryBuilders
                                    .queryStringQuery(tableProp.searchString)
                                    .defaultOperator(Operator.AND)
                                    .field("queries.tags")
                                    .field("queries.name")
                            ),
                        ScoreMode.Avg
                    )
                )
        }

        searchSourceBuilder.query(queryBuilder)

        client.threadPool().threadContext.stashContext().use {
            scope.launch {
                try {
                    val indexName = resolveFindingsIndexName(getFindingsRequest)
                    val getFindingsResponse = search(searchSourceBuilder, indexName)
                    actionListener.onResponse(getFindingsResponse)
                } catch (t: AlertingException) {
                    actionListener.onFailure(t)
                } catch (t: Exception) {
                    actionListener.onFailure(AlertingException.wrap(t))
                }
            }
        }
    }

    suspend fun resolveFindingsIndexName(findingsRequest: GetFindingsRequest): String {
        var indexName = ALL_FINDING_INDEX_PATTERN

        if (findingsRequest.findingIndex.isNullOrEmpty() == false) {
            // findingIndex has highest priority, so use that if available
            indexName = findingsRequest.findingIndex
        } else if (findingsRequest.monitorId.isNullOrEmpty() == false) {
            // second best is monitorId.
            // We will use it to fetch monitor and then read indexName from dataSources field of monitor
            withContext(Dispatchers.IO) {
                val getMonitorRequest = GetMonitorRequest(
                    findingsRequest.monitorId,
                    -3L,
                    RestRequest.Method.GET,
                    FetchSourceContext.FETCH_SOURCE
                )
                val getMonitorResponse: GetMonitorResponse =
                    this@TransportGetFindingsSearchAction.client.suspendUntil {
                        execute(GetMonitorAction.INSTANCE, getMonitorRequest, it)
                    }
                indexName = getMonitorResponse.monitor?.dataSources?.findingsIndex ?: ALL_FINDING_INDEX_PATTERN
            }
        }
        return indexName
    }

    suspend fun search(searchSourceBuilder: SearchSourceBuilder, indexName: String): GetFindingsResponse {
        val searchRequest = SearchRequest()
            .source(searchSourceBuilder)
            .indices(indexName)
        val searchResponse: SearchResponse = client.suspendUntil { client.search(searchRequest, it) }
        val totalFindingCount = searchResponse.hits.totalHits?.value?.toInt()
        val mgetRequest = MultiGetRequest()
        val findingsWithDocs = mutableListOf<FindingWithDocs>()
        val findings = mutableListOf<Finding>()
        for (hit in searchResponse.hits) {
            val xcp = XContentFactory.xContent(XContentType.JSON)
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, hit.sourceAsString)
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
            val finding = Finding.parse(xcp)
            findings.add(finding)
            val documentIds = finding.relatedDocIds
            // Add getRequests to mget request
            documentIds.forEach { docId ->
                mgetRequest.add(MultiGetRequest.Item(finding.index, docId))
            }
        }
        val documents = if (mgetRequest.items.isEmpty()) mutableMapOf() else searchDocument(mgetRequest)
        findings.forEach {
            val documentIds = it.relatedDocIds
            val relatedDocs = mutableListOf<FindingDocument>()
            for (docId in documentIds) {
                val key = "${it.index}|$docId"
                documents[key]?.let { document -> relatedDocs.add(document) }
            }
            findingsWithDocs.add(FindingWithDocs(it, relatedDocs))
        }

        return GetFindingsResponse(searchResponse.status(), totalFindingCount, findingsWithDocs)
    }

    // TODO: Verify what happens if indices are closed/deleted
    suspend fun searchDocument(
        mgetRequest: MultiGetRequest
    ): Map<String, FindingDocument> {
        val response: MultiGetResponse = client.suspendUntil { client.multiGet(mgetRequest, it) }
        val documents: MutableMap<String, FindingDocument> = mutableMapOf()
        response.responses.forEach {
            val key = "${it.index}|${it.id}"
            val docData = if (it.isFailed) "" else it.response.sourceAsString
            val findingDocument = FindingDocument(it.index, it.id, !it.isFailed, docData)
            documents[key] = findingDocument
        }

        return documents
    }
}
