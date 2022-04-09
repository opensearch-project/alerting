/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.GetFindingsAction
import org.opensearch.alerting.action.GetFindingsRequest
import org.opensearch.alerting.action.GetFindingsResponse
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALL_FINDING_INDEX_PATTERN
import org.opensearch.alerting.model.Finding
import org.opensearch.alerting.model.FindingDocument
import org.opensearch.alerting.model.FindingWithDocs
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
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.search.sort.SortBuilders
import org.opensearch.search.sort.SortOrder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportGetFindingsSearchAction::class.java)

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
                .must(
                    QueryBuilders
                        .queryStringQuery(tableProp.searchString)
                        .defaultOperator(Operator.AND)
                        .field("queries.tags")
                        .field("queries.name")
                )
        }

        searchSourceBuilder.query(queryBuilder)

        client.threadPool().threadContext.stashContext().use {
            search(searchSourceBuilder, actionListener)
        }
    }

    fun search(searchSourceBuilder: SearchSourceBuilder, actionListener: ActionListener<GetFindingsResponse>) {
        val searchRequest = SearchRequest()
            .source(searchSourceBuilder)
            .indices(ALL_FINDING_INDEX_PATTERN)
        client.search(
            searchRequest,
            object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    val totalFindingCount = response.hits.totalHits?.value?.toInt()
                    val mgetRequest = MultiGetRequest()
                    val findingsWithDocs = mutableListOf<FindingWithDocs>()
                    val findings = mutableListOf<Finding>()
                    for (hit in response.hits) {
                        val xcp = XContentFactory.xContent(XContentType.JSON)
                            .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, hit.sourceAsString)
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                        val finding = Finding.parse(xcp)
                        findings.add(finding)
                        val documentIds = finding.relatedDocIds
                        // Add getRequests to mget request
                        documentIds.forEach {
                            docId ->
                            mgetRequest.add(MultiGetRequest.Item(finding.index, docId))
                        }
                    }
                    val documents = searchDocument(mgetRequest)
                    findings.forEach {
                        val documentIds = it.relatedDocIds
                        val relatedDocs = mutableListOf<FindingDocument>()
                        for (docId in documentIds) {
                            val key = "${it.index}|$docId"
                            documents[key]?.let { document -> relatedDocs.add(document) }
                        }
                        findingsWithDocs.add(FindingWithDocs(it, relatedDocs))
                    }
                    actionListener.onResponse(GetFindingsResponse(response.status(), totalFindingCount, findingsWithDocs))
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(AlertingException.wrap(t))
                }
            }
        )
    }

    // TODO: Verify what happens if indices are closed/deleted
    fun searchDocument(
        mgetRequest: MultiGetRequest
    ): Map<String, FindingDocument> {
        val response = client.multiGet(mgetRequest).actionGet()
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
