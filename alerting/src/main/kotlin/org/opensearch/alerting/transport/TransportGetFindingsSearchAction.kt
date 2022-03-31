/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.GetFindingsSearchAction
import org.opensearch.alerting.action.GetFindingsSearchRequest
import org.opensearch.alerting.action.GetFindingsSearchResponse
import org.opensearch.alerting.elasticapi.addFilter
import org.opensearch.alerting.model.Finding
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
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.search.sort.SortBuilders
import org.opensearch.search.sort.SortOrder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.io.IOException

private val log = LogManager.getLogger(TransportGetFindingsSearchAction::class.java)

class TransportGetFindingsSearchAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    clusterService: ClusterService,
    actionFilters: ActionFilters,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetFindingsSearchRequest, GetFindingsSearchResponse> (
    GetFindingsSearchAction.NAME, transportService, actionFilters, ::GetFindingsSearchRequest
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(
        task: Task,
        getFindingsSearchRequest: GetFindingsSearchRequest,
        actionListener: ActionListener<GetFindingsSearchResponse>
    ) {
        val user = readUserFromThreadContext(client)
        val tableProp = getFindingsSearchRequest.table

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
        val matchAllQueryBuilder = QueryBuilders.matchAllQuery()
        // TODO: Update query to support other parameters of search

        searchSourceBuilder.query(matchAllQueryBuilder)

        client.threadPool().threadContext.stashContext().use {
            resolve(searchSourceBuilder, actionListener, user)
        }
    }

    fun resolve(
        searchSourceBuilder: SearchSourceBuilder,
        actionListener: ActionListener<GetFindingsSearchResponse>,
        user: User?
    ) {
        if (user == null) {
            // user is null when: 1/ security is disabled. 2/when user is super-admin.
            search(searchSourceBuilder, actionListener)
        } else if (!doFilterForUser(user)) {
            // security is enabled and filterby is disabled.
            search(searchSourceBuilder, actionListener)
        } else {
            // security is enabled and filterby is enabled.
            try {
                log.info("Filtering result by: ${user.backendRoles}")
                addFilter(user, searchSourceBuilder, "finding.user.backend_roles.keyword")
                search(searchSourceBuilder, actionListener)
            } catch (ex: IOException) {
                actionListener.onFailure(AlertingException.wrap(ex))
            }
        }
    }

    fun search(searchSourceBuilder: SearchSourceBuilder, actionListener: ActionListener<GetFindingsSearchResponse>) {
        val searchRequest = SearchRequest()
            .source(searchSourceBuilder)
            .indices(".opensearch-alerting-findings")
        client.search(
            searchRequest,
            object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    val totalFindingCount = response.hits.totalHits?.value?.toInt()
                    val mgetRequest = MultiGetRequest()
                    val findingsWithDocs = mutableListOf<FindingWithDocs>()
                    for (hit in response.hits) {
                        val id = hit.id
                        val xcp = XContentFactory.xContent(XContentType.JSON)
                            .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, hit.sourceAsString)
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                        val finding = Finding.parse(xcp, id)
                        val documentIds = finding.relatedDocId.split(",").toTypedArray()
                        // Add getRequests to mget request
                        documentIds.forEach {
                            // TODO: check if we want to add individual get document request, or use documentIds array for a single finding related_docs
                            docId ->
                            mgetRequest.add(MultiGetRequest.Item(finding.index, docId))
                        }
                    }
                    searchDocument(mgetRequest, totalFindingCount, actionListener)
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(AlertingException.wrap(t))
                }
            }
        )
    }

    fun searchDocument(
        mgetRequest: MultiGetRequest,
        totalFindingCount: Int?,
        actionListener: ActionListener<GetFindingsSearchResponse>
    ) {
        client.multiGet(
            mgetRequest,
            object : ActionListener<MultiGetResponse> {
                override fun onResponse(response: MultiGetResponse) {
                    val findingsWithDocs: List<FindingWithDocs> = mutableListOf()
                    response.responses.forEach {
                        // TODO: REMOVE DEBUG LOG
                        log.info("response: ${response.toString()}")
                        /* val xcp = XContentFactory.xContent(XContentType.JSON)
                            .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, response.toString())
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                        val findingDocument = FindingDocument.parse(xcp)
                        */
                        // TODO: Parse the searched documents and add to list of findingWithDocs, need to associate original finding id to response
                    }
                    // TODO: Form the response here with the map/list of findings
                    actionListener.onResponse(GetFindingsSearchResponse(RestStatus.OK, totalFindingCount, findingsWithDocs))
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(AlertingException.wrap(t))
                }
            }
        )
    }
}
