/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.GetDestinationsAction
import org.opensearch.alerting.action.GetDestinationsRequest
import org.opensearch.alerting.action.GetDestinationsResponse
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.opensearchapi.addFilter
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
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.Operator
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.search.sort.SortBuilders
import org.opensearch.search.sort.SortOrder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.io.IOException

private val log = LogManager.getLogger(TransportGetDestinationsAction::class.java)

class TransportGetDestinationsAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    clusterService: ClusterService,
    actionFilters: ActionFilters,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetDestinationsRequest, GetDestinationsResponse> (
    GetDestinationsAction.NAME, transportService, actionFilters, ::GetDestinationsRequest
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(
        task: Task,
        getDestinationsRequest: GetDestinationsRequest,
        actionListener: ActionListener<GetDestinationsResponse>
    ) {
        val user = readUserFromThreadContext(client)
        val tableProp = getDestinationsRequest.table

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
            .must(QueryBuilders.existsQuery("destination"))

        if (!getDestinationsRequest.destinationId.isNullOrBlank())
            queryBuilder.filter(QueryBuilders.termQuery("_id", getDestinationsRequest.destinationId))

        if (getDestinationsRequest.destinationType != "ALL")
            queryBuilder.filter(QueryBuilders.termQuery("destination.type", getDestinationsRequest.destinationType))

        if (!tableProp.searchString.isNullOrBlank()) {
            queryBuilder
                .must(
                    QueryBuilders
                        .queryStringQuery(tableProp.searchString)
                        .defaultOperator(Operator.AND)
                        .field("destination.type")
                        .field("destination.name")
                )
        }
        searchSourceBuilder.query(queryBuilder)

        client.threadPool().threadContext.stashContext().use {
            resolve(searchSourceBuilder, actionListener, user)
        }
    }

    fun resolve(
        searchSourceBuilder: SearchSourceBuilder,
        actionListener: ActionListener<GetDestinationsResponse>,
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
                addFilter(user, searchSourceBuilder, "destination.user.backend_roles.keyword")
                search(searchSourceBuilder, actionListener)
            } catch (ex: IOException) {
                actionListener.onFailure(AlertingException.wrap(ex))
            }
        }
    }

    fun search(searchSourceBuilder: SearchSourceBuilder, actionListener: ActionListener<GetDestinationsResponse>) {
        val searchRequest = SearchRequest()
            .source(searchSourceBuilder)
            .indices(ScheduledJob.SCHEDULED_JOBS_INDEX)
        client.search(
            searchRequest,
            object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    val totalDestinationCount = response.hits.totalHits?.value?.toInt()
                    val destinations = mutableListOf<Destination>()
                    for (hit in response.hits) {
                        val id = hit.id
                        val version = hit.version
                        val seqNo = hit.seqNo.toInt()
                        val primaryTerm = hit.primaryTerm.toInt()
                        val xcp = XContentFactory.xContent(XContentType.JSON)
                            .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, hit.sourceAsString)
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, xcp.nextToken(), xcp)
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                        destinations.add(Destination.parse(xcp, id, version, seqNo, primaryTerm))
                    }
                    actionListener.onResponse(GetDestinationsResponse(RestStatus.OK, totalDestinationCount, destinations))
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(AlertingException.wrap(t))
                }
            }
        )
    }
}
