/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.GetDestinationsAction
import org.opensearch.alerting.action.GetDestinationsRequest
import org.opensearch.alerting.action.GetDestinationsResponse
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertGetDestinationsRequestToGetNotificationConfigRequest
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertGetNotificationConfigResponseToGetDestinationsResponse
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.elasticapi.addFilter
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.NotificationAPIUtils
import org.opensearch.client.node.NodeClient
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
import org.opensearch.commons.ConfigConstants
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
    val client: NodeClient,
    clusterService: ClusterService,
    actionFilters: ActionFilters,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetDestinationsRequest, GetDestinationsResponse> (
    GetDestinationsAction.NAME, transportService, actionFilters, ::GetDestinationsRequest
) {

    @Volatile private var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.FILTER_BY_BACKEND_ROLES) { filterByEnabled = it }
    }

    override fun doExecute(
        task: Task,
        getDestinationsRequest: GetDestinationsRequest,
        actionListener: ActionListener<GetDestinationsResponse>
    ) {

        val getDestinationsResponse: GetDestinationsResponse
        try {
            val getRequest = convertGetDestinationsRequestToGetNotificationConfigRequest(getDestinationsRequest)
            val getNotificationConfigResponse = NotificationAPIUtils.getNotificationConfig(client, getRequest)
            getDestinationsResponse = convertGetNotificationConfigResponseToGetDestinationsResponse(getNotificationConfigResponse)
            if (getDestinationsRequest.destinationId != null) {
                if (getDestinationsResponse.destinations.isEmpty()) {
                    actionListener.onFailure(
                        OpenSearchStatusException(
                            "Destination ${getDestinationsRequest.destinationId} cannot be found",
                            RestStatus.NOT_FOUND
                        )
                    )
                } else {
                    actionListener.onResponse(getDestinationsResponse)
                }
                return
            }
        } catch (e: Exception) {
            actionListener.onFailure(AlertingException.wrap(e))
            return
        }

        val userStr = client.threadPool().threadContext.getTransient<String>(
            ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
        )
        log.debug("User and roles string from thread context: $userStr")
        val user: User? = User.parse(userStr)

        val tableProp = getDestinationsRequest.table

        val sortBuilder = SortBuilders
            .fieldSort(tableProp.sortString)
            .order(SortOrder.fromString(tableProp.sortOrder))
        if (!tableProp.missing.isNullOrBlank()) {
            sortBuilder.missing(tableProp.missing)
        }

        val searchSourceBuilder = SearchSourceBuilder()
            .sort(sortBuilder)
            .size(tableProp.size - getDestinationsResponse.destinations.size)
            .from(tableProp.startIndex)
            .fetchSource(FetchSourceContext(true, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY))
            .seqNoAndPrimaryTerm(true)
            .version(true)
        val queryBuilder = QueryBuilders.boolQuery()
            .must(QueryBuilders.existsQuery("destination"))

        if (!getDestinationsRequest.destinationId.isNullOrBlank())
            queryBuilder.filter(QueryBuilders.termQuery("_id", getDestinationsRequest.destinationId as String))

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
            resolve(searchSourceBuilder, getDestinationsResponse, actionListener, user)
        }
    }

    fun resolve(
        searchSourceBuilder: SearchSourceBuilder,
        getDestinationsResponse: GetDestinationsResponse,
        actionListener: ActionListener<GetDestinationsResponse>,
        user: User?
    ) {
        if (user == null) {
            // user is null when: 1/ security is disabled. 2/when user is super-admin.
            search(searchSourceBuilder, getDestinationsResponse, actionListener)
        } else if (!filterByEnabled) {
            // security is enabled and filterby is disabled.
            search(searchSourceBuilder, getDestinationsResponse, actionListener)
        } else {
            // security is enabled and filterby is enabled.
            try {
                log.info("Filtering result by: ${user.backendRoles}")
                addFilter(user, searchSourceBuilder, "destination.user.backend_roles.keyword")
                search(searchSourceBuilder, getDestinationsResponse, actionListener)
            } catch (ex: IOException) {
                actionListener.onFailure(AlertingException.wrap(ex))
            }
        }
    }

    fun search(
        searchSourceBuilder: SearchSourceBuilder,
        getDestinationsResponse: GetDestinationsResponse,
        actionListener: ActionListener<GetDestinationsResponse>
    ) {
        val searchRequest = SearchRequest()
            .source(searchSourceBuilder)
            .indices(ScheduledJob.SCHEDULED_JOBS_INDEX)
        client.search(
            searchRequest,
            object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    var totalDestinationCount = response.hits.totalHits?.value?.toInt() ?: 0
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
                        val destination = Destination.parse(xcp, id, version, seqNo, primaryTerm)
                        destinations.add(destination)
                    }
                    totalDestinationCount += getDestinationsResponse.totalDestinations ?: 0
                    destinations.addAll(getDestinationsResponse.destinations)
                    val getResponse = GetDestinationsResponse(
                        RestStatus.OK,
                        totalDestinationCount,
                        destinations
                    )
                    actionListener.onResponse(getResponse)
                }

                override fun onFailure(t: Exception) {
                    log.warn("Failed to get destinations from alerting config index", t)
                    actionListener.onResponse(getDestinationsResponse)
                }
            }
        )
    }
}
