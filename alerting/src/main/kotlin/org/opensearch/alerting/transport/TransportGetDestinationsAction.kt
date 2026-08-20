/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.ResourceSharingUtils
import org.opensearch.alerting.action.GetDestinationsAction
import org.opensearch.alerting.action.GetDestinationsRequest
import org.opensearch.alerting.action.GetDestinationsResponse
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.opensearchapi.addFilter
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.PluginClient
import org.opensearch.alerting.util.use
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.authuser.User
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.Strings
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.Operator
import org.opensearch.index.query.QueryBuilders
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.remote.metadata.client.SearchDataObjectRequest
import org.opensearch.remote.metadata.common.SdkClientUtils
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.search.sort.SortBuilders
import org.opensearch.search.sort.SortOrder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.io.IOException
private val log = LogManager.getLogger(TransportGetDestinationsAction::class.java)

class TransportGetDestinationsAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    clusterService: ClusterService,
    actionFilters: ActionFilters,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
    val sdkClient: SdkClient,
    private val pluginClient: PluginClient
) : HandledTransportAction<GetDestinationsRequest, GetDestinationsResponse> (
    GetDestinationsAction.NAME, transportService, actionFilters, ::GetDestinationsRequest
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    @Volatile override var filterByAccessStrategy = AlertingSettings.FILTER_BY_BACKEND_ROLES_ACCESS_STRATEGY.get(settings)

    private val multiTenancyEnabled = AlertingSettings.MULTI_TENANCY_ENABLED.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(
        task: Task,
        getDestinationsRequest: GetDestinationsRequest,
        actionListener: ActionListener<GetDestinationsResponse>
    ) {
        if (multiTenancyEnabled) {
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException(
                        "Destination operations are not allowed when multi-tenancy is enabled.",
                        RestStatus.METHOD_NOT_ALLOWED
                    )
                )
            )
            return
        }

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

        val tenantId = client.threadPool().threadContext.getHeader(AlertingPlugin.TENANT_ID_HEADER)
        client.threadPool().threadContext.stashContext().use {
            resolve(searchSourceBuilder, actionListener, user, tenantId)
        }
    }

    fun resolve(
        searchSourceBuilder: SearchSourceBuilder,
        actionListener: ActionListener<GetDestinationsResponse>,
        user: User?,
        tenantId: String? = null,
    ) {
        val useRsc = ResourceSharingUtils.shouldUseResourceAuthz(ResourceSharingUtils.MONITOR_RESOURCE_TYPE)
        if (useRsc) {
            // resource sharing framework is enabled - access control handled by security plugin
            search(searchSourceBuilder, actionListener, tenantId)
        } else if (user == null) {
            search(searchSourceBuilder, actionListener, tenantId)
        } else if (!doFilterForUser(user)) {
            search(searchSourceBuilder, actionListener, tenantId)
        } else {
            try {
                log.info("Filtering result by: ${user.backendRoles}")
                addFilter(user, searchSourceBuilder, "destination.user.backend_roles.keyword")
                search(searchSourceBuilder, actionListener, tenantId)
            } catch (ex: IOException) {
                actionListener.onFailure(AlertingException.wrap(ex))
            }
        }
    }

    fun search(
        searchSourceBuilder: SearchSourceBuilder,
        actionListener: ActionListener<GetDestinationsResponse>,
        tenantId: String? = null,
    ) {
        // When resource sharing is enabled, route search through PluginClient so it runs as the plugin subject
        // and the security plugin's DLS on the shared-resource index can filter results.
        if (ResourceSharingUtils.shouldUseResourceAuthz(ResourceSharingUtils.MONITOR_RESOURCE_TYPE)) {
            val searchRequest = org.opensearch.action.search.SearchRequest()
                .indices(ScheduledJob.SCHEDULED_JOBS_INDEX)
                .source(searchSourceBuilder)
            pluginClient.search(
                searchRequest,
                object : ActionListener<org.opensearch.action.search.SearchResponse> {
                    override fun onResponse(response: org.opensearch.action.search.SearchResponse) {
                        try {
                            actionListener.onResponse(buildResponse(response))
                        } catch (e: Exception) {
                            actionListener.onFailure(AlertingException.wrap(e))
                        }
                    }
                    override fun onFailure(e: Exception) =
                        actionListener.onFailure(AlertingException.wrap(e))
                }
            )
            return
        }

        val sdkSearchRequest = SearchDataObjectRequest.builder()
            .indices(ScheduledJob.SCHEDULED_JOBS_INDEX)
            .tenantId(tenantId)
            .searchSourceBuilder(searchSourceBuilder)
            .build()

        sdkClient.searchDataObjectAsync(sdkSearchRequest).whenComplete { response, throwable ->
            if (throwable != null) {
                actionListener.onFailure(AlertingException.wrap(SdkClientUtils.unwrapAndConvertToException(throwable)))
                return@whenComplete
            }
            try {
                val searchResponse = response.searchResponse()
                if (searchResponse == null) {
                    actionListener.onResponse(GetDestinationsResponse(RestStatus.OK, 0, emptyList()))
                    return@whenComplete
                }
                actionListener.onResponse(buildResponse(searchResponse))
            } catch (e: Exception) {
                actionListener.onFailure(AlertingException.wrap(e))
            }
        }
    }

    private fun buildResponse(searchResponse: org.opensearch.action.search.SearchResponse): GetDestinationsResponse {
        val totalDestinationCount = searchResponse.hits.totalHits?.value?.toInt()
        val destinations = mutableListOf<Destination>()
        for (hit in searchResponse.hits) {
            val id = hit.id
            val version = hit.version
            val seqNo = hit.seqNo.toInt()
            val primaryTerm = hit.primaryTerm.toInt()
            val xcp = XContentType.JSON.xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, hit.sourceAsString)
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, xcp.nextToken(), xcp)
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
            destinations.add(Destination.parse(xcp, id, version, seqNo, primaryTerm))
        }
        return GetDestinationsResponse(RestStatus.OK, totalDestinationCount, destinations)
    }
}
