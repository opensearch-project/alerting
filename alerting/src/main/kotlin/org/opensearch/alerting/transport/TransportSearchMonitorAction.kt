/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.apache.lucene.search.TotalHits
import org.apache.lucene.search.TotalHits.Relation
import org.opensearch.action.ActionRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.search.ShardSearchFailure
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.ResourceSharingUtils
import org.opensearch.alerting.opensearchapi.addFilter
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.PluginClient
import org.opensearch.alerting.util.use
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.SearchMonitorRequest
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.ExistsQueryBuilder
import org.opensearch.index.query.MatchQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.remote.metadata.client.SearchDataObjectRequest
import org.opensearch.remote.metadata.common.SdkClientUtils
import org.opensearch.search.SearchHits
import org.opensearch.search.aggregations.InternalAggregations
import org.opensearch.search.internal.InternalSearchResponse
import org.opensearch.search.profile.SearchProfileShardResults
import org.opensearch.search.suggest.Suggest
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.util.Collections
private val log = LogManager.getLogger(TransportSearchMonitorAction::class.java)

class TransportSearchMonitorAction @Inject constructor(
    transportService: TransportService,
    val settings: Settings,
    val client: Client,
    clusterService: ClusterService,
    actionFilters: ActionFilters,
    val namedWriteableRegistry: NamedWriteableRegistry,
    val xContentRegistry: NamedXContentRegistry,
    val sdkClient: SdkClient,
    private val pluginClient: PluginClient
) : HandledTransportAction<ActionRequest, SearchResponse>(
    AlertingActions.SEARCH_MONITORS_ACTION_NAME, transportService, actionFilters, ::SearchMonitorRequest
),
    SecureTransportAction {
    @Volatile
    override var filterByEnabled: Boolean = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    @Volatile
    override var filterByAccessStrategy: String = AlertingSettings.FILTER_BY_BACKEND_ROLES_ACCESS_STRATEGY.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: ActionRequest, actionListener: ActionListener<SearchResponse>) {
        val transformedRequest = request as? SearchMonitorRequest
            ?: recreateObject(request, namedWriteableRegistry) {
                SearchMonitorRequest(it)
            }

        val searchSourceBuilder = transformedRequest.searchRequest.source()
            .seqNoAndPrimaryTerm(true)
            .version(true)
        val queryBuilder = if (searchSourceBuilder.query() == null) BoolQueryBuilder()
        else QueryBuilders.boolQuery().must(searchSourceBuilder.query())

        // The SearchMonitor API supports one 'index' parameter of either the SCHEDULED_JOBS_INDEX or ALL_ALERT_INDEX_PATTERN.
        // When querying the ALL_ALERT_INDEX_PATTERN, we don't want to check whether the MONITOR_TYPE field exists
        // because we're querying alert indexes.
        if (transformedRequest.searchRequest.indices().contains(ScheduledJob.SCHEDULED_JOBS_INDEX)) {
            val monitorWorkflowType = QueryBuilders.boolQuery().should(QueryBuilders.existsQuery(Monitor.MONITOR_TYPE))
                .should(QueryBuilders.existsQuery(Workflow.WORKFLOW_TYPE))
            queryBuilder.must(monitorWorkflowType)
        }

        searchSourceBuilder.query(queryBuilder)
            .seqNoAndPrimaryTerm(true)
            .version(true)
        addOwnerFieldIfNotExists(transformedRequest.searchRequest)
        val user = readUserFromThreadContext(client)
        val tenantId = client.threadPool().threadContext.getHeader(AlertingPlugin.TENANT_ID_HEADER)
        client.threadPool().threadContext.stashContext().use {
            resolve(transformedRequest, actionListener, user, tenantId)
        }
    }

    fun resolve(
        searchMonitorRequest: SearchMonitorRequest,
        actionListener: ActionListener<SearchResponse>,
        user: User?,
        tenantId: String? = null,
    ) {
        // Only narrow (and thereby expose) backend roles when the caller opted in; otherwise the search response
        // is returned exactly as it is today, with the stored user dropped by the REST layer's secure serialization.
        val narrowingListener = if (searchMonitorRequest.includeBackendRoles) {
            narrowBackendRoles(user, actionListener)
        } else {
            actionListener
        }
        val useRsc = ResourceSharingUtils.shouldUseResourceAuthz(ResourceSharingUtils.MONITOR_RESOURCE_TYPE)
        if (useRsc) {
            // resource sharing is enabled - security plugin filters results at index layer
            search(searchMonitorRequest.searchRequest, narrowingListener, tenantId)
        } else if (user == null) {
            // user header is null when: 1/ security is disabled. 2/when user is super-admin.
            search(searchMonitorRequest.searchRequest, narrowingListener, tenantId)
        } else if (!doFilterForUser(user)) {
            // security is enabled and filterby is disabled.
            search(searchMonitorRequest.searchRequest, narrowingListener, tenantId)
        } else {
            // security is enabled and filterby is enabled.
            log.info("Filtering result by: ${user.backendRoles}")
            addFilter(user, searchMonitorRequest.searchRequest.source(), "monitor.user.backend_roles.keyword")
            search(searchMonitorRequest.searchRequest, narrowingListener, tenantId)
        }
    }

    /**
     * Rewrites each monitor hit so the user it carries holds only the backend roles the requester is entitled to
     * see. The rest of the user is left alone, so a caller reading a hit still sees the same fields it does today;
     * the REST layer writes out the backend roles and drops the rest.
     */
    private fun narrowBackendRoles(
        requester: User?,
        actionListener: ActionListener<SearchResponse>,
    ): ActionListener<SearchResponse> {
        return object : ActionListener<SearchResponse> {
            override fun onResponse(response: SearchResponse) {
                try {
                    for (hit in response.hits) {
                        val job = XContentType.JSON.xContent().createParser(
                            xContentRegistry,
                            LoggingDeprecationHandler.INSTANCE,
                            hit.sourceAsString
                        ).use { parser -> ScheduledJob.parse(parser, hit.id, hit.version) }
                        if (job !is Monitor) continue
                        val owner = job.user ?: continue
                        val visible = getVisibleBackendRoles(requester, owner) ?: continue
                        if (visible == owner.backendRoles) continue
                        val narrowed = job.copy(
                            user = User(owner.name, visible, owner.roles, owner.customAttNames)
                        )
                        val builder = jsonBuilder()
                        narrowed.toXContentWithUser(builder, ToXContent.MapParams(mapOf("with_type" to "true")))
                        hit.sourceRef(BytesReference.bytes(builder))
                    }
                } catch (e: Exception) {
                    // A hit that cannot be parsed cannot be narrowed either. Failing the whole search over one
                    // malformed document would be worse than returning it as the index holds it.
                    log.error("Failed to narrow backend roles on search monitor results", e)
                }
                actionListener.onResponse(response)
            }

            override fun onFailure(e: Exception) = actionListener.onFailure(e)
        }
    }

    // Used in Get and Search monitor functionalities to return a "no results" response
    fun getEmptySearchResponse(): SearchResponse {
        val internalSearchResponse = InternalSearchResponse(
            SearchHits(emptyArray(), TotalHits(0L, Relation.EQUAL_TO), 0.0f),
            InternalAggregations.from(Collections.emptyList()),
            Suggest(Collections.emptyList()),
            SearchProfileShardResults(Collections.emptyMap()),
            false,
            false,
            0
        )

        return SearchResponse(
            internalSearchResponse,
            "",
            0,
            0,
            0,
            0,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        )
    }

    // Checks if the exception is caused by an IndexNotFoundException (directly or nested).
    private fun isIndexNotFoundException(e: Exception): Boolean {
        var cause: Throwable? = e
        while (cause != null) {
            if (cause is IndexNotFoundException) return true
            cause = cause.cause
        }
        return false
    }

    fun search(searchRequest: SearchRequest, actionListener: ActionListener<SearchResponse>, tenantId: String? = null) {
        // When resource sharing is enabled, route search through PluginClient so it runs as the plugin subject
        // and the security plugin's DLS on the shared-resource index can filter results.
        if (ResourceSharingUtils.shouldUseResourceAuthz(ResourceSharingUtils.MONITOR_RESOURCE_TYPE)) {
            pluginClient.search(
                searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(response: SearchResponse) = actionListener.onResponse(response)
                    override fun onFailure(e: Exception) {
                        if (isIndexNotFoundException(e)) {
                            actionListener.onResponse(getEmptySearchResponse())
                        } else {
                            log.error("Unexpected error while searching monitor", e)
                            actionListener.onFailure(AlertingException.wrap(e))
                        }
                    }
                }
            )
            return
        }

        val sdkSearchRequest = SearchDataObjectRequest.builder()
            .indices(*searchRequest.indices())
            .tenantId(tenantId)
            .searchSourceBuilder(searchRequest.source())
            .build()

        sdkClient.searchDataObjectAsync(sdkSearchRequest).whenComplete { response, throwable ->
            if (throwable != null) {
                val cause = SdkClientUtils.unwrapAndConvertToException(throwable)
                if (isIndexNotFoundException(cause)) {
                    actionListener.onResponse(getEmptySearchResponse())
                } else {
                    log.error("Unexpected error while searching monitor", cause)
                    actionListener.onFailure(AlertingException.wrap(cause))
                }
                return@whenComplete
            }
            val searchResponse = response.searchResponse()
            if (searchResponse != null) {
                actionListener.onResponse(searchResponse)
            } else {
                actionListener.onResponse(getEmptySearchResponse())
            }
        }
    }

    private fun addOwnerFieldIfNotExists(searchRequest: SearchRequest) {
        if (searchRequest.source().query() == null || searchRequest.source().query().toString().contains("monitor.owner") == false) {
            var boolQueryBuilder: BoolQueryBuilder = if (searchRequest.source().query() == null) BoolQueryBuilder()
            else QueryBuilders.boolQuery().must(searchRequest.source().query())
            val bqb = BoolQueryBuilder()
            bqb.should().add(BoolQueryBuilder().mustNot(ExistsQueryBuilder("monitor.owner")))
            bqb.should().add(BoolQueryBuilder().must(MatchQueryBuilder("monitor.owner", "alerting")))
            boolQueryBuilder.filter(bqb)
            searchRequest.source().query(boolQueryBuilder)
        }
    }
}
