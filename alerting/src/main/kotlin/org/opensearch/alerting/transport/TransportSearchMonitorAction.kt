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
import org.opensearch.action.search.SearchResponse.Clusters
import org.opensearch.action.search.ShardSearchFailure
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.opensearchapi.addFilter
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.use
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.SearchMonitorRequest
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.ExistsQueryBuilder
import org.opensearch.index.query.MatchQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.SearchHits
import org.opensearch.search.aggregations.InternalAggregations
import org.opensearch.search.internal.InternalSearchResponse
import org.opensearch.search.profile.SearchProfileShardResults
import org.opensearch.search.suggest.Suggest
import org.opensearch.tasks.Task
import org.opensearch.transport.RemoteTransportException
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
    val namedWriteableRegistry: NamedWriteableRegistry
) : HandledTransportAction<ActionRequest, SearchResponse>(
    AlertingActions.SEARCH_MONITORS_ACTION_NAME, transportService, actionFilters, ::SearchMonitorRequest
),
    SecureTransportAction {
    @Volatile
    override var filterByEnabled: Boolean = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)
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
        client.threadPool().threadContext.stashContext().use {
            resolve(transformedRequest, actionListener, user)
        }
    }

    fun resolve(searchMonitorRequest: SearchMonitorRequest, actionListener: ActionListener<SearchResponse>, user: User?) {
        if (user == null) {
            // user header is null when: 1/ security is disabled. 2/when user is super-admin.
            search(searchMonitorRequest.searchRequest, actionListener)
        } else if (!doFilterForUser(user)) {
            // security is enabled and filterby is disabled.
            search(searchMonitorRequest.searchRequest, actionListener)
        } else {
            // security is enabled and filterby is enabled.
            log.info("Filtering result by: ${user.backendRoles}")
            addFilter(user, searchMonitorRequest.searchRequest.source(), "monitor.user.backend_roles.keyword")
            search(searchMonitorRequest.searchRequest, actionListener)
        }
    }

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
        if (e is IndexNotFoundException) return true
        if (e is RemoteTransportException) {
            val cause = e.cause
            if (cause is IndexNotFoundException) return true
        }
        return false
    }

    fun search(searchRequest: SearchRequest, actionListener: ActionListener<SearchResponse>) {
        client.search(
            searchRequest,
            object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    actionListener.onResponse(response)
                }

                override fun onFailure(ex: Exception) {
                    if (isIndexNotFoundException(ex)) {
                        log.error("Index not found while searching monitor", ex)
                        val emptyResponse = getEmptySearchResponse()
                        actionListener.onResponse(emptyResponse)
                    } else {
                        log.error("Unexpected error while searching monitor", ex)
                        actionListener.onFailure(AlertingException.wrap(ex))
                    }
                }
            }
        )
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
