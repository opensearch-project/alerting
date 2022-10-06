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
import org.opensearch.alerting.action.SearchMonitorAction
import org.opensearch.alerting.action.SearchMonitorRequest
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.opensearchapi.addFilter
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.ExistsQueryBuilder
import org.opensearch.index.query.MatchQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportSearchMonitorAction::class.java)

class TransportSearchMonitorAction @Inject constructor(
    transportService: TransportService,
    val settings: Settings,
    val client: Client,
    clusterService: ClusterService,
    actionFilters: ActionFilters
) : HandledTransportAction<SearchMonitorRequest, SearchResponse>(
    SearchMonitorAction.NAME, transportService, actionFilters, ::SearchMonitorRequest
),
    SecureTransportAction {
    @Volatile
    override var filterByEnabled: Boolean = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, searchMonitorRequest: SearchMonitorRequest, actionListener: ActionListener<SearchResponse>) {
        val searchSourceBuilder = searchMonitorRequest.searchRequest.source()
        val queryBuilder = if (searchSourceBuilder.query() == null) BoolQueryBuilder()
        else QueryBuilders.boolQuery().must(searchSourceBuilder.query())
        queryBuilder.filter(QueryBuilders.existsQuery(Monitor.MONITOR_TYPE))
        searchSourceBuilder.query(queryBuilder)
            .seqNoAndPrimaryTerm(true)
            .version(true)
        addOwnerFieldIfNotExists(searchMonitorRequest.searchRequest)
        val user = readUserFromThreadContext(client)
        client.threadPool().threadContext.stashContext().use {
            resolve(searchMonitorRequest, actionListener, user)
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

    fun search(searchRequest: SearchRequest, actionListener: ActionListener<SearchResponse>) {
        client.search(
            searchRequest,
            object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    actionListener.onResponse(response)
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(AlertingException.wrap(t))
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
