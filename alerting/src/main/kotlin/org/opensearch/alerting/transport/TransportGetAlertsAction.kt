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
import org.opensearch.action.ActionListener
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.GetAlertsAction
import org.opensearch.alerting.action.GetAlertsRequest
import org.opensearch.alerting.action.GetAlertsResponse
import org.opensearch.alerting.action.GetMonitorAction
import org.opensearch.alerting.action.GetMonitorRequest
import org.opensearch.alerting.action.GetMonitorResponse
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.opensearchapi.addFilter
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.Operator
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestRequest
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.search.sort.SortBuilders
import org.opensearch.search.sort.SortOrder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.io.IOException

private val log = LogManager.getLogger(TransportGetAlertsAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportGetAlertsAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    clusterService: ClusterService,
    actionFilters: ActionFilters,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
    val transportGetMonitorAction: TransportGetMonitorAction
) : HandledTransportAction<GetAlertsRequest, GetAlertsResponse>(
    GetAlertsAction.NAME, transportService, actionFilters, ::GetAlertsRequest
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(
        task: Task,
        getAlertsRequest: GetAlertsRequest,
        actionListener: ActionListener<GetAlertsResponse>
    ) {
        val user = readUserFromThreadContext(client)

        val tableProp = getAlertsRequest.table
        val sortBuilder = SortBuilders
            .fieldSort(tableProp.sortString)
            .order(SortOrder.fromString(tableProp.sortOrder))
        if (!tableProp.missing.isNullOrBlank()) {
            sortBuilder.missing(tableProp.missing)
        }

        val queryBuilder = QueryBuilders.boolQuery()

        if (getAlertsRequest.severityLevel != "ALL")
            queryBuilder.filter(QueryBuilders.termQuery("severity", getAlertsRequest.severityLevel))

        if (getAlertsRequest.alertState != "ALL")
            queryBuilder.filter(QueryBuilders.termQuery("state", getAlertsRequest.alertState))

        if (getAlertsRequest.monitorId != null) {
            queryBuilder.filter(QueryBuilders.termQuery("monitor_id", getAlertsRequest.monitorId))
        }
        if (!tableProp.searchString.isNullOrBlank()) {
            queryBuilder
                .must(
                    QueryBuilders
                        .queryStringQuery(tableProp.searchString)
                        .defaultOperator(Operator.AND)
                        .field("monitor_name")
                        .field("trigger_name")
                )
        }
        val searchSourceBuilder = SearchSourceBuilder()
            .version(true)
            .seqNoAndPrimaryTerm(true)
            .query(queryBuilder)
            .sort(sortBuilder)
            .size(tableProp.size)
            .from(tableProp.startIndex)

        client.threadPool().threadContext.stashContext().use {
            scope.launch {
                try {
                    val alertIndex = resolveAlertsIndexName(getAlertsRequest)
                    getAlerts(alertIndex, searchSourceBuilder, actionListener, user)
                } catch (t: Exception) {
                    log.error("Failed to get alerts", t)
                    if (t is AlertingException) {
                        actionListener.onFailure(t)
                    } else {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            }
        }
    }

    /** Precedence order for resolving alert index to be queried:
     1. alertIndex param.
     2. alert index mentioned in monitor data sources.
     3. Default alert indices pattern
     */
    suspend fun resolveAlertsIndexName(getAlertsRequest: GetAlertsRequest): String {
        var alertIndex = AlertIndices.ALL_ALERT_INDEX_PATTERN
        if (getAlertsRequest.alertIndex.isNullOrEmpty() == false) {
            alertIndex = getAlertsRequest.alertIndex
        } else if (getAlertsRequest.monitorId.isNullOrEmpty() == false)
            withContext(Dispatchers.IO) {
                val getMonitorRequest = GetMonitorRequest(
                    getAlertsRequest.monitorId,
                    -3L,
                    RestRequest.Method.GET,
                    FetchSourceContext.FETCH_SOURCE
                )
                val getMonitorResponse: GetMonitorResponse =
                    transportGetMonitorAction.client.suspendUntil {
                        execute(GetMonitorAction.INSTANCE, getMonitorRequest, it)
                    }
                if (getMonitorResponse.monitor != null) {
                    alertIndex = getMonitorResponse.monitor!!.dataSources.alertsIndex
                }
            }
        return alertIndex
    }

    fun getAlerts(
        alertIndex: String,
        searchSourceBuilder: SearchSourceBuilder,
        actionListener: ActionListener<GetAlertsResponse>,
        user: User?
    ) {
        // user is null when: 1/ security is disabled. 2/when user is super-admin.
        if (user == null) {
            // user is null when: 1/ security is disabled. 2/when user is super-admin.
            search(alertIndex, searchSourceBuilder, actionListener)
        } else if (!doFilterForUser(user)) {
            // security is enabled and filterby is disabled.
            search(alertIndex, searchSourceBuilder, actionListener)
        } else {
            // security is enabled and filterby is enabled.
            try {
                log.info("Filtering result by: ${user.backendRoles}")
                addFilter(user, searchSourceBuilder, "monitor_user.backend_roles.keyword")
                search(alertIndex, searchSourceBuilder, actionListener)
            } catch (ex: IOException) {
                actionListener.onFailure(AlertingException.wrap(ex))
            }
        }
    }

    fun search(alertIndex: String, searchSourceBuilder: SearchSourceBuilder, actionListener: ActionListener<GetAlertsResponse>) {
        val searchRequest = SearchRequest()
            .indices(alertIndex)
            .source(searchSourceBuilder)

        client.search(
            searchRequest,
            object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    val totalAlertCount = response.hits.totalHits?.value?.toInt()
                    val alerts = response.hits.map { hit ->
                        val xcp = XContentHelper.createParser(
                            xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                            hit.sourceRef, XContentType.JSON
                        )
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                        val alert = Alert.parse(xcp, hit.id, hit.version)
                        alert
                    }
                    actionListener.onResponse(GetAlertsResponse(alerts, totalAlertCount))
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(t)
                }
            }
        )
    }
}
