/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.opensearchapi.addFilter
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.GetAlertsRequest
import org.opensearch.commons.alerting.action.GetWorkflowAlertsRequest
import org.opensearch.commons.alerting.action.GetWorkflowAlertsResponse
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.Operator
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortBuilders
import org.opensearch.search.sort.SortOrder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.io.IOException

private val log = LogManager.getLogger(TransportGetAlertsAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportGetWorkflowAlertsAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    clusterService: ClusterService,
    actionFilters: ActionFilters,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
) : HandledTransportAction<ActionRequest, GetWorkflowAlertsResponse>(
    AlertingActions.GET_WORKFLOW_ALERTS_ACTION_NAME,
    transportService,
    actionFilters,
    ::GetAlertsRequest
),
    SecureTransportAction {

    @Volatile
    override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    @Volatile
    private var isAlertHistoryEnabled = AlertingSettings.ALERT_HISTORY_ENABLED.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.ALERT_HISTORY_ENABLED) { isAlertHistoryEnabled = it }
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(
        task: Task,
        request: ActionRequest,
        actionListener: ActionListener<GetWorkflowAlertsResponse>,
    ) {
        val getWorkflowAlertsRequest = request as? GetWorkflowAlertsRequest
            ?: recreateObject(request) { GetWorkflowAlertsRequest(it) }
        val user = readUserFromThreadContext(client)

        val tableProp = getWorkflowAlertsRequest.table
        val sortBuilder = SortBuilders.fieldSort(tableProp.sortString)
            .order(SortOrder.fromString(tableProp.sortOrder))
        if (!tableProp.missing.isNullOrBlank()) {
            sortBuilder.missing(tableProp.missing)
        }

        val queryBuilder = QueryBuilders.boolQuery()

        if (getWorkflowAlertsRequest.severityLevel != "ALL") {
            queryBuilder.filter(QueryBuilders.termQuery("severity", getWorkflowAlertsRequest.severityLevel))
        }

        if (getWorkflowAlertsRequest.alertState == "ALL") {
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(Alert.STATE_FIELD, Alert.State.AUDIT.name)))
        } else {
            queryBuilder.filter(QueryBuilders.termQuery(Alert.STATE_FIELD, getWorkflowAlertsRequest.alertState))
        }

        if (getWorkflowAlertsRequest.alertIds.isNullOrEmpty() == false) {
            queryBuilder.filter(QueryBuilders.termsQuery("_id", getWorkflowAlertsRequest.alertIds))
        }

        if (getWorkflowAlertsRequest.monitorIds.isNullOrEmpty() == false) {
            queryBuilder.filter(QueryBuilders.termsQuery("monitor_id", getWorkflowAlertsRequest.monitorIds))
        }
        if (getWorkflowAlertsRequest.workflowIds.isNullOrEmpty() == false) {
            queryBuilder.must(QueryBuilders.termsQuery("workflow_id", getWorkflowAlertsRequest.workflowIds))
            queryBuilder.must(QueryBuilders.termQuery("monitor_id", ""))
        }
        if (!tableProp.searchString.isNullOrBlank()) {
            queryBuilder
                .must(
                    QueryBuilders.queryStringQuery(tableProp.searchString)
                        .defaultOperator(Operator.AND)
                        .field("monitor_name")
                        .field("trigger_name")
                )
        }
        // if alert id is mentioned we cannot set "from" field as it may not return id. we would be using it to paginate associated alerts
        val from = if (getWorkflowAlertsRequest.alertIds.isNullOrEmpty())
            tableProp.startIndex
        else 0

        val searchSourceBuilder = SearchSourceBuilder()
            .version(true)
            .seqNoAndPrimaryTerm(true)
            .query(queryBuilder)
            .sort(sortBuilder)
            .size(tableProp.size)
            .from(from)

        client.threadPool().threadContext.stashContext().use {
            scope.launch {
                try {
                    val alertIndex = resolveAlertsIndexName(getWorkflowAlertsRequest)
                    getAlerts(getWorkflowAlertsRequest, alertIndex, searchSourceBuilder, actionListener, user)
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

    fun resolveAlertsIndexName(getAlertsRequest: GetWorkflowAlertsRequest): String {
        var alertIndex = AlertIndices.ALL_ALERT_INDEX_PATTERN
        if (getAlertsRequest.alertIndex.isNullOrEmpty() == false) {
            alertIndex = getAlertsRequest.alertIndex!!
        }
        return if (alertIndex == AlertIndices.ALERT_INDEX)
            AlertIndices.ALL_ALERT_INDEX_PATTERN
        else
            alertIndex
    }

    fun resolveAssociatedAlertsIndexName(getAlertsRequest: GetWorkflowAlertsRequest): String {
        return if (getAlertsRequest.alertIndex.isNullOrEmpty()) AlertIndices.ALL_ALERT_INDEX_PATTERN
        else getAlertsRequest.associatedAlertsIndex!!
    }

    suspend fun getAlerts(
        getWorkflowAlertsRequest: GetWorkflowAlertsRequest,
        alertIndex: String,
        searchSourceBuilder: SearchSourceBuilder,
        actionListener: ActionListener<GetWorkflowAlertsResponse>,
        user: User?,
    ) {
        // user is null when: 1/ security is disabled. 2/when user is super-admin.
        if (user == null) {
            // user is null when: 1/ security is disabled. 2/when user is super-admin.
            search(getWorkflowAlertsRequest, alertIndex, searchSourceBuilder, actionListener)
        } else if (!doFilterForUser(user)) {
            // security is enabled and filterby is disabled.
            search(getWorkflowAlertsRequest, alertIndex, searchSourceBuilder, actionListener)
        } else {
            // security is enabled and filterby is enabled.
            try {
                log.info("Filtering result by: ${user.backendRoles}")
                addFilter(user, searchSourceBuilder, "monitor_user.backend_roles.keyword")
                search(getWorkflowAlertsRequest, alertIndex, searchSourceBuilder, actionListener)
            } catch (ex: IOException) {
                actionListener.onFailure(AlertingException.wrap(ex))
            }
        }
    }

    suspend fun search(
        getWorkflowAlertsRequest: GetWorkflowAlertsRequest,
        alertIndex: String,
        searchSourceBuilder: SearchSourceBuilder,
        actionListener: ActionListener<GetWorkflowAlertsResponse>,
    ) {
        try {
            val searchRequest = SearchRequest()
                .indices(alertIndex)
                .source(searchSourceBuilder)
            val alerts = mutableListOf<Alert>()
            val associatedAlerts = mutableListOf<Alert>()

            val response: SearchResponse = client.suspendUntil { search(searchRequest, it) }
            val totalAlertCount = response.hits.totalHits?.value?.toInt()
            alerts.addAll(
                parseAlertsFromSearchResponse(response)
            )
            if (alerts.isNotEmpty() && getWorkflowAlertsRequest.getAssociatedAlerts == true)
                getAssociatedAlerts(
                    associatedAlerts,
                    alerts,
                    resolveAssociatedAlertsIndexName(getWorkflowAlertsRequest),
                    getWorkflowAlertsRequest
                )
            actionListener.onResponse(GetWorkflowAlertsResponse(alerts, associatedAlerts, totalAlertCount))
        } catch (e: Exception) {
            actionListener.onFailure(AlertingException("Failed to get alerts", RestStatus.INTERNAL_SERVER_ERROR, e))
        }
    }

    private suspend fun getAssociatedAlerts(
        associatedAlerts: MutableList<Alert>,
        alerts: MutableList<Alert>,
        alertIndex: String,
        getWorkflowAlertsRequest: GetWorkflowAlertsRequest,
    ) {
        try {
            val associatedAlertIds = mutableSetOf<String>()
            alerts.forEach { associatedAlertIds.addAll(it.associatedAlertIds) }
            if (associatedAlertIds.isEmpty()) return
            val queryBuilder = QueryBuilders.boolQuery()
            val searchRequest = SearchRequest(alertIndex)
            // if chained alert id param is non-null, paginate the associated alerts.
            if (getWorkflowAlertsRequest.alertIds.isNullOrEmpty() == false) {
                val tableProp = getWorkflowAlertsRequest.table
                val sortBuilder = SortBuilders.fieldSort(tableProp.sortString)
                    .order(SortOrder.fromString(tableProp.sortOrder))
                if (!tableProp.missing.isNullOrBlank()) {
                    sortBuilder.missing(tableProp.missing)
                }
                searchRequest.source().sort(sortBuilder).size(tableProp.size).from(tableProp.startIndex)
            }
            queryBuilder.must(QueryBuilders.termsQuery("_id", associatedAlertIds))
            queryBuilder.must(QueryBuilders.termQuery(Alert.STATE_FIELD, Alert.State.AUDIT.name))
            searchRequest.source().query(queryBuilder)
            val response: SearchResponse = client.suspendUntil { search(searchRequest, it) }
            associatedAlerts.addAll(parseAlertsFromSearchResponse(response))
        } catch (e: Exception) {
            log.error("Failed to get associated alerts in get workflow alerts action", e)
        }
    }

    private fun parseAlertsFromSearchResponse(response: SearchResponse) = response.hits.map { hit ->
        val xcp = XContentHelper.createParser(
            xContentRegistry,
            LoggingDeprecationHandler.INSTANCE,
            hit.sourceRef,
            XContentType.JSON
        )
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
        val alert = Alert.parse(xcp, hit.id, hit.version)
        alert
    }
}
