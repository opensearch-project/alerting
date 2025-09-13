package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.actionv2.GetAlertsV2Action
import org.opensearch.alerting.actionv2.GetAlertsV2Request
import org.opensearch.alerting.actionv2.GetAlertsV2Response
import org.opensearch.alerting.alertsv2.AlertV2Indices
import org.opensearch.alerting.core.modelv2.AlertV2
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.use
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
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
import org.opensearch.transport.client.Client
import java.io.IOException

private val log = LogManager.getLogger(TransportGetAlertsV2Action::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportGetAlertsV2Action @Inject constructor(
    transportService: TransportService,
    val client: Client,
    clusterService: ClusterService,
    actionFilters: ActionFilters,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
    val namedWriteableRegistry: NamedWriteableRegistry
) : HandledTransportAction<GetAlertsV2Request, GetAlertsV2Response>(
    GetAlertsV2Action.NAME,
    transportService,
    actionFilters,
    ::GetAlertsV2Request
),
    SecureTransportAction {

    @Volatile
    override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(
        task: Task,
        getAlertsV2Request: GetAlertsV2Request,
        actionListener: ActionListener<GetAlertsV2Response>,
    ) {
//        val user = readUserFromThreadContext(client)

        val tableProp = getAlertsV2Request.table
        val sortBuilder = SortBuilders
            .fieldSort(tableProp.sortString)
            .order(SortOrder.fromString(tableProp.sortOrder))
        if (!tableProp.missing.isNullOrBlank()) {
            sortBuilder.missing(tableProp.missing)
        }

        val queryBuilder = getAlertsV2Request.boolQueryBuilder ?: QueryBuilders.boolQuery()

        if (getAlertsV2Request.severityLevel != "ALL") {
            queryBuilder.filter(QueryBuilders.termQuery("severity", getAlertsV2Request.severityLevel))
        }

        if (getAlertsV2Request.alertV2Ids.isNullOrEmpty() == false) {
            queryBuilder.filter(QueryBuilders.termsQuery("_id", getAlertsV2Request.alertV2Ids))
        }

        if (getAlertsV2Request.monitorV2Id != null) {
            queryBuilder.filter(QueryBuilders.termQuery("monitor_id", getAlertsV2Request.monitorV2Id))
        } else if (!getAlertsV2Request.monitorV2Ids.isNullOrEmpty()) {
            queryBuilder.filter(QueryBuilders.termsQuery("monitor_id", getAlertsV2Request.monitorV2Ids))
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
                    getAlerts(AlertV2Indices.ALERT_V2_INDEX, searchSourceBuilder, actionListener/*, user*/)
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

    fun getAlerts(
        alertIndex: String,
        searchSourceBuilder: SearchSourceBuilder,
        actionListener: ActionListener<GetAlertsV2Response>,
//        user: User?,
    ) {
        // TODO: when implementing RBAC, pivot to this implementation
//        // user is null when: 1/ security is disabled. 2/when user is super-admin.
//        if (user == null) {
//            // user is null when: 1/ security is disabled. 2/when user is super-admin.
//            search(alertIndex, searchSourceBuilder, actionListener)
//        } else if (!doFilterForUser(user)) {
//            // security is enabled and filterby is disabled.
//            search(alertIndex, searchSourceBuilder, actionListener)
//        } else {
//            // security is enabled and filterby is enabled.
//            try {
//                log.info("Filtering result by: ${user.backendRoles}")
//                addFilter(user, searchSourceBuilder, "monitor_user.backend_roles.keyword")
//                search(alertIndex, searchSourceBuilder, actionListener)
//            } catch (ex: IOException) {
//                actionListener.onFailure(AlertingException.wrap(ex))
//            }
//        }
        try {
            search(alertIndex, searchSourceBuilder, actionListener)
        } catch (ex: IOException) {
            actionListener.onFailure(AlertingException.wrap(ex))
        }
    }

    fun search(alertIndex: String, searchSourceBuilder: SearchSourceBuilder, actionListener: ActionListener<GetAlertsV2Response>) {
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
                            xContentRegistry,
                            LoggingDeprecationHandler.INSTANCE,
                            hit.sourceRef,
                            XContentType.JSON
                        )
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                        val alertV2 = AlertV2.parse(xcp, hit.id, hit.version)
                        alertV2
                    }
                    actionListener.onResponse(GetAlertsV2Response(alerts, totalAlertCount))
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(t)
                }
            }
        )
    }
}
