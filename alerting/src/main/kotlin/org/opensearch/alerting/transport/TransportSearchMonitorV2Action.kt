package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.actionv2.SearchMonitorV2Action
import org.opensearch.alerting.actionv2.SearchMonitorV2Request
import org.opensearch.alerting.core.modelv2.MonitorV2.Companion.MONITOR_V2_TYPE
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

private val log = LogManager.getLogger(TransportSearchMonitorV2Action::class.java)

class TransportSearchMonitorV2Action @Inject constructor(
    transportService: TransportService,
    val settings: Settings,
    val client: Client,
    clusterService: ClusterService,
    actionFilters: ActionFilters,
    val namedWriteableRegistry: NamedWriteableRegistry
) : HandledTransportAction<SearchMonitorV2Request, SearchResponse>(
    SearchMonitorV2Action.NAME, transportService, actionFilters, ::SearchMonitorV2Request
),
    SecureTransportAction {

    @Volatile
    override var filterByEnabled: Boolean = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: SearchMonitorV2Request, actionListener: ActionListener<SearchResponse>) {

        val searchSourceBuilder = request.searchRequest.source()

        val queryBuilder = if (searchSourceBuilder.query() == null) BoolQueryBuilder()
        else QueryBuilders.boolQuery().must(searchSourceBuilder.query())

        // filter out MonitorV1s in the alerting config index
        // only return MonitorV2s that match the user-given search query
        queryBuilder.filter(QueryBuilders.existsQuery(MONITOR_V2_TYPE))

        searchSourceBuilder.query(queryBuilder)
            .seqNoAndPrimaryTerm(true)
            .version(true)

//        addOwnerFieldIfNotExists(transformedRequest.searchRequest)
//        val user = readUserFromThreadContext(client)
//        client.threadPool().threadContext.stashContext().use {
//            resolve(transformedRequest, actionListener, user)
//        }

        client.search(
            request.searchRequest,
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
}
