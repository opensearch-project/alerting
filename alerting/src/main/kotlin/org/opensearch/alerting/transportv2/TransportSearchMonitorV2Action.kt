/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transportv2

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.AlertingV2Utils.getEmptySearchResponse
import org.opensearch.alerting.AlertingV2Utils.isIndexNotFoundException
import org.opensearch.alerting.actionv2.SearchMonitorV2Action
import org.opensearch.alerting.actionv2.SearchMonitorV2Request
import org.opensearch.alerting.core.settings.AlertingV2Settings.Companion.ALERTING_V2_ENABLED
import org.opensearch.alerting.modelv2.MonitorV2.Companion.MONITOR_V2_TYPE
import org.opensearch.alerting.modelv2.PPLSQLMonitor.Companion.PPL_SQL_MONITOR_TYPE
import org.opensearch.alerting.opensearchapi.addFilter
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.transport.SecureTransportAction
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

private val log = LogManager.getLogger(TransportSearchMonitorV2Action::class.java)

/**
 * Transport action that contains the core logic for searching monitor V2s via an OpenSearch search query.
 *
 * @opensearch.experimental
 */
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

    @Volatile private var alertingV2Enabled = ALERTING_V2_ENABLED.get(settings)

    @Volatile
    override var filterByEnabled: Boolean = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_V2_ENABLED) { alertingV2Enabled = it }
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: SearchMonitorV2Request, actionListener: ActionListener<SearchResponse>) {
        if (!alertingV2Enabled) {
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException(
                        "Alerting V2 is currently disabled, please enable it with the " +
                            "cluster setting: ${ALERTING_V2_ENABLED.key}",
                        RestStatus.FORBIDDEN
                    ),
                )
            )
            return
        }

        val searchSourceBuilder = request.searchRequest.source()

        val queryBuilder = if (searchSourceBuilder.query() == null) BoolQueryBuilder()
        else QueryBuilders.boolQuery().must(searchSourceBuilder.query())

        // filter out MonitorV1s in the alerting config index
        // only return MonitorV2s that match the user-given search query
        queryBuilder.filter(QueryBuilders.existsQuery(MONITOR_V2_TYPE))

        searchSourceBuilder.query(queryBuilder)
            .seqNoAndPrimaryTerm(true)
            .version(true)

        val user = readUserFromThreadContext(client)
        client.threadPool().threadContext.stashContext().use {
            // if user is null, security plugin is disabled or user is super-admin
            // if doFilterForUser() is false, security is enabled but filterby is disabled
            if (user != null && doFilterForUser(user)) {
                log.info("Filtering result by: ${user.backendRoles}")
                addFilter(user, request.searchRequest.source(), "$MONITOR_V2_TYPE.$PPL_SQL_MONITOR_TYPE.user.backend_roles.keyword")
            }

            client.search(
                request.searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(response: SearchResponse) {
                        actionListener.onResponse(response)
                    }

                    override fun onFailure(e: Exception) {
                        if (isIndexNotFoundException(e)) {
                            log.error("Index not found while searching monitor", e)
                            val emptyResponse = getEmptySearchResponse()
                            actionListener.onResponse(emptyResponse)
                        } else {
                            log.error("Unexpected error while searching monitor", e)
                            actionListener.onFailure(AlertingException.wrap(e))
                        }
                    }
                }
            )
        }
    }
}
