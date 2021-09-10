/*
 *   Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import org.opensearch.action.ActionListener
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.ExportMonitorAction
import org.opensearch.alerting.action.ExportMonitorRequest
import org.opensearch.alerting.action.ExportMonitorResponse
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.checkFilterByUserBackendRoles
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentParser.Token.FIELD_NAME
import org.opensearch.common.xcontent.XContentParser.Token.START_OBJECT
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType.JSON
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportExportMonitorAction::class.java)

class TransportExportMonitorAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    val clusterService: ClusterService,
    settings: Settings
) : HandledTransportAction<ExportMonitorRequest, ExportMonitorResponse> (
    ExportMonitorAction.NAME, transportService, actionFilters, ::ExportMonitorRequest
) {

    @Volatile private var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.FILTER_BY_BACKEND_ROLES) { filterByEnabled = it }
    }

    override fun doExecute(task: Task, request: ExportMonitorRequest, actionListener: ActionListener<ExportMonitorResponse>) {
        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
        log.debug("User and roles string from thread context: $userStr")
        val user: User? = User.parse(userStr)

        if (!checkFilterByUserBackendRoles(filterByEnabled, user, actionListener)) {
            return
        }

        // Get all monitors in SearchRequest
        val queryBuilder = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("${Monitor.MONITOR_TYPE}.type", Monitor.MONITOR_TYPE))
        val searchSourceBuilder = SearchSourceBuilder()
            .query(queryBuilder)
        val searchRequest = SearchRequest()
            .source(searchSourceBuilder)
            .indices(ScheduledJob.SCHEDULED_JOBS_INDEX)

        /*
         * Remove security context before you call elasticsearch api's. By this time, permissions required
         * to call this api are validated.
         * Once system-indices [https://github.com/opendistro-for-elasticsearch/security/issues/666] is done, we
         * might further improve this logic. Also change try to kotlin-use for auto-closable.
         */
        client.threadPool().threadContext.stashContext().use {
            client.search(
                searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(response: SearchResponse) {
                        var monitors = mutableListOf<Monitor>()

                        for (hit in response.hits) {
                            val xcp = XContentFactory
                                .xContent(JSON)
                                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, hit.sourceAsString)
                            XContentParserUtils.ensureExpectedToken(START_OBJECT, xcp.nextToken(), xcp)
                            XContentParserUtils.ensureExpectedToken(FIELD_NAME, xcp.nextToken(), xcp)
                            XContentParserUtils.ensureExpectedToken(START_OBJECT, xcp.nextToken(), xcp)
                            monitors.add(Monitor.parse(xcp))
                        }

                        actionListener.onResponse(
                            ExportMonitorResponse(monitors)
                        )
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        }
    }
}
