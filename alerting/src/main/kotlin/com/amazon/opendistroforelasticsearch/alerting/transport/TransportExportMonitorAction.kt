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

package com.amazon.opendistroforelasticsearch.alerting.transport

import com.amazon.opendistroforelasticsearch.alerting.action.ExportMonitorAction
import com.amazon.opendistroforelasticsearch.alerting.action.ExportMonitorRequest
import com.amazon.opendistroforelasticsearch.alerting.action.ExportMonitorResponse
import com.amazon.opendistroforelasticsearch.alerting.core.model.ScheduledJob
import com.amazon.opendistroforelasticsearch.alerting.model.Monitor
import com.amazon.opendistroforelasticsearch.alerting.settings.AlertingSettings
import com.amazon.opendistroforelasticsearch.alerting.util.AlertingException
import com.amazon.opendistroforelasticsearch.alerting.util.checkFilterByUserBackendRoles
import com.amazon.opendistroforelasticsearch.commons.ConfigConstants
import com.amazon.opendistroforelasticsearch.commons.authuser.User
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.*
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

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
        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENDISTRO_SECURITY_USER_INFO_THREAD_CONTEXT)
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
            client.search(searchRequest, object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    var monitors = mutableListOf<Monitor>()

                    for (hit in response.hits) {
                        val xcp = XContentFactory
                            .xContent(XContentType.JSON)
                            .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, hit.sourceAsString)
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, xcp.nextToken(), xcp)
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                        monitors.add(Monitor.parse(xcp))
                    }

                    actionListener.onResponse(
                        ExportMonitorResponse(monitors)
                    )
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(AlertingException.wrap(t))
                }
            })
        }
    }
}
