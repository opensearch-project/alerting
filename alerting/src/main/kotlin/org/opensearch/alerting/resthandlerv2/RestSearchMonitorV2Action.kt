/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandlerv2

import org.apache.logging.log4j.LogManager
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.actionv2.SearchMonitorV2Action
import org.opensearch.alerting.actionv2.SearchMonitorV2Request
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.context
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.RestResponse
import org.opensearch.rest.action.RestResponseListener
import org.opensearch.search.builder.SearchSourceBuilder
import java.io.IOException

private val log = LogManager.getLogger(RestSearchMonitorV2Action::class.java)

/**
 * This class consists of the REST handler to search for v2 monitors with some OpenSearch search query.
 *
 * @opensearch.experimental
 */
class RestSearchMonitorV2Action(
    val settings: Settings,
    clusterService: ClusterService,
) : BaseRestHandler() {

    @Volatile private var filterBy = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.FILTER_BY_BACKEND_ROLES) { filterBy = it }
    }

    override fun getName(): String {
        return "search_monitor_v2_action"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(
                POST,
                "${AlertingPlugin.MONITOR_V2_BASE_URI}/_search"
            ),
            Route(
                GET,
                "${AlertingPlugin.MONITOR_V2_BASE_URI}/_search"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_V2_BASE_URI}/_search")

        val searchSourceBuilder = SearchSourceBuilder()
        searchSourceBuilder.parseXContent(request.contentOrSourceParamParser())
        searchSourceBuilder.fetchSource(context(request))

        val searchRequest = SearchRequest()
            .source(searchSourceBuilder)
            .indices(SCHEDULED_JOBS_INDEX)

        val searchMonitorV2Request = SearchMonitorV2Request(searchRequest)
        return RestChannelConsumer { channel ->
            client.execute(SearchMonitorV2Action.INSTANCE, searchMonitorV2Request, searchMonitorResponse(channel))
        }
    }

    // once the search response is received, rewrite the search hits to remove the extra "monitor_v2" JSON object wrapper
    // that is used as ScheduledJob metadata
    private fun searchMonitorResponse(channel: RestChannel): RestResponseListener<SearchResponse> {
        return object : RestResponseListener<SearchResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: SearchResponse): RestResponse {
                if (response.isTimedOut) {
                    return BytesRestResponse(RestStatus.REQUEST_TIMEOUT, response.toString())
                }

                try {
                    for (hit in response.hits) {
                        XContentType.JSON.xContent().createParser(
                            channel.request().xContentRegistry,
                            LoggingDeprecationHandler.INSTANCE, hit.sourceAsString
                        ).use { hitsParser ->
                            // when reconstructing XContent, intentionally leave out
                            // user field in response for security reasons by
                            // calling ScheduledJob.toXContent instead of
                            // a MonitorV2's toXContentWithUser
                            val monitorV2 = ScheduledJob.parse(hitsParser, hit.id, hit.version)
                            val xcb = monitorV2.toXContent(jsonBuilder(), EMPTY_PARAMS)

                            // rewrite the search hit as just the MonitorV2 source,
                            // without the extra "monitor_v2" JSON object wrapper
                            hit.sourceRef(BytesReference.bytes(xcb))
                        }
                    }
                } catch (e: Exception) {
                    // Swallow exception and return response as is
                    log.error("The monitor_v2 parsing failed. Will return response as is.")
                }
                return BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), EMPTY_PARAMS))
            }
        }
    }
}
