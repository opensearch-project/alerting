/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.SearchMonitorAction
import org.opensearch.alerting.action.SearchMonitorRequest
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALL_ALERT_INDEX_PATTERN
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.core.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.context
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.ToXContent.EMPTY_PARAMS
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.common.xcontent.XContentType
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.RestResponse
import org.opensearch.rest.RestStatus
import org.opensearch.rest.action.RestResponseListener
import org.opensearch.search.builder.SearchSourceBuilder
import java.io.IOException

private val log = LogManager.getLogger(RestSearchMonitorAction::class.java)

/**
 * Rest handlers to search for monitors.
 * TODO: Deprecate API for a set of new APIs that will support this APIs use cases
 */
class RestSearchMonitorAction(
    val settings: Settings,
    clusterService: ClusterService
) : BaseRestHandler() {

    @Volatile private var filterBy = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.FILTER_BY_BACKEND_ROLES) { filterBy = it }
    }

    override fun getName(): String {
        return "search_monitor_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            // Search for monitors
            ReplacedRoute(
                POST,
                "${AlertingPlugin.MONITOR_BASE_URI}/_search",
                POST,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/_search"
            ),
            ReplacedRoute(
                GET,
                "${AlertingPlugin.MONITOR_BASE_URI}/_search",
                GET,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/_search"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/_search")

        val index = request.param("index", SCHEDULED_JOBS_INDEX)
        if (index != SCHEDULED_JOBS_INDEX && index != ALL_ALERT_INDEX_PATTERN) {
            throw IllegalArgumentException("Invalid index name.")
        }

        val searchSourceBuilder = SearchSourceBuilder()
        searchSourceBuilder.parseXContent(request.contentOrSourceParamParser())
        searchSourceBuilder.fetchSource(context(request))

        val searchRequest = SearchRequest()
            .source(searchSourceBuilder)
            .indices(index)

        val searchMonitorRequest = SearchMonitorRequest(searchRequest)
        return RestChannelConsumer { channel ->
            client.execute(SearchMonitorAction.INSTANCE, searchMonitorRequest, searchMonitorResponse(channel))
        }
    }

    private fun searchMonitorResponse(channel: RestChannel): RestResponseListener<SearchResponse> {
        return object : RestResponseListener<SearchResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: SearchResponse): RestResponse {
                if (response.isTimedOut) {
                    return BytesRestResponse(RestStatus.REQUEST_TIMEOUT, response.toString())
                }

                // Swallow exception and return response as is
                try {
                    for (hit in response.hits) {
                        XContentType.JSON.xContent().createParser(
                            channel.request().xContentRegistry,
                            LoggingDeprecationHandler.INSTANCE, hit.sourceAsString
                        ).use { hitsParser ->
                            val monitor = ScheduledJob.parse(hitsParser, hit.id, hit.version)
                            val xcb = monitor.toXContent(jsonBuilder(), EMPTY_PARAMS)
                            hit.sourceRef(BytesReference.bytes(xcb))
                        }
                    }
                } catch (e: Exception) {
                    log.info("The monitor parsing failed. Will return response as is.")
                }
                return BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), EMPTY_PARAMS))
            }
        }
    }
}
