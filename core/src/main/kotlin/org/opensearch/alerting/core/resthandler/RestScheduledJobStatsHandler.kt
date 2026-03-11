/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.resthandler

import org.opensearch.alerting.core.action.node.ScheduledJobsStatsAction
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.action.RestActions
import org.opensearch.transport.client.node.NodeClient

/**
 * RestScheduledJobStatsHandler is handler for getting ScheduledJob Stats.
 */
class RestScheduledJobStatsHandler(private val path: String) : BaseRestHandler() {

    override fun getName(): String {
        return "${path}_jobs_stats"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<RestHandler.ReplacedRoute> {
        return mutableListOf(
            RestHandler.ReplacedRoute(
                GET,
                "/_plugins/$path/{nodeId}/stats/",
                GET,
                "/_opendistro/$path/{nodeId}/stats/"
            ),
            RestHandler.ReplacedRoute(
                GET,
                "/_plugins/$path/{nodeId}/stats/{metric}",
                GET,
                "/_opendistro/$path/{nodeId}/stats/{metric}"
            ),
            RestHandler.ReplacedRoute(
                GET,
                "/_plugins/$path/stats/",
                GET,
                "/_opendistro/$path/stats/"
            ),
            RestHandler.ReplacedRoute(
                GET,
                "/_plugins/$path/stats/{metric}",
                GET,
                "/_opendistro/$path/stats/{metric}"
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val alertingVersion = request.param("version")
        if (alertingVersion != null && alertingVersion !in listOf("v1", "v2")) {
            throw IllegalArgumentException("Version parameter must be one of v1 or v2")
        }

        val showV2ScheduledJobs: Boolean? = alertingVersion?.let { it == "v2" }

        val scheduledJobNodesStatsRequest = StatsRequestUtils.getStatsRequest(request, showV2ScheduledJobs, this::unrecognized)
        return RestChannelConsumer { channel ->
            client.execute(
                ScheduledJobsStatsAction.INSTANCE,
                scheduledJobNodesStatsRequest,
                RestActions.NodesResponseRestListener(channel)
            )
        }
    }
}
