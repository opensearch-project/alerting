/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.resthandler

import org.opensearch.alerting.core.action.node.ScheduledJobsStatsAction
import org.opensearch.alerting.core.action.node.ScheduledJobsStatsRequest
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

    companion object {
        const val JOB_SCHEDULING_METRICS: String = "job_scheduling_metrics"
        const val JOBS_INFO: String = "jobs_info"
        val METRICS = mapOf<String, (ScheduledJobsStatsRequest) -> Unit>(
            JOB_SCHEDULING_METRICS to { it -> it.jobSchedulingMetrics = true },
            JOBS_INFO to { it -> it.jobsInfo = true }
        )
    }

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
        val scheduledJobNodesStatsRequest = StatsRequestUtils.getStatsRequest(request, false, this::unrecognized)
        return RestChannelConsumer { channel ->
            client.execute(
                ScheduledJobsStatsAction.INSTANCE,
                scheduledJobNodesStatsRequest,
                RestActions.NodesResponseRestListener(channel)
            )
        }
    }
}
