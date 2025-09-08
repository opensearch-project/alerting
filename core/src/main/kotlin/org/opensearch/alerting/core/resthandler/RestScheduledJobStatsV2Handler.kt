package org.opensearch.alerting.core.resthandler

import org.opensearch.alerting.core.action.node.ScheduledJobsStatsAction
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.action.RestActions
import org.opensearch.transport.client.node.NodeClient

/**
 * RestScheduledJobStatsHandler is handler for getting ScheduledJob Stats for Alerting V2 Scheduled Jobs.
 */
class RestScheduledJobStatsV2Handler : BaseRestHandler() {

    override fun getName(): String {
        return "alerting_jobs_stats_v2"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(
                GET,
                "/_plugins/_alerting/v2/stats/"
            ),
            Route(
                GET,
                "/_plugins/_alerting/v2/stats/{metric}"
            ),
            Route(
                GET,
                "/_plugins/_alerting/v2/{nodeId}/stats/"
            ),
            Route(
                GET,
                "/_plugins/_alerting/v2/{nodeId}/stats/{metric}"
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val scheduledJobNodesStatsRequest = StatsRequestUtils.getStatsRequest(request, true, this::unrecognized)
        return RestChannelConsumer { channel ->
            client.execute(
                ScheduledJobsStatsAction.INSTANCE,
                scheduledJobNodesStatsRequest,
                RestActions.NodesResponseRestListener(channel)
            )
        }
    }
}
