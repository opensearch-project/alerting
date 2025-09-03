package org.opensearch.alerting.core.resthandler

import org.opensearch.alerting.core.action.node.ScheduledJobsStatsAction
import org.opensearch.alerting.core.action.node.ScheduledJobsStatsRequest
import org.opensearch.alerting.core.resthandler.RestScheduledJobStatsHandler.Companion.METRICS
import org.opensearch.core.common.Strings
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.action.RestActions
import org.opensearch.transport.client.node.NodeClient
import java.util.Locale
import java.util.TreeSet

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
        val scheduledJobNodesStatsRequest = getRequest(request)
        return RestChannelConsumer { channel ->
            client.execute(
                ScheduledJobsStatsAction.INSTANCE,
                scheduledJobNodesStatsRequest,
                RestActions.NodesResponseRestListener(channel)
            )
        }
    }

    private fun getRequest(request: RestRequest): ScheduledJobsStatsRequest {
        val nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"))
        val metrics = Strings.tokenizeByCommaToSet(request.param("metric"))
        val scheduledJobsStatsRequest = ScheduledJobsStatsRequest(nodeIds = nodesIds, showAlertingV2ScheduledJobs = true)
        scheduledJobsStatsRequest.timeout(request.param("timeout"))

        if (metrics.isEmpty()) {
            return scheduledJobsStatsRequest
        } else if (metrics.size == 1 && metrics.contains("_all")) {
            scheduledJobsStatsRequest.all()
        } else if (metrics.contains("_all")) {
            throw IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "request [%s] contains _all and individual metrics [%s]",
                    request.path(),
                    request.param("metric")
                )
            )
        } else {
            // use a sorted set so the unrecognized parameters appear in a reliable sorted order
            scheduledJobsStatsRequest.clear()
            val invalidMetrics = TreeSet<String>()
            for (metric in metrics) {
                val handler = METRICS[metric]
                if (handler != null) {
                    handler.invoke(scheduledJobsStatsRequest)
                } else {
                    invalidMetrics.add(metric)
                }
            }

            if (!invalidMetrics.isEmpty()) {
                throw IllegalArgumentException(unrecognized(request, invalidMetrics, METRICS.keys, "metric"))
            }
        }
        return scheduledJobsStatsRequest
    }
}
