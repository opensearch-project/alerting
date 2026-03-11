package org.opensearch.alerting.core.resthandler

import org.opensearch.alerting.core.action.node.ScheduledJobsStatsRequest
import org.opensearch.core.common.Strings
import org.opensearch.rest.RestRequest
import java.util.Locale
import java.util.TreeSet

internal object StatsRequestUtils {

    const val JOB_SCHEDULING_METRICS: String = "job_scheduling_metrics"
    const val JOBS_INFO: String = "jobs_info"
    val METRICS = mapOf<String, (ScheduledJobsStatsRequest) -> Unit>(
        JOB_SCHEDULING_METRICS to { it.jobSchedulingMetrics = true },
        JOBS_INFO to { it.jobsInfo = true }
    )

    fun getStatsRequest(
        request: RestRequest,
        showAlertingV2ScheduledJobs: Boolean?,
        unrecognizedFn: (RestRequest, Set<String>, Set<String>, String) -> String
    ): ScheduledJobsStatsRequest {
        val nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"))
        val metrics = Strings.tokenizeByCommaToSet(request.param("metric"))
        val scheduledJobsStatsRequest = ScheduledJobsStatsRequest(
            nodeIds = nodesIds,
            showAlertingV2ScheduledJobs = showAlertingV2ScheduledJobs
        )
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
                throw IllegalArgumentException(unrecognizedFn(request, invalidMetrics, METRICS.keys, "metric"))
            }
        }
        return scheduledJobsStatsRequest
    }
}
