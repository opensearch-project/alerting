package org.opensearch.alerting

import org.opensearch.alerting.core.modelv2.MonitorV2
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow

object AlertingV2Utils {
    // Validates that the given scheduled job is a Monitor
    // returns the exception to pass into actionListener.onFailure if not.
    fun validateMonitorV1(scheduledJob: ScheduledJob): Exception? {
        if (scheduledJob is MonitorV2) {
            return IllegalStateException("The ID given corresponds to a V2 Monitor, but a V1 Monitor was expected")
        } else if (scheduledJob !is Monitor && scheduledJob !is Workflow) {
            return IllegalStateException("The ID given corresponds to a scheduled job of unknown type: ${scheduledJob.javaClass.name}")
        }
        return null
    }

    // Validates that the given scheduled job is a MonitorV2
    // returns the exception to pass into actionListener.onFailure if not.
    fun validateMonitorV2(scheduledJob: ScheduledJob): Exception? {
        if (scheduledJob is Monitor || scheduledJob is Workflow) {
            return IllegalStateException("The ID given corresponds to a V1 Monitor, but a V2 Monitor was expected")
        } else if (scheduledJob !is MonitorV2) {
            return IllegalStateException("The ID given corresponds to a scheduled job of unknown type: ${scheduledJob.javaClass.name}")
        }
        return null
    }

    fun getIndicesFromPplQuery(pplQuery: String): List<String> {
        // captures comma-separated concrete indices, index patterns, and index aliases
        val indicesRegex = """(?i)source(?:\s*)=(?:\s*)([-\w.*'+]+(?:\*)?(?:\s*,\s*[-\w.*'+]+\*?)*)\s*\|*""".toRegex()

        // use find() instead of findAll() because a PPL query only ever has one source statement
        // the only capture group specified in the regex captures the comma separated list of indices/index patterns
        val indices = indicesRegex.find(pplQuery)?.groupValues?.get(1)?.split(",")?.map { it.trim() }
            ?: throw IllegalStateException(
                "Could not find indices that PPL Monitor query searches even " +
                    "after validating the query through SQL/PPL plugin"
            )

        return indices
    }
}
