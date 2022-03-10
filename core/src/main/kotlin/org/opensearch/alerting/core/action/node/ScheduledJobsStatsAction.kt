/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.action.node

import org.opensearch.action.ActionType
import org.opensearch.common.io.stream.Writeable

class ScheduledJobsStatsAction : ActionType<ScheduledJobsStatsResponse>(NAME, reader) {
    companion object {
        val INSTANCE = ScheduledJobsStatsAction()
        const val NAME = "cluster:admin/opendistro/_scheduled_jobs/stats"

        val reader = Writeable.Reader {
            val response = ScheduledJobsStatsResponse(it)
            response
        }
    }

    override fun getResponseReader(): Writeable.Reader<ScheduledJobsStatsResponse> {
        return reader
    }
}
