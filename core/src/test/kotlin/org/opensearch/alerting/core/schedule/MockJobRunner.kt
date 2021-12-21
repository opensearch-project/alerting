/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.schedule

import org.opensearch.alerting.core.JobRunner
import org.opensearch.alerting.core.model.ScheduledJob
import java.time.Instant

class MockJobRunner : JobRunner {
    var numberOfRun: Int = 0
        private set
    var numberOfIndex: Int = 0
        private set
    var numberOfDelete: Int = 0
        private set

    override fun postDelete(jobId: String) {
        numberOfDelete++
    }

    override fun postIndex(job: ScheduledJob) {
        numberOfIndex++
    }

    override fun runJob(job: ScheduledJob, periodStart: Instant, periodEnd: Instant) {
        numberOfRun++
    }
}
