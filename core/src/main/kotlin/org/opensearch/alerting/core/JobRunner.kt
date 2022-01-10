/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core

import org.opensearch.alerting.core.model.ScheduledJob
import java.time.Instant

interface JobRunner {
    fun postDelete(jobId: String)

    fun postIndex(job: ScheduledJob)

    fun runJob(job: ScheduledJob, periodStart: Instant, periodEnd: Instant)
}
