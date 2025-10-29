/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow

object AlertingV2Utils {
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
}
