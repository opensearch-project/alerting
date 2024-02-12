/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.workflow

import org.opensearch.alerting.ALWAYS_RUN
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.randomAction
import org.opensearch.alerting.randomBucketLevelMonitor
import org.opensearch.alerting.randomQueryLevelTrigger
import org.opensearch.alerting.randomTemplateScript
import org.opensearch.alerting.randomWorkflow
import org.opensearch.commons.alerting.model.Workflow

class CompositeWorkflowRunnerIT : AlertingRestTestCase() {
    fun `test streaming bucket level monitor throws unsupported operation exception`() {
        val action = randomAction(template = randomTemplateScript("Hello {{ctx.monitor.name}}"), destinationId = createDestination().id)
        val monitor = randomBucketLevelMonitor(
            triggers = listOf(randomQueryLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action)))
        )

        val workflow: Workflow = randomWorkflow(monitorIds = listOf(monitor.id))

        // TODO
        // val response = executeWorkflow(monitor, params = DRYRUN_MONITOR)
    }
}
