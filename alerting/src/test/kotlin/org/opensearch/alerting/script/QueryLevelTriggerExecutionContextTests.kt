/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.script

import org.junit.Assert
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.alerting.randomQueryLevelMonitorRunResult
import org.opensearch.alerting.randomQueryLevelTrigger
import org.opensearch.test.OpenSearchTestCase

@Suppress("UNCHECKED_CAST")
class QueryLevelTriggerExecutionContextTests : OpenSearchTestCase() {

    fun `test results are excluded from query-level context`() {
        val monitor = randomQueryLevelMonitor()
        val trigger = randomQueryLevelTrigger()
        val result = randomQueryLevelMonitorRunResult()
        val context = QueryLevelTriggerExecutionContext(monitor, trigger, result)
        val templateArgs = context.asTemplateArg()
        Assert.assertTrue(templateArgs.containsKey("results"))
        Assert.assertTrue(context.results.isNullOrEmpty())
    }
}
