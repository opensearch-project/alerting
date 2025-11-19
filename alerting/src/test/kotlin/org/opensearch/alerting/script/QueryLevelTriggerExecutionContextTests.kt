/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.script

import org.junit.Assert
import org.junit.Before
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.alerting.randomQueryLevelMonitorRunResult
import org.opensearch.alerting.randomQueryLevelTrigger
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.test.OpenSearchTestCase

@Suppress("UNCHECKED_CAST")
class QueryLevelTriggerExecutionContextTests : OpenSearchTestCase() {
    private lateinit var clusterSettings: ClusterSettings

    @Before
    fun setup() {
        val settings = Settings.builder().build()
        val settingSet = hashSetOf<Setting<*>>()
        clusterSettings = ClusterSettings(settings, settingSet)
    }

    fun `test results are excluded from query-level context`() {
        val monitor = randomQueryLevelMonitor()
        val trigger = randomQueryLevelTrigger()
        val result = randomQueryLevelMonitorRunResult()
        val context = QueryLevelTriggerExecutionContext(monitor, trigger, result, clusterSettings = clusterSettings)
        val templateArgs = context.asTemplateArg()
        Assert.assertTrue(templateArgs.containsKey("results"))
        Assert.assertTrue(context.results.isNullOrEmpty())
    }
}
