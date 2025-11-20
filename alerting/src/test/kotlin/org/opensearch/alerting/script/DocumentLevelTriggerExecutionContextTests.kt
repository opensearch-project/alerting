/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.script

import org.junit.Assert
import org.opensearch.alerting.randomDocumentLevelMonitor
import org.opensearch.alerting.randomDocumentLevelMonitorRunResult
import org.opensearch.alerting.randomDocumentLevelTrigger
import org.opensearch.alerting.randomUser
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.test.OpenSearchTestCase

@Suppress("UNCHECKED_CAST")
class DocumentLevelTriggerExecutionContextTests : OpenSearchTestCase() {
    private lateinit var clusterSettings: ClusterSettings

    fun `test results are excluded from document-level context when allowed roles are not set`() {
        val settings = Settings.builder().build()
        val settingSet = hashSetOf<Setting<*>>()
        settingSet.add(AlertingSettings.NOTIFICATION_CONTEXT_RESULTS_ALLOWED_ROLES)
        clusterSettings = ClusterSettings(settings, settingSet)

        val monitor = randomDocumentLevelMonitor()
        val trigger = randomDocumentLevelTrigger()
        val result = randomDocumentLevelMonitorRunResult(listOf(mapOf("foo" to "bar")))
        Assert.assertFalse(result.inputResults.results.isNullOrEmpty())
        val context = DocumentLevelTriggerExecutionContext(monitor, trigger, clusterSettings = clusterSettings)
        Assert.assertTrue(context.results.isNullOrEmpty())
    }

    fun `test results are excluded from document-level context when allowed roles is empty`() {
        val settings = Settings
            .builder()
            .putList("plugins.alerting.notification_context_results_allowed_roles", listOf("role1"))
            .build()
        val settingSet = hashSetOf<Setting<*>>()
        settingSet.add(AlertingSettings.NOTIFICATION_CONTEXT_RESULTS_ALLOWED_ROLES)
        clusterSettings = ClusterSettings(settings, settingSet)

        val monitor = randomDocumentLevelMonitor(user = randomUser(listOf("role2")))
        val trigger = randomDocumentLevelTrigger()
        val result = randomDocumentLevelMonitorRunResult(listOf(mapOf("foo" to "bar")))
        Assert.assertFalse(result.inputResults.results.isNullOrEmpty())
        val context = DocumentLevelTriggerExecutionContext(monitor, trigger, clusterSettings = clusterSettings)
        Assert.assertTrue(context.results.isNullOrEmpty())
    }

    fun `test results are excluded in document-level context even when allowed roles intersect monitor roles`() {
        val settings = Settings
            .builder()
            .putList("plugins.alerting.notification_context_results_allowed_roles", listOf("role1", "role2"))
            .build()
        val settingSet = hashSetOf<Setting<*>>()
        settingSet.add(AlertingSettings.NOTIFICATION_CONTEXT_RESULTS_ALLOWED_ROLES)
        clusterSettings = ClusterSettings(settings, settingSet)

        val monitor = randomDocumentLevelMonitor(user = randomUser(listOf("role1")))
        val trigger = randomDocumentLevelTrigger()
        val result = randomDocumentLevelMonitorRunResult(listOf(mapOf("foo" to "bar")))
        Assert.assertFalse(result.inputResults.results.isNullOrEmpty())
        val context = DocumentLevelTriggerExecutionContext(monitor, trigger, clusterSettings = clusterSettings)
        Assert.assertTrue(context.results.isNullOrEmpty())
    }
}
