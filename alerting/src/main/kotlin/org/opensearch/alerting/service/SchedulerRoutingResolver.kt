/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

/**
 * Resolves external-scheduler routing info from plugin settings, applying an optional
 * per-request ThreadContext override for the account id.
 *
 * Pure function — no ThreadContext or Settings access — so it is trivially unit-testable
 * and is shared by [org.opensearch.alerting.transport.TransportIndexMonitorAction] and
 * [org.opensearch.alerting.transport.TransportDeleteMonitorAction].
 */
object SchedulerRoutingResolver {

    /** Routing info for external scheduler operations. */
    data class Routing(val accountId: String, val queueName: String, val roleArn: String)

    fun resolve(
        settingsAccountId: String,
        settingsQueueName: String,
        settingsRoleArn: String,
        threadContextAccountIdOverride: String?
    ): Routing? {
        val accountId = pickAccountId(settingsAccountId, threadContextAccountIdOverride) ?: return null
        val queueName = settingsQueueName.takeIf { it.isNotBlank() } ?: return null
        val roleArn = settingsRoleArn.takeIf { it.isNotBlank() } ?: return null
        return Routing(accountId, queueName, roleArn)
    }

    /** Delete only needs accountId + roleArn; queueName is set to empty. */
    fun resolveForDelete(
        settingsAccountId: String,
        settingsRoleArn: String,
        threadContextAccountIdOverride: String?
    ): Routing? {
        val accountId = pickAccountId(settingsAccountId, threadContextAccountIdOverride) ?: return null
        val roleArn = settingsRoleArn.takeIf { it.isNotBlank() } ?: return null
        return Routing(accountId, "", roleArn)
    }

    /** ThreadContext override wins; falls back to plugin setting; null if both are blank. */
    private fun pickAccountId(settingValue: String, override: String?): String? {
        if (!override.isNullOrBlank()) return override
        return settingValue.takeIf { it.isNotBlank() }
    }
}
