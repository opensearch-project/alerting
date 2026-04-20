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

    /** Full routing info needed by create/update. Null when any required field is blank. */
    data class Routing(val accountId: String, val queueArn: String, val roleArn: String)

    /** Delete path only needs accountId + roleArn. Null when either is blank. */
    data class DeleteRouting(val accountId: String, val roleArn: String)

    fun resolve(
        settingsAccountId: String,
        settingsQueueArn: String,
        settingsRoleArn: String,
        threadContextAccountIdOverride: String?
    ): Routing? {
        val accountId = pickAccountId(settingsAccountId, threadContextAccountIdOverride) ?: return null
        val queueArn = settingsQueueArn.takeIf { it.isNotBlank() } ?: return null
        val roleArn = settingsRoleArn.takeIf { it.isNotBlank() } ?: return null
        return Routing(accountId, queueArn, roleArn)
    }

    fun resolveForDelete(
        settingsAccountId: String,
        settingsRoleArn: String,
        threadContextAccountIdOverride: String?
    ): DeleteRouting? {
        val accountId = pickAccountId(settingsAccountId, threadContextAccountIdOverride) ?: return null
        val roleArn = settingsRoleArn.takeIf { it.isNotBlank() } ?: return null
        return DeleteRouting(accountId, roleArn)
    }

    /** ThreadContext override wins; falls back to plugin setting; null if both are blank. */
    private fun pickAccountId(settingValue: String, override: String?): String? {
        if (!override.isNullOrBlank()) return override
        return settingValue.takeIf { it.isNotBlank() }
    }
}
