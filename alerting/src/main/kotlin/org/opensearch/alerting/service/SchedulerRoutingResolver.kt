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
    data class Routing(val accountId: String, val queueName: String, val roleArn: String, val executionRoleArn: String)

    fun resolve(
        settingsAccountId: String,
        settingsQueueName: String,
        settingsRoleName: String,
        settingsExecutionRoleName: String? = null,
        threadContextAccountIdOverride: String?
    ): Routing {
        val accountId = pickAccountId(settingsAccountId, threadContextAccountIdOverride)
            ?: error("External scheduler account ID is not configured and no override was provided")
        val queueName = settingsQueueName.takeIf { it.isNotBlank() }
            ?: error("External scheduler queue name is not configured")
        val roleName = settingsRoleName.takeIf { it.isNotBlank() }
            ?: error("External scheduler role name is not configured")
        val executionRoleName = settingsExecutionRoleName?.takeIf { it.isNotBlank() }
            ?: error("External scheduler execution role name is not configured")
        return Routing(accountId, queueName, buildRoleArn(accountId, roleName), buildRoleArn(accountId, executionRoleName))
    }

    /** Delete only needs accountId + roleArn; queueName is set to empty. */
    fun resolveForDelete(
        settingsAccountId: String,
        settingsRoleName: String,
        threadContextAccountIdOverride: String?
    ): Routing {
        val accountId = pickAccountId(settingsAccountId, threadContextAccountIdOverride)
            ?: error("External scheduler account ID is not configured and no override was provided")
        val roleName = settingsRoleName.takeIf { it.isNotBlank() }
            ?: error("External scheduler role name is not configured")
        return Routing(accountId, "", buildRoleArn(accountId, roleName), "")
    }

    /** ThreadContext override wins; falls back to plugin setting; null if both are blank. */
    private fun pickAccountId(settingValue: String, override: String?): String? {
        if (!override.isNullOrBlank()) return override
        return settingValue.takeIf { it.isNotBlank() }
    }

    /** Constructs an IAM role ARN from account ID and role name. */
    private fun buildRoleArn(accountId: String, roleName: String): String =
        "arn:aws:iam::$accountId:role/$roleName"
}
