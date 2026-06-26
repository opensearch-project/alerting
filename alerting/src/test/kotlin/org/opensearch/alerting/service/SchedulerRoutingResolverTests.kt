/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Test

class SchedulerRoutingResolverTests {

    private val acct = "111111111111"
    private val override = "999999999999"
    private val queue = "my-queue"
    private val roleName = "eb-role"
    private val execRoleName = "eb-exec-role"
    private val allowList = listOf(acct, override)

    // ---------- resolve() — create/update path ----------

    @Test fun `resolve uses plugin settings when no override`() {
        val r = SchedulerRoutingResolver.resolve(acct, queue, roleName, execRoleName, threadContextAccountIdOverride = null)
        assertEquals(acct, r.accountId)
        assertEquals(queue, r.queueName)
        assertEquals("arn:aws:iam::$acct:role/$roleName", r.roleArn)
        assertEquals("arn:aws:iam::$acct:role/$execRoleName", r.executionRoleArn)
    }

    @Test fun `resolve applies ThreadContext override for accountId and constructs ARN with override`() {
        val r = SchedulerRoutingResolver.resolve(acct, queue, roleName, execRoleName, threadContextAccountIdOverride = override, allowedAccountIds = allowList)
        assertEquals(override, r.accountId)
        assertEquals("arn:aws:iam::$override:role/$roleName", r.roleArn)
        assertEquals("arn:aws:iam::$override:role/$execRoleName", r.executionRoleArn)
    }

    @Test fun `resolve treats blank override as absent`() {
        val r = SchedulerRoutingResolver.resolve(acct, queue, roleName, execRoleName, threadContextAccountIdOverride = "   ")
        assertEquals(acct, r.accountId)
    }

    @Test(expected = IllegalStateException::class)
    fun `resolve throws when accountId missing in both setting and override`() {
        SchedulerRoutingResolver.resolve("", queue, roleName, execRoleName, threadContextAccountIdOverride = null)
    }

    @Test fun `resolve still succeeds when setting blank but override provided`() {
        val r = SchedulerRoutingResolver.resolve("", queue, roleName, execRoleName, threadContextAccountIdOverride = override, allowedAccountIds = allowList)
        assertEquals(override, r.accountId)
    }

    @Test(expected = IllegalStateException::class)
    fun `resolve throws when queueName blank`() {
        SchedulerRoutingResolver.resolve(acct, "", roleName, execRoleName, threadContextAccountIdOverride = null)
    }

    @Test(expected = IllegalStateException::class)
    fun `resolve throws when roleName blank`() {
        SchedulerRoutingResolver.resolve(acct, queue, "", execRoleName, threadContextAccountIdOverride = null)
    }

    @Test(expected = IllegalStateException::class)
    fun `resolve throws when executionRoleName blank`() {
        SchedulerRoutingResolver.resolve(acct, queue, roleName, "  ", threadContextAccountIdOverride = null)
    }

    @Test(expected = IllegalStateException::class)
    fun `resolve throws when executionRoleName omitted`() {
        SchedulerRoutingResolver.resolve(acct, queue, roleName, threadContextAccountIdOverride = null)
    }

    // ---------- resolveForDelete() ----------

    @Test fun `resolveForDelete uses plugin settings when no override`() {
        val r = SchedulerRoutingResolver.resolveForDelete(acct, roleName, threadContextAccountIdOverride = null)
        assertEquals(acct, r.accountId)
        assertEquals("arn:aws:iam::$acct:role/$roleName", r.roleArn)
    }

    @Test fun `resolveForDelete applies ThreadContext override and constructs ARN with override`() {
        val r = SchedulerRoutingResolver.resolveForDelete(acct, roleName, threadContextAccountIdOverride = override, allowedAccountIds = allowList)
        assertEquals(override, r.accountId)
        assertEquals("arn:aws:iam::$override:role/$roleName", r.roleArn)
    }

    @Test(expected = IllegalStateException::class)
    fun `resolveForDelete throws when accountId missing`() {
        SchedulerRoutingResolver.resolveForDelete("", roleName, threadContextAccountIdOverride = null)
    }

    @Test(expected = IllegalStateException::class)
    fun `resolveForDelete throws when roleName missing`() {
        SchedulerRoutingResolver.resolveForDelete(acct, "", threadContextAccountIdOverride = null)
    }

    @Test fun `resolveForDelete does not require queueName`() {
        val r = SchedulerRoutingResolver.resolveForDelete(acct, roleName, threadContextAccountIdOverride = null)
        assertEquals(acct, r.accountId)
    }

    // ---------- allow-list validation ----------

    @Test fun `resolve succeeds when override is in allow-list`() {
        val r = SchedulerRoutingResolver.resolve(acct, queue, roleName, execRoleName, threadContextAccountIdOverride = override, allowedAccountIds = allowList)
        assertEquals(override, r.accountId)
    }

    @Test fun `resolve does not validate settings-based account against allow-list`() {
        val r = SchedulerRoutingResolver.resolve(acct, queue, roleName, execRoleName, threadContextAccountIdOverride = null, allowedAccountIds = listOf(override))
        assertEquals(acct, r.accountId)
    }

    @Test fun `resolve rejects override not in allow-list`() {
        try {
            SchedulerRoutingResolver.resolve(acct, queue, roleName, execRoleName, threadContextAccountIdOverride = override, allowedAccountIds = listOf(acct))
            fail("Expected IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            assert(e.message!!.contains("not in allowed_account_ids"))
        }
    }

    @Test fun `resolve rejects override when allow-list is empty (fail-closed)`() {
        try {
            SchedulerRoutingResolver.resolve(acct, queue, roleName, execRoleName, threadContextAccountIdOverride = override, allowedAccountIds = emptyList())
            fail("Expected IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            assert(e.message!!.contains("allowed_account_ids is not configured"))
        }
    }

    @Test fun `resolve allows settings-based account when allow-list is empty and no override`() {
        val r = SchedulerRoutingResolver.resolve(acct, queue, roleName, execRoleName, threadContextAccountIdOverride = null, allowedAccountIds = emptyList())
        assertEquals(acct, r.accountId)
    }

    @Test fun `resolveForDelete rejects override not in allow-list`() {
        try {
            SchedulerRoutingResolver.resolveForDelete(acct, roleName, threadContextAccountIdOverride = override, allowedAccountIds = listOf(acct))
            fail("Expected IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            assert(e.message!!.contains("not in allowed_account_ids"))
        }
    }

    @Test fun `resolveForDelete rejects override when allow-list is empty (fail-closed)`() {
        try {
            SchedulerRoutingResolver.resolveForDelete(acct, roleName, threadContextAccountIdOverride = override, allowedAccountIds = emptyList())
            fail("Expected IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            assert(e.message!!.contains("allowed_account_ids is not configured"))
        }
    }
}
