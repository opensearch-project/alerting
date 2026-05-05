/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class SchedulerRoutingResolverTests {

    private val acct = "111111111111"
    private val override = "999999999999"
    private val queue = "my-queue"
    private val roleName = "eb-role"
    private val execRoleName = "eb-exec-role"

    // ---------- resolve() — create/update path ----------

    @Test fun `resolve uses plugin settings when no override`() {
        val r = SchedulerRoutingResolver.resolve(acct, queue, roleName, execRoleName, threadContextAccountIdOverride = null)!!
        assertEquals(acct, r.accountId)
        assertEquals(queue, r.queueName)
        assertEquals("arn:aws:iam::$acct:role/$roleName", r.roleArn)
        assertEquals("arn:aws:iam::$acct:role/$execRoleName", r.executionRoleArn)
    }

    @Test fun `resolve applies ThreadContext override for accountId and constructs ARN with override`() {
        val r = SchedulerRoutingResolver.resolve(acct, queue, roleName, execRoleName, threadContextAccountIdOverride = override)!!
        assertEquals(override, r.accountId)
        assertEquals("arn:aws:iam::$override:role/$roleName", r.roleArn)
        assertEquals("arn:aws:iam::$override:role/$execRoleName", r.executionRoleArn)
    }

    @Test fun `resolve treats blank override as absent`() {
        val r = SchedulerRoutingResolver.resolve(acct, queue, roleName, execRoleName, threadContextAccountIdOverride = "   ")!!
        assertEquals(acct, r.accountId)
    }

    @Test fun `resolve returns null when accountId missing in both setting and override`() {
        assertNull(SchedulerRoutingResolver.resolve("", queue, roleName, execRoleName, threadContextAccountIdOverride = null))
        assertNull(SchedulerRoutingResolver.resolve("", queue, roleName, execRoleName, threadContextAccountIdOverride = ""))
    }

    @Test fun `resolve still succeeds when setting blank but override provided`() {
        val r = SchedulerRoutingResolver.resolve("", queue, roleName, execRoleName, threadContextAccountIdOverride = override)!!
        assertEquals(override, r.accountId)
    }

    @Test fun `resolve returns null when queueName blank`() {
        assertNull(SchedulerRoutingResolver.resolve(acct, "", roleName, execRoleName, threadContextAccountIdOverride = null))
    }

    @Test fun `resolve returns null when roleName blank`() {
        assertNull(SchedulerRoutingResolver.resolve(acct, queue, "", execRoleName, threadContextAccountIdOverride = null))
    }

    @Test fun `resolve returns null when executionRoleName blank`() {
        assertNull(SchedulerRoutingResolver.resolve(acct, queue, roleName, "  ", threadContextAccountIdOverride = null))
    }

    @Test fun `resolve returns null when executionRoleName omitted`() {
        assertNull(SchedulerRoutingResolver.resolve(acct, queue, roleName, threadContextAccountIdOverride = null))
    }

    // ---------- resolveForDelete() ----------

    @Test fun `resolveForDelete uses plugin settings when no override`() {
        val r = SchedulerRoutingResolver.resolveForDelete(acct, roleName, threadContextAccountIdOverride = null)!!
        assertEquals(acct, r.accountId)
        assertEquals("arn:aws:iam::$acct:role/$roleName", r.roleArn)
    }

    @Test fun `resolveForDelete applies ThreadContext override and constructs ARN with override`() {
        val r = SchedulerRoutingResolver.resolveForDelete(acct, roleName, threadContextAccountIdOverride = override)!!
        assertEquals(override, r.accountId)
        assertEquals("arn:aws:iam::$override:role/$roleName", r.roleArn)
    }

    @Test fun `resolveForDelete returns null when accountId missing`() {
        assertNull(SchedulerRoutingResolver.resolveForDelete("", roleName, threadContextAccountIdOverride = null))
    }

    @Test fun `resolveForDelete returns null when roleName missing`() {
        assertNull(SchedulerRoutingResolver.resolveForDelete(acct, "", threadContextAccountIdOverride = null))
    }

    @Test fun `resolveForDelete does not require queueName`() {
        val r = SchedulerRoutingResolver.resolveForDelete(acct, roleName, threadContextAccountIdOverride = null)
        assertEquals(acct, r?.accountId)
    }
}
