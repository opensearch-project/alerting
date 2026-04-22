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
    private val queue = "arn:aws:sqs:us-east-1:111:queue"
    private val role = "arn:aws:iam::111:role/eb"

    // ---------- resolve() — create/update path ----------

    @Test fun `resolve uses plugin settings when no override`() {
        val r = SchedulerRoutingResolver.resolve(acct, queue, role, threadContextAccountIdOverride = null)!!
        assertEquals(acct, r.accountId)
        assertEquals(queue, r.queueArn)
        assertEquals(role, r.roleArn)
    }

    @Test fun `resolve applies ThreadContext override for accountId`() {
        val r = SchedulerRoutingResolver.resolve(acct, queue, role, threadContextAccountIdOverride = override)!!
        assertEquals(override, r.accountId)
    }

    @Test fun `resolve treats blank override as absent`() {
        val r = SchedulerRoutingResolver.resolve(acct, queue, role, threadContextAccountIdOverride = "   ")!!
        assertEquals(acct, r.accountId)
    }

    @Test fun `resolve returns null when accountId missing in both setting and override`() {
        assertNull(SchedulerRoutingResolver.resolve("", queue, role, threadContextAccountIdOverride = null))
        assertNull(SchedulerRoutingResolver.resolve("", queue, role, threadContextAccountIdOverride = ""))
    }

    @Test fun `resolve still succeeds when setting blank but override provided`() {
        val r = SchedulerRoutingResolver.resolve("", queue, role, threadContextAccountIdOverride = override)!!
        assertEquals(override, r.accountId)
    }

    @Test fun `resolve returns null when queueArn blank`() {
        assertNull(SchedulerRoutingResolver.resolve(acct, "", role, threadContextAccountIdOverride = null))
    }

    @Test fun `resolve returns null when roleArn blank`() {
        assertNull(SchedulerRoutingResolver.resolve(acct, queue, "", threadContextAccountIdOverride = null))
    }

    // ---------- resolveForDelete() ----------

    @Test fun `resolveForDelete uses plugin settings when no override`() {
        val r = SchedulerRoutingResolver.resolveForDelete(acct, role, threadContextAccountIdOverride = null)!!
        assertEquals(acct, r.accountId)
        assertEquals(role, r.roleArn)
    }

    @Test fun `resolveForDelete applies ThreadContext override`() {
        val r = SchedulerRoutingResolver.resolveForDelete(acct, role, threadContextAccountIdOverride = override)!!
        assertEquals(override, r.accountId)
    }

    @Test fun `resolveForDelete returns null when accountId missing`() {
        assertNull(SchedulerRoutingResolver.resolveForDelete("", role, threadContextAccountIdOverride = null))
    }

    @Test fun `resolveForDelete returns null when roleArn missing`() {
        assertNull(SchedulerRoutingResolver.resolveForDelete(acct, "", threadContextAccountIdOverride = null))
    }

    @Test fun `resolveForDelete does not require queueArn`() {
        // No queueArn parameter at all — setup covers delete path independent of create/update
        val r = SchedulerRoutingResolver.resolveForDelete(acct, role, threadContextAccountIdOverride = null)
        assertEquals(acct, r?.accountId)
    }
}
