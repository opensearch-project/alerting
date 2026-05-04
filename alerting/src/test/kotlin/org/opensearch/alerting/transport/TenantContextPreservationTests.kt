/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import com.carrotsearch.randomizedtesting.ThreadFilter
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.opensearch.alerting.AlertingPlugin.Companion.TENANT_ID_HEADER
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.utils.TenantContext
import org.opensearch.commons.utils.currentTenantId
import org.opensearch.test.OpenSearchTestCase
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * Tests that verify TenantContext preserves the tenant ID across
 * thread context stashes and coroutine scope launches, matching
 * the pattern used in alerting TransportActions.
 */
@ThreadLeakFilters(filters = [TenantContextPreservationTests.CoroutineThreadFilter::class])
class TenantContextPreservationTests : OpenSearchTestCase() {

    class CoroutineThreadFilter : ThreadFilter {
        override fun reject(t: Thread): Boolean = t.name.startsWith("DefaultDispatcher-worker")
    }

    private val scope = CoroutineScope(Dispatchers.IO)

    fun `test tenantId preserved after stashContext via TenantContext`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        val expectedTenantId = "tenant-123:scope-abc"
        threadContext.putHeader(TENANT_ID_HEADER, expectedTenantId)

        // Capture tenantId before stash (as TransportActions do)
        val tenantId = threadContext.getHeader(TENANT_ID_HEADER)

        val result = AtomicReference<String?>()
        val latch = CountDownLatch(1)

        // Stash context (header is now gone from threadContext)
        threadContext.stashContext().use {
            assertNull(threadContext.getHeader(TENANT_ID_HEADER))

            // Launch with TenantContext (as TransportActions do)
            scope.launch(TenantContext(tenantId)) {
                result.set(currentTenantId())
                latch.countDown()
            }
        }

        assertTrue(latch.await(2, TimeUnit.SECONDS))
        assertEquals(expectedTenantId, result.get())
    }

    fun `test null tenantId preserved when header absent`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        // No header set

        val tenantId = threadContext.getHeader(TENANT_ID_HEADER)
        assertNull(tenantId)

        val result = AtomicReference<String?>("sentinel")
        val latch = CountDownLatch(1)

        threadContext.stashContext().use {
            scope.launch(TenantContext(tenantId)) {
                result.set(currentTenantId())
                latch.countDown()
            }
        }

        assertTrue(latch.await(2, TimeUnit.SECONDS))
        assertNull(result.get())
    }

    fun `test concurrent requests have isolated tenantIds`() {
        val result1 = AtomicReference<String?>()
        val result2 = AtomicReference<String?>()
        val latch = CountDownLatch(2)

        scope.launch(TenantContext("tenant-a")) {
            // Simulate some work
            result1.set(currentTenantId())
            latch.countDown()
        }

        scope.launch(TenantContext("tenant-b")) {
            result2.set(currentTenantId())
            latch.countDown()
        }

        assertTrue(latch.await(2, TimeUnit.SECONDS))
        assertEquals("tenant-a", result1.get())
        assertEquals("tenant-b", result2.get())
    }

    fun `test tenantId available in nested suspend functions`() {
        val expectedTenantId = "tenant-nested"
        val result = AtomicReference<String?>()
        val latch = CountDownLatch(1)

        scope.launch(TenantContext(expectedTenantId)) {
            val id = nestedSuspendFunction()
            result.set(id)
            latch.countDown()
        }

        assertTrue(latch.await(2, TimeUnit.SECONDS))
        assertEquals(expectedTenantId, result.get())
    }

    private suspend fun nestedSuspendFunction(): String? {
        return deeperSuspendFunction()
    }

    private suspend fun deeperSuspendFunction(): String? {
        return currentTenantId()
    }
}
