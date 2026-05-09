/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import com.carrotsearch.randomizedtesting.ThreadFilter
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.opensearch.action.ActionType
import org.opensearch.action.index.IndexRequest
import org.opensearch.alerting.AlertingPlugin.Companion.TENANT_ID_HEADER
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.utils.SecureClientWrapper
import org.opensearch.commons.utils.TenantContext
import org.opensearch.commons.utils.currentTenantId
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.mockito.Mockito.`when` as whenever

/**
 * Tests that verify the tenant ID propagates through the full notification path:
 * TenantContext (coroutine) → runAction stash + re-inject → NotificationsPluginInterface → SecureClientWrapper → transport call
 */
@ThreadLeakFilters(filters = [MonitorRunnerNotificationTenantTests.CoroutineThreadFilter::class])
class MonitorRunnerNotificationTenantTests : OpenSearchTestCase() {

    class CoroutineThreadFilter : ThreadFilter {
        override fun reject(t: Thread): Boolean = t.name.startsWith("DefaultDispatcher-worker")
    }

    private val scope = CoroutineScope(Dispatchers.IO)

    /**
     * Simulates the QueryLevelMonitorRunner / BucketLevelMonitorRunner path:
     * 1. Coroutine launched with TenantContext("tenant-xyz")
     * 2. runAction reads currentTenantId(), stashes context, re-injects header
     * 3. NotificationsPluginInterface.sendNotification uses SecureClientWrapper
     * 4. SecureClientWrapper preserves tenant header across its own stash
     * 5. The transport execute call receives the tenant header
     */
    fun `test tenant id reaches transport execute via query level runner path`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        val client = createMockNodeClient(threadContext)
        val expectedTenantId = "tenant-query-level"

        val capturedTenantId = AtomicReference<String?>()
        val latch = CountDownLatch(1)

        // Mock the execute call to capture what headers are present
        whenever(client.execute(any<ActionType<ActionResponse>>(), any(), any<ActionListener<ActionResponse>>())).thenAnswer { invocation ->
            capturedTenantId.set(threadContext.getHeader(TENANT_ID_HEADER))
            latch.countDown()
            null
        }

        // Simulate: TransportExecuteMonitorAction launches coroutine with TenantContext
        scope.launch(TenantContext(expectedTenantId)) {
            // Simulate: MonitorRunner.runAction() pattern
            val tenantId = currentTenantId()
            val userStr = "user|backend_role"

            threadContext.stashContext().use {
                threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, userStr)
                tenantId?.let { threadContext.putHeader(TENANT_ID_HEADER, it) }

                // Simulate: NotificationsPluginInterface.sendNotification via SecureClientWrapper
                val wrapper = SecureClientWrapper(client)
                wrapper.execute(
                    Mockito.mock(ActionType::class.java) as ActionType<ActionResponse>,
                    IndexRequest("test-index"),
                    Mockito.mock(ActionListener::class.java) as ActionListener<ActionResponse>
                )
            }
        }

        assertTrue("Timed out waiting for execute call", latch.await(3, TimeUnit.SECONDS))
        assertEquals(expectedTenantId, capturedTenantId.get())
    }

    fun `test tenant id reaches transport execute via bucket level runner path`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        val client = createMockNodeClient(threadContext)
        val expectedTenantId = "tenant-bucket-level"

        val capturedTenantId = AtomicReference<String?>()
        val latch = CountDownLatch(1)

        whenever(client.execute(any<ActionType<ActionResponse>>(), any(), any<ActionListener<ActionResponse>>())).thenAnswer { invocation ->
            capturedTenantId.set(threadContext.getHeader(TENANT_ID_HEADER))
            latch.countDown()
            null
        }

        // Same pattern as query level - bucket level runner also calls MonitorRunner.runAction
        scope.launch(TenantContext(expectedTenantId)) {
            val tenantId = currentTenantId()
            val userStr = "bucket-user|role1,role2"

            threadContext.stashContext().use {
                threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, userStr)
                tenantId?.let { threadContext.putHeader(TENANT_ID_HEADER, it) }

                val wrapper = SecureClientWrapper(client)
                wrapper.execute(
                    Mockito.mock(ActionType::class.java) as ActionType<ActionResponse>,
                    IndexRequest("test-index"),
                    Mockito.mock(ActionListener::class.java) as ActionListener<ActionResponse>
                )
            }
        }

        assertTrue("Timed out waiting for execute call", latch.await(3, TimeUnit.SECONDS))
        assertEquals(expectedTenantId, capturedTenantId.get())
    }

    fun `test security context is stashed but tenant id survives double stash`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        val client = createMockNodeClient(threadContext)
        val expectedTenantId = "tenant-double-stash"

        // Set security headers that should be stashed
        threadContext.putHeader("_opendistro_security_user", "admin|backend_role")
        threadContext.putHeader(TENANT_ID_HEADER, "original-will-be-stashed")

        val capturedTenantId = AtomicReference<String?>()
        val capturedSecurityHeader = AtomicReference<String?>("not-null")
        val latch = CountDownLatch(1)

        whenever(client.execute(any<ActionType<ActionResponse>>(), any(), any<ActionListener<ActionResponse>>())).thenAnswer {
            capturedTenantId.set(threadContext.getHeader(TENANT_ID_HEADER))
            capturedSecurityHeader.set(threadContext.getHeader("_opendistro_security_user"))
            latch.countDown()
            null
        }

        scope.launch(TenantContext(expectedTenantId)) {
            val tenantId = currentTenantId()

            // First stash (MonitorRunner.runAction)
            threadContext.stashContext().use {
                tenantId?.let { threadContext.putHeader(TENANT_ID_HEADER, it) }

                // Second stash (SecureClientWrapper)
                val wrapper = SecureClientWrapper(client)
                wrapper.execute(
                    Mockito.mock(ActionType::class.java) as ActionType<ActionResponse>,
                    IndexRequest("test-index"),
                    Mockito.mock(ActionListener::class.java) as ActionListener<ActionResponse>
                )
            }
        }

        assertTrue("Timed out waiting for execute call", latch.await(3, TimeUnit.SECONDS))
        // Tenant ID from coroutine context survives both stashes
        assertEquals(expectedTenantId, capturedTenantId.get())
        // Security header is properly stashed away
        assertNull(capturedSecurityHeader.get())
    }

    fun `test null tenant id does not inject header in notification path`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        val client = createMockNodeClient(threadContext)

        val capturedTenantId = AtomicReference<String?>("sentinel")
        val latch = CountDownLatch(1)

        whenever(client.execute(any<ActionType<ActionResponse>>(), any(), any<ActionListener<ActionResponse>>())).thenAnswer {
            capturedTenantId.set(threadContext.getHeader(TENANT_ID_HEADER))
            latch.countDown()
            null
        }

        // Single-tenant deployment: no TenantContext or null tenant
        scope.launch(TenantContext(null)) {
            val tenantId = currentTenantId()

            threadContext.stashContext().use {
                tenantId?.let { threadContext.putHeader(TENANT_ID_HEADER, it) }

                val wrapper = SecureClientWrapper(client)
                wrapper.execute(
                    Mockito.mock(ActionType::class.java) as ActionType<ActionResponse>,
                    IndexRequest("test-index"),
                    Mockito.mock(ActionListener::class.java) as ActionListener<ActionResponse>
                )
            }
        }

        assertTrue("Timed out waiting for execute call", latch.await(3, TimeUnit.SECONDS))
        assertNull(capturedTenantId.get())
    }

    private fun createMockNodeClient(threadContext: ThreadContext): Client {
        val client = Mockito.mock(Client::class.java)
        val threadPool = Mockito.mock(ThreadPool::class.java)
        whenever(client.threadPool()).thenReturn(threadPool)
        whenever(threadPool.threadContext).thenReturn(threadContext)
        return client
    }
}
