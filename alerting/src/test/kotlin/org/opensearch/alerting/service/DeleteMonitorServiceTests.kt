/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import com.carrotsearch.randomizedtesting.ThreadFilter
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters
import org.junit.Before
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.alerting.anyNonNull
import org.opensearch.alerting.core.lock.LockService
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.remote.metadata.client.DeleteDataObjectRequest
import org.opensearch.remote.metadata.client.DeleteDataObjectResponse
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.remote.metadata.client.SearchDataObjectRequest
import org.opensearch.remote.metadata.client.SearchDataObjectResponse
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import kotlin.coroutines.Continuation
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import org.mockito.Mockito.`when` as whenever

@ThreadLeakFilters(filters = [DeleteMonitorServiceTests.CoroutineThreadFilter::class])
class DeleteMonitorServiceTests : OpenSearchTestCase() {

    class CoroutineThreadFilter : ThreadFilter {
        override fun reject(t: Thread): Boolean = t.name.startsWith("DefaultDispatcher-worker")
    }

    private lateinit var client: Client
    private lateinit var sdkClient: SdkClient
    private lateinit var lockService: LockService
    private lateinit var threadPool: ThreadPool
    private lateinit var threadContext: ThreadContext

    @Before
    fun setup() {
        client = Mockito.mock(Client::class.java)
        sdkClient = Mockito.mock(SdkClient::class.java)
        lockService = Mockito.mock(LockService::class.java)
        threadPool = Mockito.mock(ThreadPool::class.java)
        threadContext = ThreadContext(Settings.EMPTY)

        whenever(client.threadPool()).thenReturn(threadPool)
        whenever(threadPool.threadContext).thenReturn(threadContext)

        DeleteMonitorService.initialize(client, lockService, sdkClient)
    }

    /**
     * Looks up one of [DeleteMonitorService]'s private `suspend` methods, appending the trailing
     * [Continuation] parameter the Kotlin compiler adds to every suspend function's JVM signature.
     * [paramTypes] therefore lists only the parameters declared in Kotlin.
     */
    private fun privateSuspendMethod(name: String, vararg paramTypes: Class<*>): Method =
        DeleteMonitorService.javaClass
            .getDeclaredMethod(name, *paramTypes, Continuation::class.java)
            .apply { isAccessible = true }

    /**
     * Calls a method returned by [privateSuspendMethod] on the [DeleteMonitorService] singleton.
     *
     * A suspend function's JVM method has an erased `Object` return type holding either the result or
     * the COROUTINE_SUSPENDED sentinel; in the latter case the result reaches the caller through the
     * continuation instead of the return value. [suspendCoroutineUninterceptedOrReturn] supplies this
     * coroutine's own continuation and interprets the sentinel, so handing it down and returning
     * whatever `invoke` produced is correct whether or not the callee actually suspends. This mirrors
     * `kotlin.reflect.full.callSuspend`, which is unavailable because kotlin-reflect is not a
     * dependency. Reflection throws synchronously wrapped, so unwrap to let tests see the real cause.
     */
    private suspend fun Method.callSuspend(vararg args: Any?): Any? =
        suspendCoroutineUninterceptedOrReturn { continuation ->
            try {
                invoke(DeleteMonitorService, *args, continuation)
            } catch (e: InvocationTargetException) {
                throw e.cause ?: e
            }
        }

    fun `test monitorIsWorkflowDelegate uses SDK search`() {
        val searchResponse = Mockito.mock(SearchResponse::class.java)
        val hits = Mockito.mock(org.opensearch.search.SearchHits::class.java)
        whenever(hits.totalHits).thenReturn(org.apache.lucene.search.TotalHits(0L, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO))
        whenever(searchResponse.hits).thenReturn(hits)

        val sdkResponse = Mockito.mock(SearchDataObjectResponse::class.java)
        whenever(sdkResponse.searchResponse()).thenReturn(searchResponse)
        whenever(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest::class.java)))
            .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(sdkResponse))

        val result = kotlinx.coroutines.runBlocking {
            DeleteMonitorService.monitorIsWorkflowDelegate("test-monitor-id")
        }

        assertFalse(result)
        verify(sdkClient).searchDataObjectAsync(any(SearchDataObjectRequest::class.java))
    }

    fun `test monitorIsWorkflowDelegate returns true when workflows found`() {
        val searchResponse = Mockito.mock(SearchResponse::class.java)
        val hits = Mockito.mock(org.opensearch.search.SearchHits::class.java)
        whenever(hits.totalHits).thenReturn(org.apache.lucene.search.TotalHits(1L, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO))
        val hit = Mockito.mock(org.opensearch.search.SearchHit::class.java)
        whenever(hit.id).thenReturn("workflow-1")
        whenever(hits.hits).thenReturn(arrayOf(hit))
        whenever(searchResponse.hits).thenReturn(hits)

        val sdkResponse = Mockito.mock(SearchDataObjectResponse::class.java)
        whenever(sdkResponse.searchResponse()).thenReturn(searchResponse)
        whenever(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest::class.java)))
            .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(sdkResponse))

        val result = kotlinx.coroutines.runBlocking {
            DeleteMonitorService.monitorIsWorkflowDelegate("test-monitor-id")
        }

        assertTrue(result)
    }

    fun `test monitorIsWorkflowDelegate stashes thread context`() {
        // Put a header in the thread context to verify it gets stashed
        threadContext.putHeader("_opendistro_security_user", "test-user")

        val searchResponse = Mockito.mock(SearchResponse::class.java)
        val hits = Mockito.mock(org.opensearch.search.SearchHits::class.java)
        whenever(hits.totalHits).thenReturn(org.apache.lucene.search.TotalHits(0L, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO))
        whenever(searchResponse.hits).thenReturn(hits)

        val sdkResponse = Mockito.mock(SearchDataObjectResponse::class.java)
        whenever(sdkResponse.searchResponse()).thenReturn(searchResponse)
        whenever(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest::class.java)))
            .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(sdkResponse))

        kotlinx.coroutines.runBlocking {
            DeleteMonitorService.monitorIsWorkflowDelegate("test-monitor-id")
        }

        // Verify stashContext was called (threadPool().threadContext accessed)
        verify(client).threadPool()
    }

    fun `test monitorIsWorkflowDelegate search request targets scheduled jobs index`() {
        val searchResponse = Mockito.mock(SearchResponse::class.java)
        val hits = Mockito.mock(org.opensearch.search.SearchHits::class.java)
        whenever(hits.totalHits).thenReturn(org.apache.lucene.search.TotalHits(0L, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO))
        whenever(searchResponse.hits).thenReturn(hits)

        val sdkResponse = Mockito.mock(SearchDataObjectResponse::class.java)
        whenever(sdkResponse.searchResponse()).thenReturn(searchResponse)
        whenever(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest::class.java)))
            .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(sdkResponse))

        kotlinx.coroutines.runBlocking {
            DeleteMonitorService.monitorIsWorkflowDelegate("test-monitor-id")
        }

        val captor = ArgumentCaptor.forClass(SearchDataObjectRequest::class.java)
        verify(sdkClient).searchDataObjectAsync(captor.capture())
        assertTrue(captor.value.indices().contains(".opendistro-alerting-config"))
    }

    fun `test monitorIsWorkflowDelegate wraps exception in AlertingException`() {
        whenever(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest::class.java)))
            .thenReturn(java.util.concurrent.CompletableFuture.failedFuture(RuntimeException("connection failed")))

        val exception = expectThrows(org.opensearch.commons.alerting.util.AlertingException::class.java) {
            kotlinx.coroutines.runBlocking {
                DeleteMonitorService.monitorIsWorkflowDelegate("test-monitor-id")
            }
        }
        assertNotNull(exception)
    }

    fun `test deleteMonitorDoc uses SDK delete with correct index and id`() {
        val deleteResponse = Mockito.mock(DeleteResponse::class.java)
        whenever(deleteResponse.version).thenReturn(1L)

        val sdkDeleteResponse = Mockito.mock(DeleteDataObjectResponse::class.java)
        whenever(sdkDeleteResponse.id()).thenReturn("test-monitor-id")
        whenever(sdkDeleteResponse.deleteResponse()).thenReturn(deleteResponse)
        whenever(sdkClient.deleteDataObjectAsync(any(DeleteDataObjectRequest::class.java)))
            .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(sdkDeleteResponse))

        // Mock lock service for deleteLock
        whenever(lockService.deleteLock(anyNonNull(), anyNonNull())).thenAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as org.opensearch.core.action.ActionListener<Boolean>
            listener.onResponse(true)
        }

        // Driven directly rather than through deleteMonitor, which would also run query index
        // cleanup, metadata deletion and lock release.
        val method = privateSuspendMethod("deleteMonitorDoc", String::class.java, RefreshPolicy::class.java)

        val result = kotlinx.coroutines.runBlocking {
            method.callSuspend("test-monitor-id", RefreshPolicy.IMMEDIATE)
                as org.opensearch.commons.alerting.action.DeleteMonitorResponse
        }

        assertEquals("test-monitor-id", result.id)

        val captor = ArgumentCaptor.forClass(DeleteDataObjectRequest::class.java)
        verify(sdkClient).deleteDataObjectAsync(captor.capture())
        assertEquals(".opendistro-alerting-config", captor.value.index())
        assertEquals("test-monitor-id", captor.value.id())
    }

    fun `test deleteMetadata uses SDK delete with metadata id suffix`() {
        val deleteResponse = Mockito.mock(DeleteResponse::class.java)
        whenever(deleteResponse.version).thenReturn(1L)

        val sdkDeleteResponse = Mockito.mock(DeleteDataObjectResponse::class.java)
        whenever(sdkDeleteResponse.id()).thenReturn("test-monitor-id-metadata")
        whenever(sdkDeleteResponse.status()).thenReturn(org.opensearch.core.rest.RestStatus.OK)
        whenever(sdkDeleteResponse.deleteResponse()).thenReturn(deleteResponse)
        whenever(sdkClient.deleteDataObjectAsync(any(DeleteDataObjectRequest::class.java)))
            .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(sdkDeleteResponse))

        val monitor = Mockito.mock(org.opensearch.commons.alerting.model.Monitor::class.java)
        whenever(monitor.id).thenReturn("test-monitor-id")

        val method = privateSuspendMethod("deleteMetadata", org.opensearch.commons.alerting.model.Monitor::class.java)

        kotlinx.coroutines.runBlocking {
            method.callSuspend(monitor)
        }

        val captor = ArgumentCaptor.forClass(DeleteDataObjectRequest::class.java)
        verify(sdkClient).deleteDataObjectAsync(captor.capture())
        assertEquals("test-monitor-id-metadata", captor.value.id())
        assertEquals(".opendistro-alerting-config", captor.value.index())
    }

    fun `test deleteMetadata does not throw on failure`() {
        whenever(sdkClient.deleteDataObjectAsync(any(DeleteDataObjectRequest::class.java)))
            .thenReturn(java.util.concurrent.CompletableFuture.failedFuture(RuntimeException("delete failed")))

        val monitor = Mockito.mock(org.opensearch.commons.alerting.model.Monitor::class.java)
        whenever(monitor.id).thenReturn("test-monitor-id")

        val method = privateSuspendMethod("deleteMetadata", org.opensearch.commons.alerting.model.Monitor::class.java)

        // Should not throw - errors are logged but swallowed
        kotlinx.coroutines.runBlocking {
            method.callSuspend(monitor)
        }

        // The delete has to have been attempted, otherwise completing without throwing proves nothing.
        verify(sdkClient).deleteDataObjectAsync(any(DeleteDataObjectRequest::class.java))
    }

    fun `test initialize sets sdkClient`() {
        val newSdkClient = Mockito.mock(SdkClient::class.java)
        val newClient = Mockito.mock(Client::class.java)
        val newLockService = Mockito.mock(LockService::class.java)

        DeleteMonitorService.initialize(newClient, newLockService, newSdkClient)

        // Verify by calling a method that uses sdkClient
        val searchResponse = Mockito.mock(SearchResponse::class.java)
        val hits = Mockito.mock(org.opensearch.search.SearchHits::class.java)
        whenever(hits.totalHits).thenReturn(org.apache.lucene.search.TotalHits(0L, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO))
        whenever(searchResponse.hits).thenReturn(hits)

        val sdkResponse = Mockito.mock(SearchDataObjectResponse::class.java)
        whenever(sdkResponse.searchResponse()).thenReturn(searchResponse)
        whenever(newSdkClient.searchDataObjectAsync(any(SearchDataObjectRequest::class.java)))
            .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(sdkResponse))

        val newThreadPool = Mockito.mock(ThreadPool::class.java)
        whenever(newClient.threadPool()).thenReturn(newThreadPool)
        whenever(newThreadPool.threadContext).thenReturn(ThreadContext(Settings.EMPTY))

        kotlinx.coroutines.runBlocking {
            DeleteMonitorService.monitorIsWorkflowDelegate("test-id")
        }

        verify(newSdkClient).searchDataObjectAsync(any(SearchDataObjectRequest::class.java))
        verify(sdkClient, never()).searchDataObjectAsync(any(SearchDataObjectRequest::class.java))
    }
}
