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
        whenever(lockService.deleteLock(any(), any())).thenAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as org.opensearch.core.action.ActionListener<Boolean>
            listener.onResponse(true)
        }

        // We need to call the public deleteMonitor method which calls deleteMonitorDoc internally
        // But deleteMonitor also calls deleteDocLevelMonitorQueriesAndIndices and deleteMetadata
        // For this test, we'll use reflection to call deleteMonitorDoc directly
        val method = DeleteMonitorService.javaClass.getDeclaredMethod(
            "deleteMonitorDoc",
            String::class.java,
            RefreshPolicy::class.java
        )
        method.isAccessible = true

        val result = kotlinx.coroutines.runBlocking {
            @Suppress("UNCHECKED_CAST")
            method.invoke(DeleteMonitorService, "test-monitor-id", RefreshPolicy.IMMEDIATE)
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

        val method = DeleteMonitorService.javaClass.getDeclaredMethod(
            "deleteMetadata",
            org.opensearch.commons.alerting.model.Monitor::class.java
        )
        method.isAccessible = true

        kotlinx.coroutines.runBlocking {
            method.invoke(DeleteMonitorService, monitor)
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

        val method = DeleteMonitorService.javaClass.getDeclaredMethod(
            "deleteMetadata",
            org.opensearch.commons.alerting.model.Monitor::class.java
        )
        method.isAccessible = true

        // Should not throw - errors are logged but swallowed
        kotlinx.coroutines.runBlocking {
            method.invoke(DeleteMonitorService, monitor)
        }
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
