/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.mockito.Mockito
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.action.ActionListener
import org.opensearch.remote.metadata.client.PutDataObjectRequest
import org.opensearch.remote.metadata.client.PutDataObjectResponse
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.mockito.Mockito.`when` as whenever

class SdkClientExtensionsTests : OpenSearchTestCase() {
    fun `test sdk dispatch restores context before awaiting completion`() = verifySdkContext(false)

    fun `test sdk dispatch restores context on asynchronous failure`() = verifySdkContext(true)

    private fun verifySdkContext(fail: Boolean) = runBlocking {
        ThreadContext(Settings.EMPTY).let { context ->
            val caller = captureCaller(context)
            context.putTransient("dispatch", "original")
            val sdk = Mockito.mock(SdkClient::class.java)
            val request = PutDataObjectRequest.builder().index("test").build()
            val future = CompletableFuture<PutDataObjectResponse>()
            whenever(sdk.putDataObjectAsync(request)).thenAnswer {
                assertInternalDispatch(context)
                future
            }
            val result = async(start = CoroutineStart.UNDISPATCHED) {
                runCatching { sdk.putDataObjectStashed(request, context, caller) }
            }
            val failure = IllegalArgumentException("test failure")
            val response = Mockito.mock(PutDataObjectResponse::class.java)
            try {
                assertEquals("original", context.getTransient<String>("dispatch"))
                assertNull(context.getPersistent("owner"))
            } finally {
                Executors.newSingleThreadExecutor().use { executor ->
                    executor.submit {
                        context.putTransient("completion", "original")
                        if (fail) future.completeExceptionally(failure) else future.complete(response)
                        assertEquals("original", context.getTransient<String>("completion"))
                        assertNull(context.getPersistent("owner"))
                        assertNull(context.getTransient<String>("dispatch"))
                    }.get(10, TimeUnit.SECONDS)
                }
            }
            if (fail) {
                val actual = result.await().exceptionOrNull()
                assertTrue(actual is IllegalArgumentException)
                assertEquals(failure.message, actual!!.message)
            } else {
                assertSame(response, result.await().getOrThrow())
            }
        }
    }

    fun `test sdk synchronous failure restores dispatch context`() {
        ThreadContext(Settings.EMPTY).let { context ->
            val caller = captureCaller(context)
            context.putTransient("dispatch", "original")
            val sdk = Mockito.mock(SdkClient::class.java)
            val request = PutDataObjectRequest.builder().index("test").build()
            whenever(sdk.putDataObjectAsync(request)).thenThrow(IllegalArgumentException("test failure"))
            expectThrows(IllegalArgumentException::class.java) {
                runBlocking { sdk.putDataObjectStashed(request, context, caller) }
            }
            assertEquals("original", context.getTransient<String>("dispatch"))
            assertNull(context.getPersistent("owner"))
        }
    }

    fun `test index dispatch restores context before awaiting completion`() = runBlocking {
        ThreadContext(Settings.EMPTY).let { context ->
            val caller = captureCaller(context)
            context.putTransient("dispatch", "original")
            val client = Mockito.mock(Client::class.java)
            val pool = Mockito.mock(ThreadPool::class.java)
            whenever(client.threadPool()).thenReturn(pool)
            whenever(pool.threadContext).thenReturn(context)
            val request = IndexRequest("test")
            lateinit var listener: ActionListener<IndexResponse>
            Mockito.doAnswer {
                assertInternalDispatch(context)
                listener = it.getArgument(1)
                null
            }.`when`(client).index(Mockito.eq(request), Mockito.any())
            val result = async(start = CoroutineStart.UNDISPATCHED) { client.indexStashed(request, caller) }
            val response = Mockito.mock(IndexResponse::class.java)
            try {
                assertEquals("original", context.getTransient<String>("dispatch"))
                assertNull(context.getPersistent("owner"))
            } finally {
                listener.onResponse(response)
            }
            assertSame(response, result.await())
        }
    }

    private fun captureCaller(context: ThreadContext): ThreadContext.StoredContext = context.newStoredContext(false).use {
        context.putPersistent("owner", "alice")
        context.putTransient("caller", "alice")
        context.newStoredContext(false)
    }

    private fun assertInternalDispatch(context: ThreadContext) {
        assertEquals("alice", context.getPersistent("owner"))
        assertNull(context.getTransient<String>("caller"))
        assertNull(context.getTransient<String>("dispatch"))
    }
}
