/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.action.ActionListener
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.util.concurrent.atomic.AtomicReference

class SdkClientExtensionsTests : OpenSearchTestCase() {

    private val callerHeader = "caller"

    /**
     * The coroutine starts on this thread and resumes on the thread that answers the write, as it can on
     * Dispatchers.IO. Neither thread may keep the caller's context afterwards.
     */
    fun `test indexWithContext applies the caller's context to the write and leaves it on no thread`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        val threadPool = mock(ThreadPool::class.java)
        `when`(threadPool.threadContext).thenReturn(threadContext)
        val client = mock(Client::class.java)
        `when`(client.threadPool()).thenReturn(threadPool)

        val caller = threadContext.stashContext().use {
            threadContext.putPersistent(callerHeader, "alice")
            threadContext.newStoredContext(false)
        }

        val seenByWrite = AtomicReference<Any?>()
        val pending = AtomicReference<ActionListener<IndexResponse>>()
        doAnswer {
            seenByWrite.set(threadContext.getPersistent(callerHeader))
            pending.set(it.getArgument(1))
            null
        }.`when`(client).index(any(IndexRequest::class.java), any())

        val response = mock(IndexResponse::class.java)
        val result = AtomicReference<IndexResponse>()
        val seenByStarter = AtomicReference<Any?>("unset")
        val seenByResponder = AtomicReference<Any?>("unset")
        runBlocking {
            // Unconfined runs the coroutine here until it suspends, and resumes it on the thread that answers
            launch(Dispatchers.Unconfined) { result.set(client.indexWithContext(caller, IndexRequest("index"))) }
            seenByStarter.set(threadContext.getPersistent(callerHeader))

            val responder = Thread {
                pending.get().onResponse(response)
                seenByResponder.set(threadContext.getPersistent(callerHeader))
            }
            responder.start()
            responder.join()
        }

        assertEquals("alice", seenByWrite.get())
        assertSame(response, result.get())
        assertNull("the starting thread kept the caller's context", seenByStarter.get())
        assertNull("the answering thread took the caller's context", seenByResponder.get())
    }
}
