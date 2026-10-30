/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.remote.metadata.client.DeleteDataObjectRequest
import org.opensearch.remote.metadata.client.DeleteDataObjectResponse
import org.opensearch.remote.metadata.client.GetDataObjectRequest
import org.opensearch.remote.metadata.client.GetDataObjectResponse
import org.opensearch.remote.metadata.client.PutDataObjectRequest
import org.opensearch.remote.metadata.client.PutDataObjectResponse
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.test.OpenSearchTestCase
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference

/**
 * Each call starts on the test thread and is completed from a second thread, as a coroutine on Dispatchers.IO can
 * resume elsewhere. Neither thread may end up with a context other than its own.
 */
class DataObjectStashTests : OpenSearchTestCase() {

    private val threadContext = ThreadContext(Settings.EMPTY)
    private val sdkClient = mock(SdkClient::class.java)

    /** What the request saw when it was dispatched, and what each thread held afterwards. */
    private data class Observed(
        val dispatchedCaller: Any?,
        val dispatchedTransient: Any?,
        val starterTransient: Any?,
        val starterCaller: Any?,
        val responderTransient: Any?,
    )

    fun `test putDataObjectStashed dispatches stashed and leaves both threads as they were`() {
        val pending = CompletableFuture<PutDataObjectResponse>()
        val dispatched = recordDispatch()
        `when`(sdkClient.putDataObjectAsync(any(PutDataObjectRequest::class.java))).thenAnswer { dispatched.run(); pending }

        val observed = observe(dispatched, { pending.complete(mock(PutDataObjectResponse::class.java)) }) {
            sdkClient.putDataObjectStashed(PutDataObjectRequest.builder().index("index").build(), threadContext)
        }

        assertNull("transient headers must be stashed for the dispatch", observed.dispatchedTransient)
        assertOwnContextsKept(observed)
    }

    fun `test getDataObjectStashed dispatches stashed and leaves both threads as they were`() {
        val pending = CompletableFuture<GetDataObjectResponse>()
        val dispatched = recordDispatch()
        `when`(sdkClient.getDataObjectAsync(any(GetDataObjectRequest::class.java))).thenAnswer { dispatched.run(); pending }

        val observed = observe(dispatched, { pending.complete(mock(GetDataObjectResponse::class.java)) }) {
            sdkClient.getDataObjectStashed(GetDataObjectRequest.builder().index("index").id("id").build(), threadContext)
        }

        assertNull("transient headers must be stashed for the dispatch", observed.dispatchedTransient)
        assertOwnContextsKept(observed)
    }

    fun `test deleteDataObjectStashed dispatches stashed and leaves both threads as they were`() {
        val pending = CompletableFuture<DeleteDataObjectResponse>()
        val dispatched = recordDispatch()
        `when`(sdkClient.deleteDataObjectAsync(any(DeleteDataObjectRequest::class.java))).thenAnswer { dispatched.run(); pending }

        val observed = observe(dispatched, { pending.complete(mock(DeleteDataObjectResponse::class.java)) }) {
            sdkClient.deleteDataObjectStashed(DeleteDataObjectRequest.builder().index("index").id("id").build(), threadContext)
        }

        assertNull("transient headers must be stashed for the dispatch", observed.dispatchedTransient)
        assertOwnContextsKept(observed)
    }

    fun `test putDataObjectAsCaller dispatches with the caller's persistent headers and leaves both threads as they were`() {
        val caller = threadContext.stashContext().use {
            threadContext.putPersistent(CALLER, "alice")
            threadContext.putTransient(TRANSIENT, "caller transient")
            threadContext.newStoredContext(false)
        }
        val pending = CompletableFuture<PutDataObjectResponse>()
        val dispatched = recordDispatch()
        `when`(sdkClient.putDataObjectAsync(any(PutDataObjectRequest::class.java))).thenAnswer { dispatched.run(); pending }

        val observed = observe(dispatched, { pending.complete(mock(PutDataObjectResponse::class.java)) }) {
            sdkClient.putDataObjectAsCaller(PutDataObjectRequest.builder().index("index").build(), threadContext, caller)
        }

        assertEquals("alice", observed.dispatchedCaller)
        assertNull("the caller's transient headers must be stashed for the dispatch", observed.dispatchedTransient)
        assertOwnContextsKept(observed)
    }

    private class Dispatch(private val threadContext: ThreadContext) : Runnable {
        val caller = AtomicReference<Any?>()
        val transient = AtomicReference<Any?>()
        override fun run() {
            caller.set(threadContext.getPersistent(CALLER))
            transient.set(threadContext.getTransient<Any>(TRANSIENT))
        }
    }

    private fun recordDispatch() = Dispatch(threadContext)

    /**
     * Runs [call] with the test thread holding its own transient header, then completes it from a second thread that
     * holds no context, and records what each thread held afterwards.
     */
    private fun observe(dispatched: Dispatch, complete: () -> Unit, call: suspend () -> Any): Observed {
        threadContext.putTransient(TRANSIENT, "starter's own")
        val result = AtomicReference<Any>()
        val starterTransient = AtomicReference<Any?>("unset")
        val starterCaller = AtomicReference<Any?>("unset")
        val responderTransient = AtomicReference<Any?>("unset")
        runBlocking {
            // Unconfined runs the call here until it suspends, and resumes it on the thread that completes it
            launch(Dispatchers.Unconfined) { result.set(call()) }
            starterTransient.set(threadContext.getTransient<Any>(TRANSIENT))
            starterCaller.set(threadContext.getPersistent(CALLER))

            val responder = Thread {
                complete()
                responderTransient.set(threadContext.getTransient<Any>(TRANSIENT))
            }
            responder.start()
            responder.join()
        }
        assertNotNull("the call must complete", result.get())
        return Observed(dispatched.caller.get(), dispatched.transient.get(), starterTransient.get(), starterCaller.get(), responderTransient.get())
    }

    private fun assertOwnContextsKept(observed: Observed) {
        assertEquals("the starting thread must get its own context back", "starter's own", observed.starterTransient)
        assertNull("the starting thread must not keep the caller's headers", observed.starterCaller)
        assertNull("the completing thread must not take another thread's context", observed.responderTransient)
    }

    companion object {
        private const val CALLER = "caller"
        private const val TRANSIENT = "transient"
    }
}
