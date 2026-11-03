/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.action.ActionListener
import org.opensearch.test.OpenSearchTestCase
import java.util.concurrent.atomic.AtomicReference

class ThreadContextExtensionsTests : OpenSearchTestCase() {

    /**
     * The call starts on the test thread and is answered from a second thread, as a coroutine on Dispatchers.IO can
     * resume elsewhere. Neither thread may end up with the caller's context.
     */
    fun `test suspendAsCaller dispatches as the caller and leaves the caller on no thread`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        val caller = threadContext.stashContext().use {
            threadContext.putPersistent(CALLER, "alice")
            threadContext.newStoredContext(false)
        }

        val seenByDispatch = AtomicReference<Any?>()
        val pending = AtomicReference<ActionListener<String>>()
        val result = AtomicReference<String>()
        val seenByStarter = AtomicReference<Any?>("unset")
        val seenByResponder = AtomicReference<Any?>("unset")
        runBlocking {
            // Unconfined runs the coroutine here until it suspends, and resumes it on the thread that answers
            launch(Dispatchers.Unconfined) {
                result.set(
                    threadContext.suspendAsCaller(caller) { listener ->
                        seenByDispatch.set(threadContext.getPersistent(CALLER))
                        pending.set(listener)
                    }
                )
            }
            seenByStarter.set(threadContext.getPersistent(CALLER))

            val responder = Thread {
                pending.get().onResponse("done")
                seenByResponder.set(threadContext.getPersistent(CALLER))
            }
            responder.start()
            responder.join()
        }

        assertEquals("alice", seenByDispatch.get())
        assertEquals("done", result.get())
        assertNull("the starting thread kept the caller's context", seenByStarter.get())
        assertNull("the answering thread took the caller's context", seenByResponder.get())
    }

    fun `test suspendAsCaller passes a failure through`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        val failure = IllegalStateException("lookup failed")

        val thrown = expectThrows(IllegalStateException::class.java) {
            runBlocking { threadContext.suspendAsCaller<String>(null) { listener -> listener.onFailure(failure) } }
        }

        assertSame(failure, thrown)
    }

    fun `test suspendAsCaller without a caller dispatches stashed and keeps this thread's context`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        threadContext.putTransient(TRANSIENT, "own")
        val seenByDispatch = AtomicReference<Any?>("unset")

        val result = runBlocking {
            threadContext.suspendAsCaller<String>(null) { listener ->
                seenByDispatch.set(threadContext.getTransient<Any>(TRANSIENT))
                listener.onResponse("done")
            }
        }

        assertEquals("done", result)
        assertNull("transient headers must be stashed for the dispatch", seenByDispatch.get())
        assertEquals("own", threadContext.getTransient<Any>(TRANSIENT))
    }

    /**
     * Headers set in prepare reach the dispatch, and neither the starting nor the answering thread keeps them.
     */
    fun `test suspendStashed dispatches with the prepared headers and leaves them on no thread`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        threadContext.putTransient(TRANSIENT, "own")

        val seenByDispatch = AtomicReference<Any?>()
        val pending = AtomicReference<ActionListener<String>>()
        val result = AtomicReference<String>()
        val seenByStarter = AtomicReference<Any?>("unset")
        val seenByResponder = AtomicReference<Any?>("unset")
        runBlocking {
            launch(Dispatchers.Unconfined) {
                result.set(
                    threadContext.suspendStashed({ threadContext.putTransient(PREPARED, "prepared") }) { listener ->
                        seenByDispatch.set(threadContext.getTransient<Any>(PREPARED))
                        pending.set(listener)
                    }
                )
            }
            seenByStarter.set(threadContext.getTransient<Any>(TRANSIENT))

            val responder = Thread {
                pending.get().onResponse("done")
                seenByResponder.set(threadContext.getTransient<Any>(PREPARED))
            }
            responder.start()
            responder.join()
        }

        assertEquals("prepared", seenByDispatch.get())
        assertEquals("done", result.get())
        assertEquals("the starting thread must get its own context back", "own", seenByStarter.get())
        assertNull("the starting thread must not keep the prepared headers", threadContext.getTransient<Any>(PREPARED))
        assertNull("the answering thread took the prepared headers", seenByResponder.get())
    }

    fun `test suspendStashed closes the stash when prepare fails`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        threadContext.putTransient(TRANSIENT, "own")
        val failure = IllegalStateException("bad target")

        val thrown = expectThrows(IllegalStateException::class.java) {
            runBlocking { threadContext.suspendStashed<String>({ throw failure }) { fail("must not dispatch") } }
        }

        assertSame(failure, thrown)
        assertEquals("own", threadContext.getTransient<Any>(TRANSIENT))
    }

    companion object {
        private const val CALLER = "caller"
        private const val TRANSIENT = "transient"
        private const val PREPARED = "prepared"
    }
}
