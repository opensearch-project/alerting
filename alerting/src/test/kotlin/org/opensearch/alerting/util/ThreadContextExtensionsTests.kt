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

    /**
     * A block that suspends more than once runs every step with the prepared headers, on whichever thread resumes it.
     * The code after the block, which runs on the answering thread, and both threads afterwards hold no prepared headers.
     */
    fun `test withStashedContext keeps the prepared headers across suspensions and leaves them on no thread`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        threadContext.putTransient(TRANSIENT, "own")

        val seen = java.util.Collections.synchronizedList(mutableListOf<Any?>())
        val first = AtomicReference<ActionListener<String>>()
        val second = AtomicReference<ActionListener<String>>()
        val afterBlock = AtomicReference<Any?>("unset")
        val seenByStarter = AtomicReference<Any?>("unset")
        val seenByResponder = AtomicReference<Any?>("unset")
        val ownOnResponder = AtomicReference<Any?>("unset")
        runBlocking {
            launch(Dispatchers.Unconfined) {
                threadContext.withStashedContext({ threadContext.putTransient(PREPARED, "prepared") }) {
                    seen.add(threadContext.getTransient<Any>(TRANSIENT))
                    awaitAnswer<String>(first)
                    seen.add(threadContext.getTransient<Any>(PREPARED))
                    awaitAnswer<String>(second)
                    seen.add(threadContext.getTransient<Any>(PREPARED))
                }
                afterBlock.set(threadContext.getTransient<Any>(PREPARED))
            }
            seenByStarter.set(threadContext.getTransient<Any>(PREPARED))

            val responder = Thread {
                first.get().onResponse("one")
                second.get().onResponse("two")
                seenByResponder.set(threadContext.getTransient<Any>(PREPARED))
                ownOnResponder.set(threadContext.getTransient<Any>(TRANSIENT))
            }
            responder.start()
            responder.join()
        }

        assertEquals("the block must not see the caller's transients, then the prepared headers each time", listOf(null, "prepared", "prepared"), seen)
        assertNull("the code after the block ran with the prepared headers", afterBlock.get())
        assertNull("the starting thread kept the prepared headers", seenByStarter.get())
        assertEquals("the starting thread must get its own context back", "own", threadContext.getTransient<Any>(TRANSIENT))
        assertNull("the answering thread took the prepared headers", seenByResponder.get())
        assertNull("the answering thread took the starting thread's context", ownOnResponder.get())
    }

    fun `test withStashedContext passes a failure through and keeps this thread's context`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        threadContext.putTransient(TRANSIENT, "own")
        val failure = IllegalStateException("send failed")

        val thrown = expectThrows(IllegalStateException::class.java) {
            runBlocking { threadContext.withStashedContext({ threadContext.putTransient(PREPARED, "prepared") }) { throw failure } }
        }

        // withContext may rethrow a copy that carries the coroutine's stack, so compare the message
        assertEquals(failure.message, thrown.message)
        assertEquals("own", threadContext.getTransient<Any>(TRANSIENT))
        assertNull(threadContext.getTransient<Any>(PREPARED))
    }

    /**
     * The block fails after it resumed on the answering thread. The failure reaches the caller, which runs on that
     * thread without the prepared headers.
     */
    fun `test withStashedContext passes a failure after a resume and leaves the headers on no thread`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        threadContext.putTransient(TRANSIENT, "own")

        val first = AtomicReference<ActionListener<String>>()
        val caught = AtomicReference<Throwable?>()
        val afterFailure = AtomicReference<Any?>("unset")
        val seenByResponder = AtomicReference<Any?>("unset")
        runBlocking {
            launch(Dispatchers.Unconfined) {
                try {
                    threadContext.withStashedContext({ threadContext.putTransient(PREPARED, "prepared") }) {
                        awaitAnswer<String>(first)
                        throw IllegalStateException("send failed")
                    }
                } catch (e: IllegalStateException) {
                    caught.set(e)
                }
                afterFailure.set(threadContext.getTransient<Any>(PREPARED))
            }

            val responder = Thread {
                first.get().onResponse("one")
                seenByResponder.set(threadContext.getTransient<Any>(PREPARED))
            }
            responder.start()
            responder.join()
        }

        assertEquals("send failed", caught.get()?.message)
        assertNull("the caller handled the failure with the prepared headers", afterFailure.get())
        assertNull("the answering thread took the prepared headers", seenByResponder.get())
        assertEquals("own", threadContext.getTransient<Any>(TRANSIENT))
    }

    /**
     * An inner call runs under its own stash, and the outer block gets its stash back when the inner call returns on
     * the answering thread.
     */
    fun `test nested withStashedContext gives the outer block its stash back`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        threadContext.putTransient(TRANSIENT, "own")

        val first = AtomicReference<ActionListener<String>>()
        val second = AtomicReference<ActionListener<String>>()
        val inInner = AtomicReference<List<Any?>>()
        val afterInner = AtomicReference<List<Any?>>()
        val afterSecond = AtomicReference<List<Any?>>()
        val afterOuter = AtomicReference<List<Any?>>()
        val seenByResponder = AtomicReference<List<Any?>>()
        fun headers(): List<Any?> = listOf(threadContext.getTransient<Any>(OUTER), threadContext.getTransient<Any>(PREPARED))
        runBlocking {
            launch(Dispatchers.Unconfined) {
                threadContext.withStashedContext({ threadContext.putTransient(OUTER, "outer") }) {
                    threadContext.withStashedContext({ threadContext.putTransient(PREPARED, "inner") }) {
                        awaitAnswer<String>(first)
                        inInner.set(headers())
                    }
                    afterInner.set(headers())
                    awaitAnswer<String>(second)
                    afterSecond.set(headers())
                }
                afterOuter.set(headers())
            }

            val responder = Thread {
                first.get().onResponse("one")
                second.get().onResponse("two")
                seenByResponder.set(headers())
            }
            responder.start()
            responder.join()
        }

        assertEquals("the inner block runs under the inner stash only", listOf(null, "inner"), inInner.get())
        assertEquals("the outer block must get its stash back after the inner call", listOf("outer", null), afterInner.get())
        assertEquals(listOf("outer", null), afterSecond.get())
        assertEquals("the code after the outer block ran with a stash", listOf(null, null), afterOuter.get())
        assertEquals("the answering thread kept a stash", listOf(null, null), seenByResponder.get())
        assertEquals(listOf(null, null), headers())
        assertEquals("own", threadContext.getTransient<Any>(TRANSIENT))
    }

    /** Suspends until the listener left in [slot] is answered, as a transport call does. */
    private suspend fun <T> awaitAnswer(slot: AtomicReference<ActionListener<T>>): T =
        kotlin.coroutines.suspendCoroutine { cont ->
            slot.set(
                object : ActionListener<T> {
                    override fun onResponse(response: T) = cont.resumeWith(Result.success(response))
                    override fun onFailure(e: Exception) = cont.resumeWith(Result.failure(e))
                }
            )
        }

    companion object {
        private const val CALLER = "caller"
        private const val TRANSIENT = "transient"
        private const val PREPARED = "prepared"
        private const val OUTER = "outer"
    }
}
