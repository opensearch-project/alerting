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

    companion object {
        private const val CALLER = "caller"
    }
}
