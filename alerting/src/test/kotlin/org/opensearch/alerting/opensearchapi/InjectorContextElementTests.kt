/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.opensearchapi

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.core.action.ActionListener
import org.opensearch.test.OpenSearchTestCase
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

class InjectorContextElementTests : OpenSearchTestCase() {

    /**
     * The block is answered from a second thread. Each thread that runs it gets the injected roles, and each gets its
     * own context back: the starting thread when the block suspends, the answering thread when the block returns.
     */
    fun `test withClosableContext injects on each thread and gives each its own context back`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        threadContext.putTransient(OWN, "own")
        val user = User("monitor-owner", listOf("backend"), listOf("monitor_role"), listOf())

        val pending = AtomicReference<ActionListener<String>>()
        val rolesBefore = AtomicReference<Any?>()
        val rolesAfterResume = AtomicReference<Any?>()
        val afterBlock = AtomicReference<List<Any?>>()
        val seenByStarter = AtomicReference<List<Any?>>()
        val seenByResponder = AtomicReference<List<Any?>>()
        fun state(): List<Any?> = listOf(threadContext.getTransient<Any>(ROLES), threadContext.getTransient<Any>(OWN))
        runBlocking {
            launch(Dispatchers.Unconfined) {
                withClosableContext(InjectorContextElement("monitor-1", Settings.EMPTY, threadContext, listOf("monitor_role"), user)) {
                    rolesBefore.set(threadContext.getTransient<Any>(ROLES))
                    suspendCoroutine<String> { cont ->
                        pending.set(
                            object : ActionListener<String> {
                                override fun onResponse(response: String) = cont.resume(response)
                                override fun onFailure(e: Exception) = cont.resumeWithException(e)
                            }
                        )
                    }
                    rolesAfterResume.set(threadContext.getTransient<Any>(ROLES))
                }
                afterBlock.set(state())
            }
            seenByStarter.set(state())

            val responder = Thread {
                pending.get().onResponse("done")
                seenByResponder.set(state())
            }
            responder.start()
            responder.join()
        }

        assertNotNull("the roles must be injected", rolesBefore.get())
        assertEquals("the roles must be injected again on the answering thread", rolesBefore.get(), rolesAfterResume.get())
        assertEquals("the starting thread must get its own context back", listOf(null, "own"), seenByStarter.get())
        assertEquals("the code after the block ran in the injected or another thread's context", listOf(null, null), afterBlock.get())
        assertEquals("the answering thread took another thread's context", listOf(null, null), seenByResponder.get())
    }

    companion object {
        private const val OWN = "own"
        private const val ROLES = ConfigConstants.OPENSEARCH_SECURITY_INJECTED_ROLES
    }
}
