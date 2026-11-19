/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.opensearchapi

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Before
import org.junit.Test
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class InjectorContextElementTests {

    private val marker = "test-marker"
    private lateinit var threadContext: ThreadContext
    private lateinit var worker: ExecutorService

    @Before
    fun setUp() {
        threadContext = ThreadContext(Settings.EMPTY)
        worker = Executors.newSingleThreadExecutor()
    }

    @After
    fun tearDown() {
        worker.shutdownNow()
        worker.awaitTermination(10, TimeUnit.SECONDS)
    }

    /** A guard on the injection itself; it passes with the previous element too. */
    @Test
    fun `test the roles stay injected after the coroutine resumes on a worker`() {
        val element = InjectorContextElement("monitor", Settings.EMPTY, threadContext, listOf("role_a"))

        val seen = runBlocking(worker.asCoroutineDispatcher()) {
            withClosableContext(element) {
                val beforeSuspension = injectedRoles()
                delay(10)
                listOf(beforeSuspension, injectedRoles())
            }
        }

        assertEquals(listOf("plugin|role_a", "plugin|role_a"), seen)
    }

    @Test
    fun `test the worker keeps its own context once the coroutine leaves it`() {
        worker.submit { threadContext.putHeader(marker, "worker") }.get(10, TimeUnit.SECONDS)
        threadContext.putHeader(marker, "creator")
        val element = InjectorContextElement("monitor", Settings.EMPTY, threadContext, listOf("role_a"))

        runBlocking(worker.asCoroutineDispatcher()) {
            withClosableContext(element) {
                delay(10)
            }
        }

        val workerState = worker.submit<List<String?>> { listOf(threadContext.getHeader(marker), injectedRoles()) }
            .get(10, TimeUnit.SECONDS)
        assertEquals("the worker's own header, and no injected roles", listOf("worker", null), workerState)
        assertEquals("the creating thread is not changed", "creator", threadContext.getHeader(marker))
        assertNull(injectedRoles())
    }

    @Test
    fun `test the caller's context is back after the block resumes on another thread`() {
        val other = Executors.newSingleThreadExecutor()
        try {
            val user = User("owner", listOf("backend_a"), listOf("role_a"), listOf())
            val element = InjectorContextElement("monitor", Settings.EMPTY, threadContext, listOf("role_a"), user)

            val seen = runBlocking(worker.asCoroutineDispatcher()) {
                val entryThread = Thread.currentThread()
                val inBlock = withClosableContext(element) {
                    withContext(other.asCoroutineDispatcher()) { delay(10) }
                    delay(10)
                    listOf(injectedRoles(), Thread.currentThread() !== entryThread)
                }
                inBlock + listOf(injectedRoles(), injectedUserInfo())
            }

            // The block ends on the worker it entered on, after a hop to the other dispatcher.
            assertEquals(listOf("plugin|role_a", false, null, null), seen)
        } finally {
            other.shutdownNow()
            other.awaitTermination(10, TimeUnit.SECONDS)
        }
    }

    @Test
    fun `test the caller's context is back when the block ends on a different thread from entry`() {
        val element = InjectorContextElement("monitor", Settings.EMPTY, threadContext, listOf("role_a"))
        val entryThread = Thread.currentThread()

        val seen = runBlocking(Dispatchers.Unconfined) {
            withClosableContext(element) {
                // Unconfined resumes on the thread that completes the delay, which is the worker.
                withContext(worker.asCoroutineDispatcher()) { delay(10) }
                assertEquals("plugin|role_a", injectedRoles())
            }
            listOf(Thread.currentThread() !== entryThread, injectedRoles())
        }

        assertEquals("the caller continues on another thread, without the injected roles", listOf(true, null), seen)
        assertNull("the entry thread has no injected roles", injectedRoles())
    }

    private fun injectedUserInfo(): String? =
        threadContext.getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)

    private fun injectedRoles(): String? = threadContext.getTransient(ConfigConstants.OPENSEARCH_SECURITY_INJECTED_ROLES)
}
