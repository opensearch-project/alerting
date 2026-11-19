/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.opensearchapi

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Before
import org.junit.Test
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.ConfigConstants
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

    private fun injectedRoles(): String? = threadContext.getTransient(ConfigConstants.OPENSEARCH_SECURITY_INJECTED_ROLES)
}
