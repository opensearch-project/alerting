/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.mock
import org.opensearch.action.ActionType
import org.opensearch.alerting.script.QueryLevelTriggerExecutionContext
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.MustacheTemplateService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.alerting.model.ActionRunResult
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.script.Script
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.node.NodeClient
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import org.mockito.Mockito.`when` as whenever

class MonitorRunnerActionContextTests : OpenSearchTestCase() {

    /**
     * The notification lookup is answered from a second thread, as it is when the action's coroutine resumes elsewhere.
     * The monitor's roles and the user info reach the lookup, and neither the starting thread nor the answering thread
     * keeps them.
     */
    fun `test runAction sends with the monitor's context and leaves it on no thread`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, "runner|role")
        threadContext.putTransient(OWN, "own")

        val client = mock(NodeClient::class.java)
        val threadPool = mock(ThreadPool::class.java)
        whenever(client.threadPool()).thenReturn(threadPool)
        whenever(threadPool.threadContext).thenReturn(threadContext)

        val seenUserInfo = AtomicReference<Any?>()
        val seenRoles = AtomicReference<Any?>()
        val seenOwn = AtomicReference<Any?>("unset")
        val pending = AtomicReference<ActionListener<ActionResponse>>()
        doAnswer {
            seenUserInfo.set(threadContext.getTransient<Any>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT))
            seenRoles.set(threadContext.getTransient<Any>(ConfigConstants.OPENSEARCH_SECURITY_INJECTED_ROLES))
            seenOwn.set(threadContext.getTransient<Any>(OWN))
            pending.set(it.getArgument(2))
            null
        }.`when`(client).execute(any<ActionType<ActionResponse>>(), any(), any<ActionListener<ActionResponse>>())

        val templates = mock(MustacheTemplateService::class.java)
        // Kotlin parameters are not nullable, so each matcher is paired with a placeholder value
        whenever(templates.renderScript(any(Script::class.java) ?: Script(""), any() ?: emptyMap())).thenReturn("message")
        // runAction renders templates and reads the time through the shared runner context
        val originalTemplates = MonitorRunnerService.monitorCtx.mustacheTemplateService
        val originalThreadPool = MonitorRunnerService.monitorCtx.threadPool
        MonitorRunnerService.monitorCtx.mustacheTemplateService = templates
        MonitorRunnerService.monitorCtx.threadPool = threadPool

        val monitor = randomQueryLevelMonitor(user = randomUser(roles = listOf("monitor_role")))
        val trigger = randomQueryLevelTrigger()
        val ctx = QueryLevelTriggerExecutionContext(
            monitor, trigger, listOf(), listOf(), Instant.now(), Instant.now(), null, null,
            ClusterSettings(Settings.EMPTY, setOf(AlertingSettings.NOTIFICATION_CONTEXT_RESULTS_ALLOWED_ROLES))
        )
        val monitorCtx = MonitorRunnerExecutionContext(client = client, settings = Settings.EMPTY, threadPool = threadPool)

        val result = AtomicReference<ActionRunResult>()
        val starterRoles = AtomicReference<Any?>("unset")
        val starterUserInfo = AtomicReference<Any?>("unset")
        val starterOwn = AtomicReference<Any?>("unset")
        val responderRoles = AtomicReference<Any?>("unset")
        val responderOwn = AtomicReference<Any?>("unset")
        try {
            runBlocking {
                // Unconfined runs the coroutine here until it suspends, and resumes it on the thread that answers
                launch(Dispatchers.Unconfined) {
                    result.set(QueryLevelMonitorRunner.runAction(randomAction(), ctx, monitorCtx, monitor, false))
                }
                starterRoles.set(threadContext.getTransient<Any>(ConfigConstants.OPENSEARCH_SECURITY_INJECTED_ROLES))
                starterUserInfo.set(threadContext.getTransient<Any>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT))
                starterOwn.set(threadContext.getTransient<Any>(OWN))

                val responder = Thread {
                    pending.get().onFailure(IllegalStateException("lookup failed"))
                    responderRoles.set(threadContext.getTransient<Any>(ConfigConstants.OPENSEARCH_SECURITY_INJECTED_ROLES))
                    responderOwn.set(threadContext.getTransient<Any>(OWN))
                }
                responder.start()
                responder.join()
            }
        } finally {
            MonitorRunnerService.monitorCtx.mustacheTemplateService = originalTemplates
            MonitorRunnerService.monitorCtx.threadPool = originalThreadPool
        }

        assertEquals("runner|role", seenUserInfo.get())
        assertNotNull("the monitor's roles must reach the lookup", seenRoles.get())
        assertNull("the lookup must not see the runner's other transients", seenOwn.get())
        assertEquals("lookup failed", result.get().error?.message)
        assertNull("the starting thread kept the monitor's roles", starterRoles.get())
        assertEquals("the starting thread must get its own context back", "runner|role", starterUserInfo.get())
        assertEquals("the starting thread must get its own context back", "own", starterOwn.get())
        assertNull("the answering thread took the monitor's roles", responderRoles.get())
        assertNull("the answering thread took the starting thread's context", responderOwn.get())
    }

    companion object {
        private const val OWN = "own"
    }
}
