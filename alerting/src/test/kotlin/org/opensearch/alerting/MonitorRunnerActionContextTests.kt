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
import org.opensearch.alerting.AlertingPlugin.Companion.TENANT_ID_HEADER
import org.opensearch.alerting.script.QueryLevelTriggerExecutionContext
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.MustacheTemplateService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.InjectSecurity
import org.opensearch.commons.alerting.model.ActionRunResult
import org.opensearch.commons.notifications.action.GetNotificationConfigResponse
import org.opensearch.commons.notifications.model.Chime
import org.opensearch.commons.notifications.model.ConfigType
import org.opensearch.commons.notifications.model.NotificationConfig
import org.opensearch.commons.notifications.model.NotificationConfigInfo
import org.opensearch.commons.notifications.model.NotificationConfigSearchResult
import org.opensearch.commons.utils.TenantContext
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.script.Script
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.node.NodeClient
import java.time.Instant
import java.util.Collections
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.mockito.Mockito.`when` as whenever

class MonitorRunnerActionContextTests : OpenSearchTestCase() {

    /**
     * The channel lookup and the send are each answered from a new thread, as when the action's coroutine resumes
     * elsewhere. Both calls see the action's context. The code after runAction, and every thread afterwards, hold
     * none of it.
     *
     * The runner thread already holds user info, so that user info is what the action sends with: the monitor's own
     * user info is injected only when none is present, the same as before the action used withStashedContext.
     */
    fun `test runAction sends with the action's context and leaves it on no thread`() {
        val threadContext = ThreadContext(Settings.EMPTY)
        threadContext.putTransient(USER_INFO, "runner|role")
        threadContext.putTransient(OWN, "own")

        val client = mock(NodeClient::class.java)
        val threadPool = mock(ThreadPool::class.java)
        whenever(client.threadPool()).thenReturn(threadPool)
        whenever(threadPool.threadContext).thenReturn(threadContext)

        val dispatches = Collections.synchronizedList(mutableListOf<List<Any?>>())
        val pending = LinkedBlockingQueue<ActionListener<ActionResponse>>()
        doAnswer {
            dispatches.add(
                listOf(
                    threadContext.getTransient<Any>(USER_INFO),
                    threadContext.getTransient<Any>(ROLES),
                    threadContext.getHeader(TENANT_ID_HEADER),
                    threadContext.getTransient<Any>(OWN)
                )
            )
            pending.add(it.getArgument(2))
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
        val expectedRoles = ThreadContext(Settings.EMPTY).let {
            InjectSecurity(monitor.id, Settings.EMPTY, it).injectRoles(monitor.user!!.roles)
            it.getTransient<Any>(ROLES)
        }
        val ctx = QueryLevelTriggerExecutionContext(
            monitor, randomQueryLevelTrigger(), listOf(), listOf(), Instant.now(), Instant.now(), null, null,
            ClusterSettings(Settings.EMPTY, setOf(AlertingSettings.NOTIFICATION_CONTEXT_RESULTS_ALLOWED_ROLES))
        )
        val monitorCtx = MonitorRunnerExecutionContext(client = client, settings = Settings.EMPTY, threadPool = threadPool)
        val action = randomAction(destinationId = CHANNEL_ID)

        val result = AtomicReference<ActionRunResult>()
        val afterRunAction = AtomicReference<List<Any?>>()
        val starter = AtomicReference<List<Any?>>()
        val lookupResponder = AtomicReference<List<Any?>>()
        val sendResponder = AtomicReference<List<Any?>>()
        fun state(): List<Any?> = listOf(
            threadContext.getTransient<Any>(USER_INFO),
            threadContext.getTransient<Any>(ROLES),
            threadContext.getHeader(TENANT_ID_HEADER),
            threadContext.getTransient<Any>(OWN)
        )
        try {
            runBlocking {
                // Unconfined runs the coroutine here until it suspends, and resumes it on the thread that answers
                launch(Dispatchers.Unconfined + TenantContext(TENANT)) {
                    result.set(QueryLevelMonitorRunner.runAction(action, ctx, monitorCtx, monitor, false))
                    afterRunAction.set(state())
                }
                starter.set(state())

                val lookup = Thread {
                    pending.poll(10, TimeUnit.SECONDS)!!.onResponse(GetNotificationConfigResponse(NotificationConfigSearchResult(channel())))
                    lookupResponder.set(state())
                }
                lookup.start()
                lookup.join()

                val send = Thread {
                    pending.poll(10, TimeUnit.SECONDS)!!.onFailure(IllegalStateException("send failed"))
                    sendResponder.set(state())
                }
                send.start()
                send.join()
            }
        } finally {
            MonitorRunnerService.monitorCtx.mustacheTemplateService = originalTemplates
            MonitorRunnerService.monitorCtx.threadPool = originalThreadPool
        }

        assertEquals(2, dispatches.size)
        assertEquals("the lookup must see the action's context", listOf("runner|role", expectedRoles, TENANT, null), dispatches[0])
        // The send goes through SecureClientWrapper, which stashes again and keeps only the tenant header
        assertEquals("the send must see the action's tenant header", TENANT, dispatches[1][2])
        assertEquals("the send must not see the runner's other transients", null, dispatches[1][3])
        assertEquals("send failed", result.get().error?.message)
        assertEquals("the code after runAction ran with the action's context", listOf(null, null, null, null), afterRunAction.get())
        assertEquals("the starting thread must get its own context back", listOf("runner|role", null, null, "own"), starter.get())
        assertEquals("the lookup's answering thread kept a context", listOf(null, null, null, null), lookupResponder.get())
        assertEquals("the send's answering thread kept a context", listOf(null, null, null, null), sendResponder.get())
    }

    private fun channel() = NotificationConfigInfo(
        CHANNEL_ID,
        Instant.now(),
        Instant.now(),
        NotificationConfig("channel", "", ConfigType.CHIME, Chime("https://hooks.chime.aws/incomingwebhooks/sample"), true)
    )

    companion object {
        private const val OWN = "own"
        private const val TENANT = "tenant-a"
        private const val CHANNEL_ID = "channel-1"
        private const val USER_INFO = ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
        private const val ROLES = ConfigConstants.OPENSEARCH_SECURITY_INJECTED_ROLES
    }
}
