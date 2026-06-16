/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.mockito.Mockito
import org.mockito.Mockito.doNothing
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.model.InputRunResults
import org.opensearch.script.Script
import org.opensearch.script.ScriptContext
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.time.Instant
import java.time.temporal.ChronoUnit

class QueryLevelMonitorRunnerTests : OpenSearchTestCase() {

    private lateinit var monitorCtx: MonitorRunnerExecutionContext
    private lateinit var scriptService: ScriptService
    private lateinit var triggerService: TriggerService
    private lateinit var alertService: AlertService
    private lateinit var alertIndices: AlertIndices
    private lateinit var clusterService: ClusterService
    private lateinit var threadPool: ThreadPool
    private lateinit var transportService: TransportService
    private lateinit var inputService: InputService

    @Before
    fun setup() {
        scriptService = mock(ScriptService::class.java)
        triggerService = mock(TriggerService::class.java)
        alertService = mock(AlertService::class.java)
        alertIndices = mock(AlertIndices::class.java)
        clusterService = mock(ClusterService::class.java)
        threadPool = mock(ThreadPool::class.java)
        transportService = mock(TransportService::class.java)
        inputService = mock(InputService::class.java)

        val settings = Settings.builder().build()
        val settingSet = hashSetOf<Setting<*>>()
        settingSet.add(AlertingSettings.MAX_COMMENTS_PER_NOTIFICATION)
        val clusterSettings = ClusterSettings(settings, settingSet)
        `when`(clusterService.clusterSettings).thenReturn(clusterSettings)

        val threadContext = org.opensearch.common.util.concurrent.ThreadContext(Settings.EMPTY)
        `when`(threadPool.threadContext).thenReturn(threadContext)

        monitorCtx = MonitorRunnerExecutionContext(
            scriptService = scriptService,
            triggerService = triggerService,
            alertService = alertService,
            alertIndices = alertIndices,
            clusterService = clusterService,
            threadPool = threadPool,
            inputService = inputService,
            settings = settings
        )
    }

    fun `test multiTenantTriggerEvalEnabled with search failure sets triggered true`() = runBlocking {
        monitorCtx.multiTenantTriggerEvalEnabled = true

        val trigger = randomQueryLevelTrigger()
        val monitor = randomQueryLevelMonitor(triggers = listOf(trigger))

        doNothing().`when`(alertIndices).createOrUpdateAlertIndex(Mockito.any())
        doNothing().`when`(alertIndices).createOrUpdateInitialAlertHistoryIndex(Mockito.any())
        doReturn(mapOf(trigger to null))
            .`when`(alertService).loadCurrentAlertsForQueryLevelMonitor(Mockito.any(), Mockito.any())
        doReturn(InputRunResults(emptyList(), RuntimeException("403 Forbidden")))
            .`when`(inputService).collectInputResults(
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyBoolean()
            )

        val result = QueryLevelMonitorRunner.runMonitor(
            monitor = monitor,
            monitorCtx = monitorCtx,
            periodStart = Instant.now().minus(1, ChronoUnit.MINUTES),
            periodEnd = Instant.now(),
            dryrun = false,
            workflowRunContext = null,
            executionId = "test-exec-id",
            transportService = transportService
        )

        verify(scriptService, never())
            .compile(Mockito.any(Script::class.java), Mockito.any(ScriptContext::class.java))
        verify(triggerService, never()).runQueryLevelTrigger(Mockito.any(), Mockito.any(), Mockito.any())

        val triggerResult = result.triggerResults[trigger.id]
        assertNotNull(triggerResult)
        assertTrue("Trigger should fire for error alert", triggerResult!!.triggered)
        assertNotNull("Error should be propagated", triggerResult.error)
    }

    fun `test multiTenantTriggerEvalEnabled with search failure sets triggered false on dryrun`() = runBlocking {
        monitorCtx.multiTenantTriggerEvalEnabled = true

        val trigger = randomQueryLevelTrigger()
        val monitor = randomQueryLevelMonitor(triggers = listOf(trigger))

        doNothing().`when`(alertIndices).createOrUpdateAlertIndex(Mockito.any())
        doNothing().`when`(alertIndices).createOrUpdateInitialAlertHistoryIndex(Mockito.any())
        doReturn(mapOf(trigger to null))
            .`when`(alertService).loadCurrentAlertsForQueryLevelMonitor(Mockito.any(), Mockito.any())
        doReturn(InputRunResults(emptyList(), RuntimeException("403 Forbidden")))
            .`when`(inputService).collectInputResults(
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyBoolean()
            )

        val result = QueryLevelMonitorRunner.runMonitor(
            monitor = monitor,
            monitorCtx = monitorCtx,
            periodStart = Instant.now().minus(1, ChronoUnit.MINUTES),
            periodEnd = Instant.now(),
            dryrun = true,
            workflowRunContext = null,
            executionId = "test-exec-id",
            transportService = transportService
        )

        verify(scriptService, never())
            .compile(Mockito.any(Script::class.java), Mockito.any(ScriptContext::class.java))
        verify(triggerService, never()).runQueryLevelTrigger(Mockito.any(), Mockito.any(), Mockito.any())

        val triggerResult = result.triggerResults[trigger.id]
        assertNotNull(triggerResult)
        assertFalse("Trigger should NOT fire on dryrun", triggerResult!!.triggered)
        assertNotNull("Error should be propagated", triggerResult.error)
    }
}
