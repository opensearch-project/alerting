/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import com.github.mustachejava.MustacheException
import kotlinx.coroutines.runBlocking
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.Mockito.verifyNoMoreInteractions
import org.mockito.Mockito.`when`
import org.opensearch.action.support.ActionFilters
import org.opensearch.alerting.script.ChainedAlertTriggerExecutionContext
import org.opensearch.alerting.script.DocumentLevelTriggerExecutionContext
import org.opensearch.alerting.script.QueryLevelTriggerExecutionContext
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.transport.TransportDocLevelMonitorFanOutAction
import org.opensearch.alerting.util.MustacheTemplateService
import org.opensearch.alerting.workflow.CompositeWorkflowRunner
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.model.ActionRunResult
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.alerting.model.WorkflowRunResult
import org.opensearch.commons.alerting.model.action.Action
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.query.QueryBuilders
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.search.SearchModule
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.nio.file.Files
import java.time.Instant

class MustacheTemplateCallSiteTests : OpenSearchTestCase() {
    private lateinit var originalMonitorCtx: MonitorRunnerExecutionContext
    private lateinit var monitorCtx: MonitorRunnerExecutionContext
    private lateinit var settings: Settings
    private lateinit var clusterSettings: ClusterSettings
    private lateinit var scriptService: ScriptService
    private lateinit var client: Client
    private lateinit var threadPool: ThreadPool

    override fun setUp() {
        super.setUp()
        originalMonitorCtx = MonitorRunnerService.monitorCtx
        settings = Settings.builder().put(AlertingSettings.MULTI_TENANCY_ENABLED.key, true).build()
        clusterSettings = ClusterSettings(settings, alertingPluginSettings())
        scriptService = mock(ScriptService::class.java)
        client = mock(Client::class.java)
        threadPool = mock(ThreadPool::class.java)
        monitorCtx = MonitorRunnerExecutionContext(
            client = client,
            threadPool = threadPool,
            settings = settings,
            mustacheTemplateService = MustacheTemplateService(scriptService, settings)
        )
        MonitorRunnerService.monitorCtx = monitorCtx
    }

    override fun tearDown() {
        try {
            MonitorRunnerService.monitorCtx = originalMonitorCtx
        } finally {
            super.tearDown()
        }
    }

    fun `test monitor actions reject partials without sending or returning partial output`() = runBlocking {
        val trigger = randomQueryLevelTrigger()
        val monitor = randomQueryLevelMonitor(triggers = listOf(trigger))
        val ctx = QueryLevelTriggerExecutionContext(
            monitor, trigger, emptyList(), emptyList(), Instant.EPOCH, Instant.EPOCH,
            clusterSettings = clusterSettings
        )

        assertRejectedActions(partialScript(), "Partial templates are not supported") { action, dryrun ->
            QueryLevelMonitorRunner.runAction(action, ctx, monitorCtx, monitor, dryrun)
        }
        verifyNoInteractions(client, scriptService)
    }

    fun `test workflow actions reject partials without sending or returning partial output`() = runBlocking {
        val trigger = randomChainedAlertTrigger()
        val workflow = randomWorkflow(monitorIds = listOf("monitor-1", "monitor-2"), triggers = listOf(trigger))
        val workflowResult = WorkflowRunResult(
            workflowId = workflow.id,
            workflowName = workflow.name,
            monitorRunResults = emptyList(),
            executionStartTime = Instant.EPOCH,
            executionEndTime = Instant.EPOCH,
            executionId = "test-execution",
            error = null,
            triggerResults = mutableMapOf()
        )
        val ctx = ChainedAlertTriggerExecutionContext(
            workflow, workflowResult, Instant.EPOCH, Instant.EPOCH,
            trigger = trigger, alertGeneratingMonitors = emptySet(), monitorIdToAlertIdsMap = emptyMap()
        )

        assertRejectedActions(partialScript(), "Partial templates are not supported") { action, dryrun ->
            CompositeWorkflowRunner.runAction(action, ctx, monitorCtx, workflow, dryrun)
        }
        verifyNoInteractions(client, scriptService)
    }

    fun `test document fanout actions reject partials before notification access`() = runBlocking {
        val clusterService = mock(ClusterService::class.java)
        `when`(clusterService.clusterSettings).thenReturn(clusterSettings)
        `when`(client.threadPool()).thenReturn(threadPool)
        val fanout = TransportDocLevelMonitorFanOutAction(
            mock(TransportService::class.java), client, ActionFilters(emptySet()), clusterService,
            mock(TriggerService::class.java), mock(AlertService::class.java), scriptService, settings, NamedXContentRegistry.EMPTY
        )
        val trigger = randomDocumentLevelTrigger()
        val monitor = randomDocumentLevelMonitor(triggers = listOf(trigger))
        val ctx = DocumentLevelTriggerExecutionContext(monitor, trigger, clusterSettings = clusterSettings)

        assertRejectedActions(partialScript(), "Partial templates are not supported") { action, dryrun ->
            fanout.runAction(action, ctx, monitor, dryrun)
        }
        // The fan-out runner reads the clock through the client when constructing each failure result.
        verify(client, times(4)).threadPool()
        verifyNoMoreInteractions(client)
        verifyNoInteractions(scriptService)
    }

    fun `test oversized action output fails without sending or returning a rendered subject`() = runBlocking {
        val trigger = randomQueryLevelTrigger()
        val monitor = randomQueryLevelMonitor(triggers = listOf(trigger))
        val ctx = QueryLevelTriggerExecutionContext(
            monitor, trigger, emptyList(), emptyList(), Instant.EPOCH, Instant.EPOCH,
            clusterSettings = clusterSettings
        )
        val oversizedScript = randomTemplateScript(
            "{{payload}}", mapOf("payload" to "x".repeat(MustacheTemplateService.MAX_TEMPLATE_OUTPUT_CHARS + 1))
        )

        assertRejectedActions(oversizedScript, "maximum output size") { action, dryrun ->
            QueryLevelMonitorRunner.runAction(action, ctx, monitorCtx, monitor, dryrun)
        }
        verifyNoInteractions(client, scriptService)
    }

    fun `test rejected search templates produce input errors without executing searches`() = runBlocking {
        val searchModule = SearchModule(Settings.EMPTY, emptyList())
        val inputService = InputService(
            client, scriptService, NamedWriteableRegistry(searchModule.namedWriteables),
            NamedXContentRegistry(searchModule.namedXContents), mock(ClusterService::class.java), settings,
            mock(IndexNameExpressionResolver::class.java)
        )
        val rejectedTemplates = listOf(
            partialScript().idOrCode to "Partial templates are not supported",
            "x".repeat(MustacheTemplateService.MAX_TEMPLATE_OUTPUT_CHARS + 1) to "maximum output size"
        )

        for ((template, errorMessage) in rejectedTemplates) {
            val input = SearchInput(listOf("test-index"), SearchSourceBuilder().query(QueryBuilders.termQuery("message", template)))
            val monitor = randomQueryLevelMonitor(inputs = listOf(input), triggers = emptyList())
            for (adMonitor in listOf(false, true)) {
                val result = if (adMonitor) {
                    inputService.collectInputResultsForADMonitor(monitor, Instant.EPOCH, Instant.EPOCH)
                } else {
                    inputService.collectInputResults(monitor, Instant.EPOCH, Instant.EPOCH)
                }
                assertTemplateError(result.error, errorMessage)
                assertTrue("Rejected input must not return search results", result.results.isEmpty())
            }
        }
        verifyNoInteractions(client, scriptService)
    }

    private suspend fun assertRejectedActions(
        rejectedTemplate: Script,
        expectedError: String,
        runAction: suspend (Action, Boolean) -> ActionRunResult
    ) {
        for (rejectSubject in listOf(true, false)) {
            val validTemplate = randomTemplateScript("Successfully rendered content")
            val action = randomAction(
                subjectTemplate = if (rejectSubject) rejectedTemplate else validTemplate,
                template = if (rejectSubject) validTemplate else rejectedTemplate
            )
            for (dryrun in listOf(true, false)) {
                val result = runAction(action, dryrun)
                assertTemplateError(result.error, expectedError)
                assertTrue("A failed action must discard all output, including an already rendered subject", result.output.isEmpty())
                assertFalse("Template errors must not be reported as throttled actions", result.throttled)
            }
        }
    }

    private fun assertTemplateError(error: Exception?, expectedMessage: String) {
        assertNotNull("Expected template rendering to fail", error)
        val causes = generateSequence<Throwable>(error!!) { it.cause }.toList()
        assertTrue("Expected a Mustache error, got $error", causes.any { it is MustacheException })
        assertTrue("Expected '$expectedMessage', got $error", causes.any { it.message?.contains(expectedMessage) == true })
    }

    private fun partialScript(): Script {
        val partial = createTempFile("alerting-template-partial", ".mustache")
        Files.writeString(partial, "test fixture content")
        return randomTemplateScript("{{>${partial.toUri()}}}")
    }
}
