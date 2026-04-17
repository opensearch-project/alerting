/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.junit.Before
import org.mockito.Mockito
import org.mockito.Mockito.verify
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.support.ActionFilters
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.action.ExecuteMonitorRequest
import org.opensearch.alerting.action.ExecuteMonitorResponse
import org.opensearch.alerting.action.ExecuteWorkflowRequest
import org.opensearch.alerting.action.ExecuteWorkflowResponse
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.core.lock.LockService
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.alerting.action.DeleteWorkflowRequest
import org.opensearch.commons.alerting.action.DeleteWorkflowResponse
import org.opensearch.commons.alerting.action.GetWorkflowAlertsRequest
import org.opensearch.commons.alerting.action.GetWorkflowAlertsResponse
import org.opensearch.commons.alerting.action.GetWorkflowRequest
import org.opensearch.commons.alerting.action.GetWorkflowResponse
import org.opensearch.commons.alerting.action.IndexMonitorRequest
import org.opensearch.commons.alerting.action.IndexMonitorResponse
import org.opensearch.commons.alerting.action.IndexWorkflowRequest
import org.opensearch.commons.alerting.action.IndexWorkflowResponse
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.IntervalSchedule
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.alerting.model.Table
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.rest.RestRequest
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.mockito.Mockito.`when` as whenever

class TransportMultiTenancyBlockTests : OpenSearchTestCase() {

    private lateinit var client: Client
    private lateinit var transportService: TransportService
    private lateinit var actionFilters: ActionFilters
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var clusterService: ClusterService
    private lateinit var threadPool: ThreadPool
    private lateinit var threadContext: ThreadContext

    private val multiTenancySettings: Settings = Settings.builder()
        .put("plugins.alerting.multi_tenancy_enabled", true)
        .build()

    @Before
    fun setup() {
        client = Mockito.mock(Client::class.java)
        transportService = Mockito.mock(TransportService::class.java)
        actionFilters = Mockito.mock(ActionFilters::class.java)
        xContentRegistry = Mockito.mock(NamedXContentRegistry::class.java)
        clusterService = Mockito.mock(ClusterService::class.java)
        threadPool = Mockito.mock(ThreadPool::class.java)
        threadContext = ThreadContext(Settings.EMPTY)

        whenever(client.threadPool()).thenReturn(threadPool)
        whenever(threadPool.threadContext).thenReturn(threadContext)

        val settingSet = hashSetOf<Setting<*>>()
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        settingSet.add(AlertingSettings.FILTER_BY_BACKEND_ROLES)
        settingSet.add(AlertingSettings.MULTI_TENANCY_ENABLED)
        settingSet.add(AlertingSettings.ALERT_HISTORY_ENABLED)
        val clusterSettings = ClusterSettings(multiTenancySettings, settingSet)
        whenever(clusterService.clusterSettings).thenReturn(clusterSettings)
    }

    // --- Workflow action tests ---

    fun `test get workflow blocked when multi-tenancy enabled`() {
        val action = TransportGetWorkflowAction(
            transportService, client, actionFilters, xContentRegistry, clusterService, multiTenancySettings
        )
        val request = GetWorkflowRequest("test-id", RestRequest.Method.GET)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<GetWorkflowResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    fun `test index workflow blocked when multi-tenancy enabled`() {
        val action = TransportIndexWorkflowAction(
            transportService, client, actionFilters,
            Mockito.mock(ScheduledJobIndices::class.java),
            clusterService, multiTenancySettings, xContentRegistry,
            Mockito.mock(NamedWriteableRegistry::class.java)
        )
        val request = Mockito.mock(IndexWorkflowRequest::class.java)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<IndexWorkflowResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    fun `test delete workflow blocked when multi-tenancy enabled`() {
        val action = TransportDeleteWorkflowAction(
            transportService, client, actionFilters, clusterService,
            multiTenancySettings, xContentRegistry,
            Mockito.mock(LockService::class.java)
        )
        val request = DeleteWorkflowRequest("test-id", false)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<DeleteWorkflowResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    fun `test execute workflow blocked when multi-tenancy enabled`() {
        val action = TransportExecuteWorkflowAction(
            transportService, client,
            Mockito.mock(MonitorRunnerService::class.java),
            actionFilters, xContentRegistry, multiTenancySettings
        )
        val request = ExecuteWorkflowRequest(true, TimeValue(Instant.now().toEpochMilli()), "test-id", null)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<ExecuteWorkflowResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    fun `test get workflow alerts blocked when multi-tenancy enabled`() {
        val action = TransportGetWorkflowAlertsAction(
            transportService, client, clusterService, actionFilters,
            multiTenancySettings, xContentRegistry,
            Mockito.mock(SdkClient::class.java)
        )
        val request = GetWorkflowAlertsRequest(
            table = Table("asc", "monitor_id", null, 100, 0, null),
            severityLevel = "ALL",
            alertState = Alert.State.ACTIVE.name,
            alertIndex = "",
            associatedAlertsIndex = "",
            monitorIds = emptyList(),
            workflowIds = listOf("test-id"),
            alertIds = emptyList(),
            getAssociatedAlerts = false
        )
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<GetWorkflowAlertsResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    // --- Monitor type tests ---

    fun `test index doc-level monitor blocked when multi-tenancy enabled`() {
        val action = TransportIndexMonitorAction(
            transportService, client, actionFilters,
            Mockito.mock(ScheduledJobIndices::class.java),
            Mockito.mock(DocLevelMonitorQueries::class.java),
            clusterService, multiTenancySettings, xContentRegistry,
            Mockito.mock(NamedWriteableRegistry::class.java),
            Mockito.mock(SdkClient::class.java)
        )
        val monitor = Monitor(
            name = "test", monitorType = Monitor.MonitorType.DOC_LEVEL_MONITOR.value,
            enabled = false, schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(), enabledTime = null, user = null,
            inputs = listOf(DocLevelMonitorInput("desc", listOf("index"), emptyList())),
            triggers = emptyList(), uiMetadata = mapOf()
        )
        val request = IndexMonitorRequest(
            Monitor.NO_ID, SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE, RestRequest.Method.POST, monitor
        )
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<IndexMonitorResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    fun `test index cluster-metrics monitor blocked when multi-tenancy enabled`() {
        val action = TransportIndexMonitorAction(
            transportService, client, actionFilters,
            Mockito.mock(ScheduledJobIndices::class.java),
            Mockito.mock(DocLevelMonitorQueries::class.java),
            clusterService, multiTenancySettings, xContentRegistry,
            Mockito.mock(NamedWriteableRegistry::class.java),
            Mockito.mock(SdkClient::class.java)
        )
        val monitor = Monitor(
            name = "test", monitorType = Monitor.MonitorType.CLUSTER_METRICS_MONITOR.value,
            enabled = false, schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(), enabledTime = null, user = null,
            inputs = listOf(SearchInput(emptyList(), SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))),
            triggers = emptyList(), uiMetadata = mapOf()
        )
        val request = IndexMonitorRequest(
            Monitor.NO_ID, SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE, RestRequest.Method.POST, monitor
        )
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<IndexMonitorResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    fun `test index query-level monitor allowed when multi-tenancy enabled`() {
        val action = TransportIndexMonitorAction(
            transportService, client, actionFilters,
            Mockito.mock(ScheduledJobIndices::class.java),
            Mockito.mock(DocLevelMonitorQueries::class.java),
            clusterService, multiTenancySettings, xContentRegistry,
            Mockito.mock(NamedWriteableRegistry::class.java),
            Mockito.mock(SdkClient::class.java)
        )
        val monitor = Monitor(
            name = "test", monitorType = Monitor.MonitorType.QUERY_LEVEL_MONITOR.value,
            enabled = false, schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(), enabledTime = null, user = null,
            inputs = listOf(SearchInput(emptyList(), SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))),
            triggers = emptyList(), uiMetadata = mapOf()
        )
        val request = IndexMonitorRequest(
            Monitor.NO_ID, SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE, RestRequest.Method.POST, monitor
        )
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<IndexMonitorResponse>

        invokeDoExecute(action, request, listener)

        // Should NOT call onFailure with METHOD_NOT_ALLOWED — it proceeds past the check
        verify(listener, Mockito.never()).onFailure(
            org.mockito.ArgumentMatchers.argThat { ex ->
                val cause = (ex as? org.opensearch.commons.alerting.util.AlertingException)?.cause ?: ex
                cause is OpenSearchStatusException && cause.status() == RestStatus.METHOD_NOT_ALLOWED
            }
        )
    }

    fun `test execute inline doc-level monitor blocked when multi-tenancy enabled`() {
        val action = TransportExecuteMonitorAction(
            transportService, client, clusterService,
            Mockito.mock(MonitorRunnerService::class.java),
            actionFilters, xContentRegistry,
            Mockito.mock(DocLevelMonitorQueries::class.java),
            multiTenancySettings, Mockito.mock(SdkClient::class.java)
        )
        val monitor = Monitor(
            name = "test", monitorType = Monitor.MonitorType.DOC_LEVEL_MONITOR.value,
            enabled = false, schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(), enabledTime = null, user = null,
            inputs = listOf(DocLevelMonitorInput("desc", listOf("index"), emptyList())),
            triggers = emptyList(), uiMetadata = mapOf()
        )
        val request = ExecuteMonitorRequest(true, TimeValue(Instant.now().toEpochMilli()), null, monitor)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<ExecuteMonitorResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    // --- Helpers ---

    private fun assertMethodNotAllowed(listener: ActionListener<*>) {
        val captor = org.mockito.ArgumentCaptor.forClass(Exception::class.java)
        verify(listener).onFailure(captor.capture())
        val cause = (captor.value as? org.opensearch.commons.alerting.util.AlertingException)?.cause
            ?: captor.value
        assertTrue(cause is OpenSearchStatusException)
        assertEquals(RestStatus.METHOD_NOT_ALLOWED, (cause as OpenSearchStatusException).status())
    }

    private fun invokeDoExecute(action: Any, request: Any, listener: ActionListener<*>) {
        val method = action.javaClass.getDeclaredMethod(
            "doExecute",
            org.opensearch.tasks.Task::class.java,
            org.opensearch.action.ActionRequest::class.java,
            ActionListener::class.java
        )
        method.isAccessible = true
        method.invoke(action, Mockito.mock(org.opensearch.tasks.Task::class.java), request, listener)
    }
}
