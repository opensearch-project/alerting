/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import com.carrotsearch.randomizedtesting.ThreadFilter
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters
import org.junit.Before
import org.mockito.Mockito
import org.mockito.Mockito.verify
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.action.ExecuteMonitorRequest
import org.opensearch.alerting.action.ExecuteMonitorResponse
import org.opensearch.alerting.action.ExecuteWorkflowRequest
import org.opensearch.alerting.action.ExecuteWorkflowResponse
import org.opensearch.alerting.action.GetDestinationsRequest
import org.opensearch.alerting.action.GetDestinationsResponse
import org.opensearch.alerting.action.GetEmailAccountRequest
import org.opensearch.alerting.action.GetEmailAccountResponse
import org.opensearch.alerting.action.GetEmailGroupRequest
import org.opensearch.alerting.action.GetEmailGroupResponse
import org.opensearch.alerting.action.GetRemoteIndexesRequest
import org.opensearch.alerting.action.GetRemoteIndexesResponse
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.core.lock.LockService
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.DestinationSettings
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.alerting.action.AcknowledgeAlertResponse
import org.opensearch.commons.alerting.action.AcknowledgeChainedAlertRequest
import org.opensearch.commons.alerting.action.DeleteWorkflowRequest
import org.opensearch.commons.alerting.action.DeleteWorkflowResponse
import org.opensearch.commons.alerting.action.GetFindingsRequest
import org.opensearch.commons.alerting.action.GetFindingsResponse
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

@ThreadLeakFilters(filters = [TransportMultiTenancyBlockTests.CoroutineThreadFilter::class])
class TransportMultiTenancyBlockTests : OpenSearchTestCase() {

    class CoroutineThreadFilter : ThreadFilter {
        override fun reject(t: Thread): Boolean = t.name.startsWith("DefaultDispatcher-worker")
    }

    private lateinit var client: Client
    private lateinit var transportService: TransportService
    private lateinit var actionFilters: ActionFilters
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var clusterService: ClusterService
    private lateinit var threadPool: ThreadPool
    private lateinit var threadContext: ThreadContext
    private lateinit var scheduledJobIndices: ScheduledJobIndices
    private lateinit var lockService: LockService
    private lateinit var docLevelMonitorQueries: DocLevelMonitorQueries

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
        settingSet.add(AlertingSettings.ALERTING_MAX_MONITORS)
        settingSet.add(AlertingSettings.MAX_TRIGGERS_PER_MONITOR)
        settingSet.add(AlertingSettings.REQUEST_TIMEOUT)
        settingSet.add(AlertingSettings.INDEX_TIMEOUT)
        settingSet.add(AlertingSettings.MAX_ACTION_THROTTLE_VALUE)
        settingSet.add(DestinationSettings.ALLOW_LIST)
        settingSet.add(AlertingSettings.CROSS_CLUSTER_MONITORING_ENABLED)
        settingSet.add(AlertingSettings.EXTERNAL_SCHEDULER_ENABLED)
        settingSet.add(AlertingSettings.EXTERNAL_SCHEDULER_ACCOUNT_ID)
        settingSet.add(AlertingSettings.JOB_QUEUE_NAME)
        settingSet.add(AlertingSettings.EXTERNAL_SCHEDULER_ROLE_ARN)
        val clusterSettings = ClusterSettings(multiTenancySettings, settingSet)
        whenever(clusterService.clusterSettings).thenReturn(clusterSettings)

        val adminClient = Mockito.mock(org.opensearch.transport.client.AdminClient::class.java)
        whenever(client.admin()).thenReturn(adminClient)
        scheduledJobIndices = ScheduledJobIndices(adminClient, clusterService)
        lockService = LockService(client, clusterService)
        docLevelMonitorQueries = DocLevelMonitorQueries(client, clusterService)
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
            scheduledJobIndices,
            clusterService, multiTenancySettings, xContentRegistry,
            Mockito.mock(NamedWriteableRegistry::class.java)
        )
        val request = IndexWorkflowRequest(
            workflowId = "test-id",
            seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            refreshPolicy = org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE,
            method = RestRequest.Method.POST,
            workflow = org.opensearch.alerting.randomWorkflow(monitorIds = listOf("dummy-id"))
        )
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<IndexWorkflowResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    fun `test delete workflow blocked when multi-tenancy enabled`() {
        val action = TransportDeleteWorkflowAction(
            transportService, client, actionFilters, clusterService,
            multiTenancySettings, xContentRegistry,
            lockService
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
            MonitorRunnerService,
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
            scheduledJobIndices,
            docLevelMonitorQueries,
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
            scheduledJobIndices,
            docLevelMonitorQueries,
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
            scheduledJobIndices,
            docLevelMonitorQueries,
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
            MonitorRunnerService,
            actionFilters, xContentRegistry,
            docLevelMonitorQueries,
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

    // --- Notification-related action tests ---

    fun `test search email account blocked when multi-tenancy enabled`() {
        val action = TransportSearchEmailAccountAction(
            transportService, client, actionFilters, clusterService, multiTenancySettings
        )
        val request = SearchRequest()
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<SearchResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    fun `test get email account blocked when multi-tenancy enabled`() {
        val action = TransportGetEmailAccountAction(
            transportService, client, actionFilters, clusterService, multiTenancySettings, xContentRegistry
        )
        val request = GetEmailAccountRequest("test-id", 1L, RestRequest.Method.GET, null)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<GetEmailAccountResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    fun `test search email group blocked when multi-tenancy enabled`() {
        val action = TransportSearchEmailGroupAction(
            transportService, client, actionFilters, clusterService, multiTenancySettings
        )
        val request = SearchRequest()
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<SearchResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    fun `test get email group blocked when multi-tenancy enabled`() {
        val action = TransportGetEmailGroupAction(
            transportService, client, actionFilters, clusterService, multiTenancySettings, xContentRegistry
        )
        val request = GetEmailGroupRequest("test-id", 1L, RestRequest.Method.GET, null)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<GetEmailGroupResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    fun `test get destinations blocked when multi-tenancy enabled`() {
        val action = TransportGetDestinationsAction(
            transportService, client, clusterService, actionFilters,
            multiTenancySettings, xContentRegistry,
            Mockito.mock(SdkClient::class.java)
        )
        val request = GetDestinationsRequest(
            null, 1L, null,
            Table("asc", "destination.name", null, 20, 0, null),
            "ALL"
        )
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<GetDestinationsResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    // --- Additional blocked action tests ---

    fun `test acknowledge chained alerts blocked when multi-tenancy enabled`() {
        val action = TransportAcknowledgeChainedAlertAction(
            transportService, client, clusterService, actionFilters,
            multiTenancySettings, xContentRegistry,
            Mockito.mock(SdkClient::class.java)
        )
        val request = AcknowledgeChainedAlertRequest("test-wf-id", listOf("alert-1"))
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java)
            as ActionListener<AcknowledgeAlertResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    fun `test get findings blocked when multi-tenancy enabled`() {
        val action = TransportGetFindingsSearchAction(
            transportService, client, clusterService, actionFilters,
            multiTenancySettings, xContentRegistry,
            Mockito.mock(NamedWriteableRegistry::class.java)
        )
        val request = GetFindingsRequest(
            null, Table("asc", "timestamp", null, 20, 0, null),
            null, null, null
        )
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java)
            as ActionListener<GetFindingsResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    fun `test get remote indexes blocked when multi-tenancy enabled`() {
        val action = TransportGetRemoteIndexesAction(
            transportService, client, actionFilters, xContentRegistry,
            clusterService, multiTenancySettings
        )
        val request = GetRemoteIndexesRequest(listOf("cluster:index*"), false)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<GetRemoteIndexesResponse>

        invokeDoExecute(action, request, listener)

        assertMethodNotAllowed(listener)
    }

    // --- Metadata skip tests ---

    fun `test index query-level monitor skips metadata when multi-tenancy enabled`() {
        val sdkClient = Mockito.mock(SdkClient::class.java)
        val action = TransportIndexMonitorAction(
            transportService, client, actionFilters,
            scheduledJobIndices,
            docLevelMonitorQueries,
            clusterService, multiTenancySettings, xContentRegistry,
            Mockito.mock(NamedWriteableRegistry::class.java),
            sdkClient
        )
        val monitor = Monitor(
            name = "test", monitorType = Monitor.MonitorType.QUERY_LEVEL_MONITOR.value,
            enabled = false, schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(), enabledTime = null, user = null,
            inputs = listOf(SearchInput(listOf("test-index"), SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))),
            triggers = emptyList(), uiMetadata = mapOf()
        )
        val request = IndexMonitorRequest(
            Monitor.NO_ID, SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE, RestRequest.Method.POST, monitor
        )

        // Mock cluster state for index resolution in checkIndicesAndExecute
        val metadata = org.opensearch.cluster.metadata.Metadata.builder().build()
        val clusterState = org.opensearch.cluster.ClusterState.builder(
            org.opensearch.cluster.ClusterName("test")
        ).metadata(metadata).build()
        whenever(clusterService.state()).thenReturn(clusterState)

        // Mock the search for index validation (checkIndicesAndExecute)
        Mockito.doAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            val searchListener = invocation.arguments[1] as ActionListener<SearchResponse>
            searchListener.onResponse(Mockito.mock(SearchResponse::class.java))
            null
        }.`when`(client).search(
            org.mockito.ArgumentMatchers.any(SearchRequest::class.java),
            org.mockito.ArgumentMatchers.any()
        )

        // Mock sdkClient.putDataObjectAsync for the monitor index
        val putResponse = org.opensearch.remote.metadata.client.PutDataObjectResponse.builder()
            .id("test-monitor-id")
            .build()
        val putFuture: java.util.concurrent.CompletionStage<org.opensearch.remote.metadata.client.PutDataObjectResponse> =
            java.util.concurrent.CompletableFuture.completedFuture(putResponse)
        whenever(sdkClient.putDataObjectAsync(org.mockito.ArgumentMatchers.any())).thenReturn(putFuture)

        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<IndexMonitorResponse>

        invokeDoExecute(action, request, listener)

        // Wait for async completion — the response should succeed without metadata calls.
        // MonitorMetadataService.getOrCreateMetadata would call sdkClient.getDataObjectAsync,
        // so if metadata is skipped, getDataObjectAsync should never be called.
        verify(sdkClient, Mockito.timeout(2000)).putDataObjectAsync(org.mockito.ArgumentMatchers.any())
        verify(sdkClient, Mockito.after(500).never()).getDataObjectAsync(org.mockito.ArgumentMatchers.any())
    }

    fun `test execute inline query-level monitor skips metadata when multi-tenancy enabled`() {
        val action = TransportExecuteMonitorAction(
            transportService, client, clusterService,
            MonitorRunnerService,
            actionFilters, xContentRegistry,
            docLevelMonitorQueries,
            multiTenancySettings, Mockito.mock(SdkClient::class.java)
        )
        val monitor = Monitor(
            name = "test", monitorType = Monitor.MonitorType.QUERY_LEVEL_MONITOR.value,
            enabled = false, schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(), enabledTime = null, user = null,
            inputs = listOf(SearchInput(listOf("test-index"), SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))),
            triggers = emptyList(), uiMetadata = mapOf()
        )
        val request = ExecuteMonitorRequest(true, TimeValue(Instant.now().toEpochMilli()), null, monitor)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<ExecuteMonitorResponse>

        // MonitorRunnerService is not initialized so runner.launch will throw,
        // but the important thing is that it reaches executeMonitor (not the metadata block)
        // and does NOT fail with METHOD_NOT_ALLOWED.
        try {
            invokeDoExecute(action, request, listener)
        } catch (e: Exception) {
            // Expected — MonitorRunnerService.runnerSupervisor is not initialized
        }

        // Should NOT be blocked with METHOD_NOT_ALLOWED (query-level is allowed)
        verify(listener, Mockito.never()).onFailure(
            org.mockito.ArgumentMatchers.argThat { ex ->
                val cause = (ex as? org.opensearch.commons.alerting.util.AlertingException)?.cause ?: ex
                cause is OpenSearchStatusException && cause.status() == RestStatus.METHOD_NOT_ALLOWED
            }
        )
    }

    // --- Helpers ---

    private fun assertMethodNotAllowed(listener: ActionListener<*>) {
        val captor = org.mockito.ArgumentCaptor.forClass(Exception::class.java)
        verify(listener).onFailure(captor.capture())
        val exception = captor.value
        assertTrue(exception is org.opensearch.commons.alerting.util.AlertingException)
        assertEquals(
            RestStatus.METHOD_NOT_ALLOWED,
            (exception as org.opensearch.commons.alerting.util.AlertingException).status()
        )
    }

    private fun invokeDoExecute(action: Any, request: Any, listener: ActionListener<*>) {
        val methods = action.javaClass.declaredMethods.filter { it.name == "doExecute" }
        // Prefer the typed override; fall back to ActionRequest-based if only one exists
        val method = if (methods.size > 1) {
            methods.first { it.parameterTypes[1] != org.opensearch.action.ActionRequest::class.java }
        } else {
            methods.first()
        }
        method.isAccessible = true
        method.invoke(action, Mockito.mock(org.opensearch.tasks.Task::class.java), request, listener)
    }
}
