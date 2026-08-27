/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import com.carrotsearch.randomizedtesting.ThreadFilter
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters
import org.junit.Before
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.never
import org.mockito.Mockito.timeout
import org.mockito.Mockito.verify
import org.opensearch.OpenSearchSecurityException
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.action.ExecuteMonitorRequest
import org.opensearch.alerting.action.ExecuteMonitorResponse
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.IntervalSchedule
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.query.QueryBuilders
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.time.Instant
import java.time.temporal.ChronoUnit

@ThreadLeakFilters(filters = [TransportExecuteMonitorActionTests.CoroutineThreadFilter::class])
class TransportExecuteMonitorActionTests : OpenSearchTestCase() {

    class CoroutineThreadFilter : ThreadFilter {
        override fun reject(t: Thread): Boolean = t.name.startsWith("DefaultDispatcher-worker")
    }

    private lateinit var client: Client
    private lateinit var clusterService: ClusterService
    private lateinit var threadPool: ThreadPool
    private lateinit var threadContext: ThreadContext

    @Before
    fun setup() {
        client = Mockito.mock(Client::class.java)
        clusterService = Mockito.mock(ClusterService::class.java)
        threadPool = Mockito.mock(ThreadPool::class.java)
        threadContext = ThreadContext(Settings.EMPTY)

        Mockito.`when`(client.threadPool()).thenReturn(threadPool)
        Mockito.`when`(threadPool.threadContext).thenReturn(threadContext)

        // SecureTransportAction.listenFilterBySettingChange registers update consumers for both of these,
        // so they must be present in the ClusterSettings used by the mocked ClusterService.
        val settingSet = hashSetOf<Setting<*>>()
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        settingSet.add(AlertingSettings.FILTER_BY_BACKEND_ROLES)
        settingSet.add(AlertingSettings.FILTER_BY_BACKEND_ROLES_ACCESS_STRATEGY)
        val clusterSettings = ClusterSettings(Settings.EMPTY, settingSet)
        Mockito.`when`(clusterService.clusterSettings).thenReturn(clusterSettings)

        val metadata = Mockito.mock(Metadata::class.java)
        Mockito.`when`(metadata.indicesLookup).thenReturn(sortedMapOf())
        Mockito.`when`(metadata.hasAlias(any())).thenReturn(false)
        Mockito.`when`(metadata.dataStreams()).thenReturn(mapOf())
        val clusterState = Mockito.mock(ClusterState::class.java)
        Mockito.`when`(clusterState.metadata).thenReturn(metadata)
        Mockito.`when`(clusterState.metadata()).thenReturn(metadata)
        Mockito.`when`(clusterService.state()).thenReturn(clusterState)
    }

    fun `test inline monitor with search input triggers index access check`() {
        Mockito.doAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.getArgument<ActionListener<SearchResponse>>(1)
            listener.onFailure(OpenSearchSecurityException("no permissions for [indices:data/read/search]"))
            null
        }.`when`(client).search(any(SearchRequest::class.java), any())

        val action = createAction()
        val monitor = Monitor(
            name = "test", monitorType = Monitor.MonitorType.QUERY_LEVEL_MONITOR.value,
            enabled = false, schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(), enabledTime = null, user = null,
            inputs = listOf(SearchInput(listOf("sensitive-index"), SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))),
            triggers = emptyList(), uiMetadata = mapOf()
        )
        val request = ExecuteMonitorRequest(true, TimeValue(Instant.now().toEpochMilli()), null, monitor)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<ExecuteMonitorResponse>

        invokeDoExecute(action, request, listener)

        val captor = org.mockito.ArgumentCaptor.forClass(Exception::class.java)
        verify(listener, timeout(1000)).onFailure(captor.capture())
        val exception = captor.value
        assertTrue("Expected AlertingException but got ${exception.javaClass}", exception is AlertingException)
        assertEquals(RestStatus.FORBIDDEN, (exception as AlertingException).status())
    }

    fun `test inline doc-level monitor with custom indices triggers index access check`() {
        Mockito.doAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.getArgument<ActionListener<SearchResponse>>(1)
            listener.onFailure(OpenSearchSecurityException("no permissions for [indices:data/read/search]"))
            null
        }.`when`(client).search(any(SearchRequest::class.java), any())

        val action = createAction()
        val monitor = Monitor(
            name = "test", monitorType = Monitor.MonitorType.DOC_LEVEL_MONITOR.value,
            enabled = false, schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(), enabledTime = null, user = null,
            inputs = listOf(DocLevelMonitorInput("desc", listOf("unauthorized-index"), emptyList())),
            triggers = emptyList(), uiMetadata = mapOf()
        )
        val request = ExecuteMonitorRequest(true, TimeValue(Instant.now().toEpochMilli()), null, monitor)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<ExecuteMonitorResponse>

        invokeDoExecute(action, request, listener)

        val captor = org.mockito.ArgumentCaptor.forClass(Exception::class.java)
        verify(listener, timeout(1000)).onFailure(captor.capture())
        val exception = captor.value
        assertTrue("Expected AlertingException but got ${exception.javaClass}", exception is AlertingException)
        assertEquals(RestStatus.FORBIDDEN, (exception as AlertingException).status())
    }

    fun `test inline monitor with no inputs does not trigger access check`() {
        val action = createAction()
        val monitor = Monitor(
            name = "test", monitorType = Monitor.MonitorType.QUERY_LEVEL_MONITOR.value,
            enabled = false, schedule = IntervalSchedule(5, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(), enabledTime = null, user = null,
            inputs = emptyList(),
            triggers = emptyList(), uiMetadata = mapOf()
        )
        val request = ExecuteMonitorRequest(true, TimeValue(Instant.now().toEpochMilli()), null, monitor)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<ExecuteMonitorResponse>

        try {
            invokeDoExecute(action, request, listener)
        } catch (e: java.lang.reflect.InvocationTargetException) {
            // MonitorRunnerService is not initialized in unit tests — expected once execution is launched
        }

        verify(client, never()).search(any(SearchRequest::class.java), any())
    }

    private fun invokeDoExecute(
        action: TransportExecuteMonitorAction,
        request: ExecuteMonitorRequest,
        listener: ActionListener<ExecuteMonitorResponse>
    ) {
        val method = action.javaClass.getDeclaredMethod(
            "doExecute",
            org.opensearch.tasks.Task::class.java,
            ExecuteMonitorRequest::class.java,
            ActionListener::class.java
        )
        method.isAccessible = true
        method.invoke(action, Mockito.mock(org.opensearch.tasks.Task::class.java), request, listener)
    }

    private fun createAction(): TransportExecuteMonitorAction {
        return TransportExecuteMonitorAction(
            Mockito.mock(TransportService::class.java),
            client, clusterService,
            MonitorRunnerService,
            Mockito.mock(ActionFilters::class.java),
            Mockito.mock(NamedXContentRegistry::class.java),
            DocLevelMonitorQueries(client, clusterService),
            Settings.EMPTY,
            Mockito.mock(SdkClient::class.java)
        )
    }
}
