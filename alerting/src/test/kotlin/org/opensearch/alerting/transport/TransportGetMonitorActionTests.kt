/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import com.carrotsearch.randomizedtesting.ThreadFilter
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters
import org.junit.Before
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.alerting.AlertingPlugin.Companion.TENANT_ID_HEADER
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.alerting.action.GetMonitorRequest
import org.opensearch.commons.alerting.action.GetMonitorResponse
import org.opensearch.core.action.ActionListener
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.remote.metadata.client.GetDataObjectRequest
import org.opensearch.remote.metadata.client.GetDataObjectResponse
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.rest.RestRequest
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import org.mockito.Mockito.`when` as whenever

@ThreadLeakFilters(filters = [TransportGetMonitorActionTests.CoroutineThreadFilter::class])
class TransportGetMonitorActionTests : OpenSearchTestCase() {

    class CoroutineThreadFilter : ThreadFilter {
        override fun reject(t: Thread): Boolean = t.name.startsWith("DefaultDispatcher-worker")
    }

    private lateinit var client: Client
    private lateinit var sdkClient: SdkClient
    private lateinit var transportService: TransportService
    private lateinit var actionFilters: ActionFilters
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var clusterService: ClusterService
    private lateinit var threadPool: ThreadPool
    private lateinit var threadContext: ThreadContext

    @Before
    fun setup() {
        client = Mockito.mock(Client::class.java)
        sdkClient = Mockito.mock(SdkClient::class.java)
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
        val clusterSettings = ClusterSettings(Settings.EMPTY, settingSet)
        whenever(clusterService.clusterSettings).thenReturn(clusterSettings)
    }

    fun `test SDK called with correct tenantId and monitorId`() {
        val expectedTenantId = "test-tenant:test-scope"
        threadContext.putHeader(TENANT_ID_HEADER, expectedTenantId)

        val future: CompletionStage<GetDataObjectResponse> = CompletableFuture.completedFuture(
            GetDataObjectResponse.builder()
                .id("test-monitor-id")
                .index(".opendistro-alerting-config")
                .source(emptyMap())
                .build()
        )
        whenever(sdkClient.getDataObjectAsync(any(GetDataObjectRequest::class.java))).thenReturn(future)

        val action = createAction(Settings.builder().build())
        val request = GetMonitorRequest("test-monitor-id", 0L, RestRequest.Method.GET, null)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<GetMonitorResponse>

        invokeDoExecute(action, request, listener)

        val requestCaptor = ArgumentCaptor.forClass(GetDataObjectRequest::class.java)
        verify(sdkClient).getDataObjectAsync(requestCaptor.capture())
        assertEquals(expectedTenantId, requestCaptor.value.tenantId())
        assertEquals("test-monitor-id", requestCaptor.value.id())
    }

    fun `test SDK called with null tenantId when header absent`() {
        val future: CompletionStage<GetDataObjectResponse> = CompletableFuture.completedFuture(
            GetDataObjectResponse.builder()
                .id("test-monitor-id")
                .index(".opendistro-alerting-config")
                .source(emptyMap())
                .build()
        )
        whenever(sdkClient.getDataObjectAsync(any(GetDataObjectRequest::class.java))).thenReturn(future)

        val action = createAction(Settings.builder().build())
        val request = GetMonitorRequest("test-monitor-id", 0L, RestRequest.Method.GET, null)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<GetMonitorResponse>

        invokeDoExecute(action, request, listener)

        val requestCaptor = ArgumentCaptor.forClass(GetDataObjectRequest::class.java)
        verify(sdkClient).getDataObjectAsync(requestCaptor.capture())
        assertNull(requestCaptor.value.tenantId())
    }

    fun `test SDK exception propagated to listener`() {
        threadContext.putHeader(TENANT_ID_HEADER, "test-tenant:test-scope")

        val future: CompletionStage<GetDataObjectResponse> = CompletableFuture<GetDataObjectResponse>().also {
            it.completeExceptionally(RuntimeException("SDK connection failed"))
        }
        whenever(sdkClient.getDataObjectAsync(any(GetDataObjectRequest::class.java))).thenReturn(future)

        val action = createAction(Settings.builder().build())
        val request = GetMonitorRequest("test-monitor-id", 0L, RestRequest.Method.GET, null)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<GetMonitorResponse>

        invokeDoExecute(action, request, listener)

        verify(listener).onFailure(any())
    }

    fun `test multi-tenancy enabled skips getAssociatedWorkflows search`() {
        val settings = Settings.builder()
            .put("plugins.alerting.multi_tenancy_enabled", true)
            .build()

        val action = createAction(settings)
        // Invoke the private getAssociatedWorkflows method via reflection
        val method = action.javaClass.getDeclaredMethod("getAssociatedWorkflows", String::class.java, kotlin.coroutines.Continuation::class.java)
        method.isAccessible = true

        // Use runBlocking to call the suspend function
        val result = kotlinx.coroutines.runBlocking {
            kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn<List<*>> { cont ->
                method.invoke(action, "test-monitor-id", cont)
            }
        }

        // Should return empty list without searching
        assertTrue(result.isEmpty())
        // client.search should never be called
        verify(client, never()).search(any(SearchRequest::class.java), any())
    }

    private fun invokeDoExecute(
        action: TransportGetMonitorAction,
        request: GetMonitorRequest,
        listener: ActionListener<GetMonitorResponse>
    ) {
        val method = action.javaClass.getDeclaredMethod(
            "doExecute",
            org.opensearch.tasks.Task::class.java,
            org.opensearch.action.ActionRequest::class.java,
            ActionListener::class.java
        )
        method.isAccessible = true
        method.invoke(action, Mockito.mock(org.opensearch.tasks.Task::class.java), request, listener)
    }

    private fun createAction(settings: Settings): TransportGetMonitorAction {
        val settingSet = hashSetOf<Setting<*>>()
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        settingSet.add(AlertingSettings.FILTER_BY_BACKEND_ROLES)
        settingSet.add(AlertingSettings.MULTI_TENANCY_ENABLED)
        val clusterSettings = ClusterSettings(settings, settingSet)
        whenever(clusterService.clusterSettings).thenReturn(clusterSettings)

        return TransportGetMonitorAction(
            transportService, client, actionFilters, xContentRegistry, clusterService, settings, sdkClient
        )
    }
}
