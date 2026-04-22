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
import org.mockito.Mockito.timeout
import org.mockito.Mockito.verify
import org.opensearch.action.support.ActionFilters
import org.opensearch.alerting.service.ExternalSchedulerService
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.alerting.action.DeleteMonitorRequest
import org.opensearch.commons.alerting.action.DeleteMonitorResponse
import org.opensearch.core.action.ActionListener
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.remote.metadata.client.GetDataObjectRequest
import org.opensearch.remote.metadata.client.GetDataObjectResponse
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import org.mockito.Mockito.`when` as whenever

@ThreadLeakFilters(filters = [TransportDeleteMonitorActionTests.CoroutineThreadFilter::class])
class TransportDeleteMonitorActionTests : OpenSearchTestCase() {

    class CoroutineThreadFilter : ThreadFilter {
        override fun reject(t: Thread): Boolean = t.name.startsWith("DefaultDispatcher-worker")
    }

    private lateinit var client: Client
    private lateinit var sdkClient: SdkClient
    private lateinit var clusterService: ClusterService
    private lateinit var threadPool: ThreadPool
    private lateinit var threadContext: ThreadContext

    @Before
    fun setup() {
        client = Mockito.mock(Client::class.java)
        sdkClient = Mockito.mock(SdkClient::class.java)
        clusterService = Mockito.mock(ClusterService::class.java)
        threadPool = Mockito.mock(ThreadPool::class.java)
        threadContext = ThreadContext(Settings.EMPTY)

        whenever(client.threadPool()).thenReturn(threadPool)
        whenever(threadPool.threadContext).thenReturn(threadContext)

        val settingSet = hashSetOf<Setting<*>>()
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        settingSet.add(AlertingSettings.FILTER_BY_BACKEND_ROLES)
        settingSet.add(AlertingSettings.EXTERNAL_SCHEDULER_ENABLED)
        settingSet.add(AlertingSettings.EXTERNAL_SCHEDULER_ACCOUNT_ID)
        settingSet.add(AlertingSettings.EXTERNAL_SCHEDULER_ROLE_ARN)
        val clusterSettings = ClusterSettings(Settings.EMPTY, settingSet)
        whenever(clusterService.clusterSettings).thenReturn(clusterSettings)
    }

    fun `test SDK getDataObject is called`() {
        val response = GetDataObjectResponse.builder()
            .id("test-monitor-id")
            .index(".opendistro-alerting-config")
            .source(null)
            .build()
        whenever(sdkClient.getDataObject(any(GetDataObjectRequest::class.java))).thenReturn(response)

        val action = createAction()
        val request = DeleteMonitorRequest("test-monitor-id", org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<DeleteMonitorResponse>

        invokeDoExecute(action, request, listener)

        verify(sdkClient, timeout(1000)).getDataObject(any(GetDataObjectRequest::class.java))
    }

    fun `test getMonitor not found calls onFailure`() {
        val response = GetDataObjectResponse.builder()
            .id("test-monitor-id")
            .index(".opendistro-alerting-config")
            .source(null)
            .build()
        whenever(sdkClient.getDataObject(any(GetDataObjectRequest::class.java))).thenReturn(response)

        val action = createAction()
        val request = DeleteMonitorRequest("test-monitor-id", org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<DeleteMonitorResponse>

        invokeDoExecute(action, request, listener)

        verify(listener, timeout(1000)).onFailure(any())
    }

    fun `test SDK exception propagated to listener`() {
        whenever(sdkClient.getDataObject(any(GetDataObjectRequest::class.java)))
            .thenThrow(RuntimeException("SDK connection failed"))

        val action = createAction()
        val request = DeleteMonitorRequest("test-monitor-id", org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE)
        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<DeleteMonitorResponse>

        invokeDoExecute(action, request, listener)

        verify(listener, timeout(1000)).onFailure(any())
    }

    fun `test action constructs with external scheduler enabled`() {
        val settings = Settings.builder()
            .put("plugins.alerting.external_scheduler.enabled", true)
            .put("plugins.alerting.external_scheduler.account_id", "111111111111")
            .put("plugins.alerting.external_scheduler.role_arn", "arn:aws:iam::111:role/eb")
            .build()

        val settingSet = hashSetOf<Setting<*>>()
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        settingSet.add(AlertingSettings.FILTER_BY_BACKEND_ROLES)
        settingSet.add(AlertingSettings.EXTERNAL_SCHEDULER_ENABLED)
        settingSet.add(AlertingSettings.EXTERNAL_SCHEDULER_ACCOUNT_ID)
        settingSet.add(AlertingSettings.EXTERNAL_SCHEDULER_ROLE_ARN)
        val cs = ClusterSettings(settings, settingSet)
        whenever(clusterService.clusterSettings).thenReturn(cs)

        val action = TransportDeleteMonitorAction(
            Mockito.mock(TransportService::class.java),
            client,
            Mockito.mock(ActionFilters::class.java),
            clusterService,
            settings,
            Mockito.mock(NamedXContentRegistry::class.java),
            sdkClient
        )
        // Action should construct without error — settings consumers wired correctly
        assertNotNull(action)
    }

    fun `test action constructs with external scheduler disabled by default`() {
        // Default Settings.EMPTY → externalSchedulerEnabled = false
        val action = createAction()
        assertNotNull(action)
    }

    fun `test ThreadContext scheduler account id is readable`() {
        threadContext.putTransient(ExternalSchedulerService.SCHEDULER_ACCOUNT_ID_KEY, "999999999999")
        val value = threadContext.getTransient<String>(ExternalSchedulerService.SCHEDULER_ACCOUNT_ID_KEY)
        assertEquals("999999999999", value)
    }

    private fun invokeDoExecute(
        action: TransportDeleteMonitorAction,
        request: DeleteMonitorRequest,
        listener: ActionListener<DeleteMonitorResponse>
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

    private fun createAction(): TransportDeleteMonitorAction {
        return TransportDeleteMonitorAction(
            Mockito.mock(TransportService::class.java),
            client,
            Mockito.mock(ActionFilters::class.java),
            clusterService,
            Settings.EMPTY,
            Mockito.mock(NamedXContentRegistry::class.java),
            sdkClient
        )
    }
}
