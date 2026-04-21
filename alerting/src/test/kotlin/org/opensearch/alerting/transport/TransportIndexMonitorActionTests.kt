/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import com.carrotsearch.randomizedtesting.ThreadFilter
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters
import org.junit.Before
import org.mockito.Mockito
import org.opensearch.action.support.ActionFilters
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.service.ExternalSchedulerService
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.DestinationSettings
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import org.mockito.Mockito.`when` as whenever

@ThreadLeakFilters(filters = [TransportIndexMonitorActionTests.CoroutineThreadFilter::class])
class TransportIndexMonitorActionTests : OpenSearchTestCase() {

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

        whenever(client.threadPool()).thenReturn(threadPool)
        whenever(threadPool.threadContext).thenReturn(threadContext)
    }

    private fun clusterSettingsFor(settings: Settings): ClusterSettings {
        val settingSet = hashSetOf<Setting<*>>()
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        settingSet.add(AlertingSettings.FILTER_BY_BACKEND_ROLES)
        settingSet.add(AlertingSettings.MULTI_TENANCY_ENABLED)
        settingSet.add(AlertingSettings.ALERTING_MAX_MONITORS)
        settingSet.add(AlertingSettings.MAX_TRIGGERS_PER_MONITOR)
        settingSet.add(AlertingSettings.REQUEST_TIMEOUT)
        settingSet.add(AlertingSettings.INDEX_TIMEOUT)
        settingSet.add(AlertingSettings.MAX_ACTION_THROTTLE_VALUE)
        settingSet.add(DestinationSettings.ALLOW_LIST)
        settingSet.add(AlertingSettings.EXTERNAL_SCHEDULER_ENABLED)
        settingSet.add(AlertingSettings.EXTERNAL_SCHEDULER_ACCOUNT_ID)
        settingSet.add(AlertingSettings.EXTERNAL_SCHEDULER_QUEUE_ARN)
        settingSet.add(AlertingSettings.EXTERNAL_SCHEDULER_ROLE_ARN)
        return ClusterSettings(settings, settingSet)
    }

    private fun createAction(settings: Settings = Settings.EMPTY): TransportIndexMonitorAction {
        whenever(clusterService.clusterSettings).thenReturn(clusterSettingsFor(settings))
        val adminClient = Mockito.mock(org.opensearch.transport.client.AdminClient::class.java)
        whenever(client.admin()).thenReturn(adminClient)
        val scheduledJobIndices = ScheduledJobIndices(adminClient, clusterService)
        val docLevelMonitorQueries = DocLevelMonitorQueries(client, clusterService)

        return TransportIndexMonitorAction(
            Mockito.mock(TransportService::class.java),
            client,
            Mockito.mock(ActionFilters::class.java),
            scheduledJobIndices,
            docLevelMonitorQueries,
            clusterService,
            settings,
            Mockito.mock(NamedXContentRegistry::class.java),
            Mockito.mock(NamedWriteableRegistry::class.java),
            Mockito.mock(SdkClient::class.java)
        )
    }

    fun `test action constructs with default settings`() {
        val action = createAction()
        assertNotNull(action)
    }

    fun `test action constructs with external scheduler enabled`() {
        val settings = Settings.builder()
            .put("plugins.alerting.external_scheduler.enabled", true)
            .put("plugins.alerting.external_scheduler.account_id", "111111111111")
            .put("plugins.alerting.external_scheduler.queue_arn", "arn:aws:sqs:us-east-1:111:queue")
            .put("plugins.alerting.external_scheduler.role_arn", "arn:aws:iam::111:role/eb")
            .build()

        val action = createAction(settings)
        assertNotNull(action)
    }

    fun `test action constructs with external scheduler disabled explicitly`() {
        val settings = Settings.builder()
            .put("plugins.alerting.external_scheduler.enabled", false)
            .build()

        val action = createAction(settings)
        assertNotNull(action)
    }

    fun `test ThreadContext scheduler account id override is readable`() {
        threadContext.putTransient(ExternalSchedulerService.SCHEDULER_ACCOUNT_ID_KEY, "999999999999")
        val value = threadContext.getTransient<String>(ExternalSchedulerService.SCHEDULER_ACCOUNT_ID_KEY)
        assertEquals("999999999999", value)
    }

    fun `test scheduler settings are registered as dynamic`() {
        assertTrue(AlertingSettings.EXTERNAL_SCHEDULER_ENABLED.isDynamic)
        assertTrue(AlertingSettings.EXTERNAL_SCHEDULER_ACCOUNT_ID.isDynamic)
        assertTrue(AlertingSettings.EXTERNAL_SCHEDULER_QUEUE_ARN.isDynamic)
        assertTrue(AlertingSettings.EXTERNAL_SCHEDULER_ROLE_ARN.isDynamic)
    }

    fun `test scheduler enabled defaults to false`() {
        val value = AlertingSettings.EXTERNAL_SCHEDULER_ENABLED.get(Settings.EMPTY)
        assertFalse(value)
    }

    fun `test scheduler string settings default to empty`() {
        assertEquals("", AlertingSettings.EXTERNAL_SCHEDULER_ACCOUNT_ID.get(Settings.EMPTY))
        assertEquals("", AlertingSettings.EXTERNAL_SCHEDULER_QUEUE_ARN.get(Settings.EMPTY))
        assertEquals("", AlertingSettings.EXTERNAL_SCHEDULER_ROLE_ARN.get(Settings.EMPTY))
    }
}
