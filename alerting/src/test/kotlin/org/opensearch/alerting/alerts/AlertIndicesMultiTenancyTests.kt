/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.alerts

import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.mockito.Mockito
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.opensearch.Version
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client

class AlertIndicesMultiTenancyTests : OpenSearchTestCase() {

    private lateinit var client: Client
    private lateinit var threadPool: ThreadPool
    private lateinit var clusterService: ClusterService

    private val multiTenancySettings: Settings = Settings.builder()
        .put("plugins.alerting.multi_tenancy_enabled", true)
        .build()

    private val defaultSettings: Settings = Settings.builder().build()

    private fun buildClusterService(settings: Settings): ClusterService {
        val settingSet = hashSetOf<Setting<*>>()
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        settingSet.add(AlertingSettings.ALERT_HISTORY_ENABLED)
        settingSet.add(AlertingSettings.ALERT_HISTORY_MAX_DOCS)
        settingSet.add(AlertingSettings.ALERT_HISTORY_INDEX_MAX_AGE)
        settingSet.add(AlertingSettings.ALERT_HISTORY_ROLLOVER_PERIOD)
        settingSet.add(AlertingSettings.ALERT_HISTORY_RETENTION_PERIOD)
        settingSet.add(AlertingSettings.REQUEST_TIMEOUT)
        settingSet.add(AlertingSettings.FINDING_HISTORY_ENABLED)
        settingSet.add(AlertingSettings.FINDING_HISTORY_MAX_DOCS)
        settingSet.add(AlertingSettings.FINDING_HISTORY_INDEX_MAX_AGE)
        settingSet.add(AlertingSettings.FINDING_HISTORY_ROLLOVER_PERIOD)
        settingSet.add(AlertingSettings.FINDING_HISTORY_RETENTION_PERIOD)
        settingSet.add(AlertingSettings.MULTI_TENANCY_ENABLED)
        val discoveryNode = DiscoveryNode("node", buildNewFakeTransportAddress(), Version.CURRENT)
        val clusterSettings = ClusterSettings(settings, settingSet)
        return Mockito.spy(ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings))
    }

    @Before
    fun setup() {
        client = Mockito.mock(Client::class.java)
        threadPool = Mockito.mock(ThreadPool::class.java)
    }

    fun `test createOrUpdateAlertIndex does not call client when multiTenancyEnabled`() {
        clusterService = buildClusterService(multiTenancySettings)
        val alertIndices = AlertIndices(multiTenancySettings, client, threadPool, clusterService)

        runBlocking {
            alertIndices.createOrUpdateAlertIndex()
        }

        verify(client, never()).admin()
    }

    fun `test createOrUpdateAlertIndex with dataSources does not call client when multiTenancyEnabled`() {
        clusterService = buildClusterService(multiTenancySettings)
        val alertIndices = AlertIndices(multiTenancySettings, client, threadPool, clusterService)

        runBlocking {
            alertIndices.createOrUpdateAlertIndex(DataSources())
        }

        verify(client, never()).admin()
    }

    fun `test createOrUpdateInitialAlertHistoryIndex does not call client when multiTenancyEnabled`() {
        clusterService = buildClusterService(multiTenancySettings)
        val alertIndices = AlertIndices(multiTenancySettings, client, threadPool, clusterService)

        runBlocking {
            alertIndices.createOrUpdateInitialAlertHistoryIndex()
        }

        verify(client, never()).admin()
    }

    fun `test createOrUpdateInitialAlertHistoryIndex with dataSources does not call client when multiTenancyEnabled`() {
        clusterService = buildClusterService(multiTenancySettings)
        val alertIndices = AlertIndices(multiTenancySettings, client, threadPool, clusterService)

        runBlocking {
            alertIndices.createOrUpdateInitialAlertHistoryIndex(DataSources())
        }

        verify(client, never()).admin()
    }

    fun `test createOrUpdateInitialFindingHistoryIndex does not call client when multiTenancyEnabled`() {
        clusterService = buildClusterService(multiTenancySettings)
        val alertIndices = AlertIndices(multiTenancySettings, client, threadPool, clusterService)

        runBlocking {
            alertIndices.createOrUpdateInitialFindingHistoryIndex()
        }

        verify(client, never()).admin()
    }

    fun `test createOrUpdateInitialFindingHistoryIndex with dataSources does not call client when multiTenancyEnabled`() {
        clusterService = buildClusterService(multiTenancySettings)
        val alertIndices = AlertIndices(multiTenancySettings, client, threadPool, clusterService)

        runBlocking {
            alertIndices.createOrUpdateInitialFindingHistoryIndex(DataSources())
        }

        verify(client, never()).admin()
    }

    fun `test isAlertInitialized returns true when multiTenancyEnabled`() {
        clusterService = buildClusterService(multiTenancySettings)
        val alertIndices = AlertIndices(multiTenancySettings, client, threadPool, clusterService)

        assertTrue(alertIndices.isAlertInitialized())
    }

    fun `test isAlertInitialized with dataSources returns true when multiTenancyEnabled`() {
        clusterService = buildClusterService(multiTenancySettings)
        val alertIndices = AlertIndices(multiTenancySettings, client, threadPool, clusterService)

        assertTrue(alertIndices.isAlertInitialized(DataSources()))
    }

    fun `test isAlertInitialized returns false when multiTenancyDisabled and indices not initialized`() {
        clusterService = buildClusterService(defaultSettings)
        val alertIndices = AlertIndices(defaultSettings, client, threadPool, clusterService)

        assertFalse(alertIndices.isAlertInitialized())
    }
}
