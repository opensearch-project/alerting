/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import kotlinx.coroutines.runBlocking
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.mock
import org.opensearch.Version
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.alerting.randomDocumentLevelMonitor
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.AliasMetadata
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorMetadata
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient
import kotlin.test.assertFailsWith
import org.mockito.Mockito.`when` as whenever

class DocLevelMonitorQueriesTests : OpenSearchTestCase() {

    private lateinit var client: Client
    private lateinit var adminClient: AdminClient
    private lateinit var indicesAdminClient: IndicesAdminClient
    private lateinit var clusterService: ClusterService

    override fun setUp() {
        super.setUp()
        client = mock(Client::class.java)
        adminClient = mock(AdminClient::class.java)
        indicesAdminClient = mock(IndicesAdminClient::class.java)
        clusterService = mock(ClusterService::class.java)

        whenever(client.admin()).thenReturn(adminClient)
        whenever(adminClient.indices()).thenReturn(indicesAdminClient)

        Mockito.doAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<AcknowledgedResponse>
            listener.onResponse(AcknowledgedResponse(true))
            null
        }.`when`(indicesAdminClient).putMapping(any(PutMappingRequest::class.java), any())
    }

    private class TestableDocLevelMonitorQueries(
        client: Client,
        clusterService: ClusterService,
        var resolvedIndexName: String? = "concrete-query-index-000001"
    ) : DocLevelMonitorQueries(client, clusterService) {
        var getWriteIndexNameForAliasCallCount = 0
        var lastAliasQueried: String? = null
        var lastPutConcreteQueryIndex: String? = null

        override fun getWriteIndexNameForAlias(alias: String): String? {
            getWriteIndexNameForAliasCallCount++
            lastAliasQueried = alias
            return resolvedIndexName
        }

        override suspend fun adjustMaxFieldLimitForQueryIndex(sourceIndexFieldLimit: Long, concreteQueryIndex: String) {
            lastPutConcreteQueryIndex = concreteQueryIndex
        }
    }

    private fun createMonitor(
        queryIndexAlias: String = "query-index-alias",
        deleteQueryIndexInEveryRun: Boolean? = true,
        monitorId: String = "test-monitor-id"
    ): Monitor {
        val dataSources = DataSources(queryIndex = queryIndexAlias, queryIndexMappingsByType = emptyMap())
        return randomDocumentLevelMonitor(dataSources = dataSources).copy(
            id = monitorId,
            deleteQueryIndexInEveryRun = deleteQueryIndexInEveryRun
        )
    }

    private fun createMonitorMetadata(
        monitorId: String = "test-monitor-id",
        sourceIndex: String = "test-source-index",
        initialTargetQueryIndex: String? = null
    ): MonitorMetadata {
        val mapping = mutableMapOf<String, String>()
        if (initialTargetQueryIndex != null) {
            mapping[sourceIndex + monitorId] = initialTargetQueryIndex
        }
        return MonitorMetadata(
            id = "$monitorId-metadata",
            seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            monitorId = monitorId,
            lastActionExecutionTimes = emptyList(),
            lastRunContext = emptyMap(),
            sourceToQueryIndexMapping = mapping
        )
    }

    fun `test getWriteIndexNameForAlias returns write index name when alias has write index`() {
        val aliasName = "test-query-alias"
        val concreteIndex = "concrete-index-000001"

        val indexMetadata = IndexMetadata.builder(concreteIndex)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder(aliasName).writeIndex(true).build())
            .build()
        val metadata = Metadata.builder().put(indexMetadata, false).build()
        val clusterState = ClusterState.builder(ClusterName("test-cluster")).metadata(metadata).build()
        whenever(clusterService.state()).thenReturn(clusterState)

        val docLevelMonitorQueries = DocLevelMonitorQueries(client, clusterService)
        val writeIndex = docLevelMonitorQueries.getWriteIndexNameForAlias(aliasName)

        assertEquals("Expected write index name to match concrete backing index", concreteIndex, writeIndex)
    }

    fun `test getWriteIndexNameForAlias returns null when alias does not exist`() {
        val metadata = Metadata.builder().build()
        val clusterState = ClusterState.builder(ClusterName("test-cluster")).metadata(metadata).build()
        whenever(clusterService.state()).thenReturn(clusterState)

        val docLevelMonitorQueries = DocLevelMonitorQueries(client, clusterService)
        val writeIndex = docLevelMonitorQueries.getWriteIndexNameForAlias("non-existent-alias")

        assertNull("Expected null write index for non-existent alias", writeIndex)
    }

    fun `test updateQueryIndexMappings when targetQueryIndex is concrete and deleteQueryIndexInEveryRun true skips resolution`() {
        val monitor = createMonitor(queryIndexAlias = "query-index-alias", deleteQueryIndexInEveryRun = true)
        val sourceIndex = "test-source-index"
        val existingConcreteIndex = "concrete-query-index-000001"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = existingConcreteIndex
        )

        val testableQueries = TestableDocLevelMonitorQueries(client, clusterService)

        val (response, targetIndex) = runBlocking {
            testableQueries.updateQueryIndexMappings(
                monitor = monitor,
                monitorMetadata = metadata,
                sourceIndex = sourceIndex,
                sourceIndexFieldLimit = 1000L,
                updatedProperties = mutableMapOf(),
                indexTimeout = TimeValue.timeValueSeconds(30)
            )
        }

        assertTrue("Expected update mapping response to be acknowledged", response.isAcknowledged)
        assertEquals("Expected target index to be the existing concrete index", existingConcreteIndex, targetIndex)
        assertEquals(
            "getWriteIndexNameForAlias should NOT be called when concrete index already exists",
            0,
            testableQueries.getWriteIndexNameForAliasCallCount
        )
        assertEquals(
            "Metadata mapping should preserve existing concrete index",
            existingConcreteIndex,
            metadata.sourceToQueryIndexMapping[sourceIndex + monitor.id]
        )
    }

    fun `test updateQueryIndexMappings when deleteQueryIndexInEveryRun true and targetQueryIndex equals alias resolves concrete index`() {
        val aliasName = "query-index-alias"
        val monitor = createMonitor(queryIndexAlias = aliasName, deleteQueryIndexInEveryRun = true)
        val sourceIndex = "test-source-index"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = aliasName
        )

        val concreteResolvedIndex = "concrete-query-index-000002"
        val testableQueries = TestableDocLevelMonitorQueries(
            client,
            clusterService,
            resolvedIndexName = concreteResolvedIndex
        )

        val (response, targetIndex) = runBlocking {
            testableQueries.updateQueryIndexMappings(
                monitor = monitor,
                monitorMetadata = metadata,
                sourceIndex = sourceIndex,
                sourceIndexFieldLimit = 1000L,
                updatedProperties = mutableMapOf(),
                indexTimeout = TimeValue.timeValueSeconds(30)
            )
        }

        assertTrue("Expected update mapping response to be acknowledged", response.isAcknowledged)
        assertEquals("Expected target index to be the newly resolved concrete index", concreteResolvedIndex, targetIndex)
        assertEquals(
            "getWriteIndexNameForAlias should be called once to fix alias-as-target",
            1,
            testableQueries.getWriteIndexNameForAliasCallCount
        )
        assertEquals("Alias queried should match monitor queryIndex alias", aliasName, testableQueries.lastAliasQueried)
        assertEquals(
            "Metadata mapping should be updated to resolved concrete index",
            concreteResolvedIndex,
            metadata.sourceToQueryIndexMapping[sourceIndex + monitor.id]
        )
    }

    fun `test updateQueryIndexMappings when targetQueryIndex is null resolves concrete index`() {
        val aliasName = "query-index-alias"
        val monitor = createMonitor(queryIndexAlias = aliasName, deleteQueryIndexInEveryRun = true)
        val sourceIndex = "test-source-index"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = null
        )

        val concreteResolvedIndex = "concrete-query-index-000003"
        val testableQueries = TestableDocLevelMonitorQueries(
            client,
            clusterService,
            resolvedIndexName = concreteResolvedIndex
        )

        val (response, targetIndex) = runBlocking {
            testableQueries.updateQueryIndexMappings(
                monitor = monitor,
                monitorMetadata = metadata,
                sourceIndex = sourceIndex,
                sourceIndexFieldLimit = 1000L,
                updatedProperties = mutableMapOf(),
                indexTimeout = TimeValue.timeValueSeconds(30)
            )
        }

        assertTrue("Expected update mapping response to be acknowledged", response.isAcknowledged)
        assertEquals("Expected target index to be the newly resolved concrete index", concreteResolvedIndex, targetIndex)
        assertEquals(
            "getWriteIndexNameForAlias should be called once when targetQueryIndex is null",
            1,
            testableQueries.getWriteIndexNameForAliasCallCount
        )
        assertEquals("Alias queried should match monitor queryIndex alias", aliasName, testableQueries.lastAliasQueried)
        assertEquals(
            "Metadata mapping should be set to resolved concrete index",
            concreteResolvedIndex,
            metadata.sourceToQueryIndexMapping[sourceIndex + monitor.id]
        )
    }

    fun `test updateQueryIndexMappings when deleteQueryIndexInEveryRun false retains concrete index without resolution`() {
        val monitor = createMonitor(queryIndexAlias = "query-index-alias", deleteQueryIndexInEveryRun = false)
        val sourceIndex = "test-source-index"
        val existingConcreteIndex = "concrete-query-index-000001"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = existingConcreteIndex
        )

        val testableQueries = TestableDocLevelMonitorQueries(client, clusterService)

        val (response, targetIndex) = runBlocking {
            testableQueries.updateQueryIndexMappings(
                monitor = monitor,
                monitorMetadata = metadata,
                sourceIndex = sourceIndex,
                sourceIndexFieldLimit = 1000L,
                updatedProperties = mutableMapOf(),
                indexTimeout = TimeValue.timeValueSeconds(30)
            )
        }

        assertTrue("Expected update mapping response to be acknowledged", response.isAcknowledged)
        assertEquals("Expected target index to be the existing concrete index", existingConcreteIndex, targetIndex)
        assertEquals(
            "getWriteIndexNameForAlias should NOT be called when deleteQueryIndexInEveryRun is false",
            0,
            testableQueries.getWriteIndexNameForAliasCallCount
        )
        assertEquals(
            "Metadata mapping should preserve existing concrete index",
            existingConcreteIndex,
            metadata.sourceToQueryIndexMapping[sourceIndex + monitor.id]
        )
    }

    fun `test updateQueryIndexMappings when deleteQueryIndexInEveryRun null retains concrete index without resolution`() {
        val monitor = createMonitor(queryIndexAlias = "query-index-alias", deleteQueryIndexInEveryRun = null)
        val sourceIndex = "test-source-index"
        val existingConcreteIndex = "concrete-query-index-000001"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = existingConcreteIndex
        )

        val testableQueries = TestableDocLevelMonitorQueries(client, clusterService)

        val (response, targetIndex) = runBlocking {
            testableQueries.updateQueryIndexMappings(
                monitor = monitor,
                monitorMetadata = metadata,
                sourceIndex = sourceIndex,
                sourceIndexFieldLimit = 1000L,
                updatedProperties = mutableMapOf(),
                indexTimeout = TimeValue.timeValueSeconds(30)
            )
        }

        assertTrue("Expected update mapping response to be acknowledged", response.isAcknowledged)
        assertEquals("Expected target index to be the existing concrete index", existingConcreteIndex, targetIndex)
        assertEquals(
            "getWriteIndexNameForAlias should NOT be called when deleteQueryIndexInEveryRun is null",
            0,
            testableQueries.getWriteIndexNameForAliasCallCount
        )
        assertEquals(
            "Metadata mapping should preserve existing concrete index",
            existingConcreteIndex,
            metadata.sourceToQueryIndexMapping[sourceIndex + monitor.id]
        )
    }

    fun `test updateQueryIndexMappings when deleteQueryIndexInEveryRun false and targetQueryIndex equals alias does not re-resolve`() {
        val aliasName = "query-index-alias"
        val monitor = createMonitor(queryIndexAlias = aliasName, deleteQueryIndexInEveryRun = false)
        val sourceIndex = "test-source-index"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = aliasName
        )

        val testableQueries = TestableDocLevelMonitorQueries(client, clusterService)

        val (response, targetIndex) = runBlocking {
            testableQueries.updateQueryIndexMappings(
                monitor = monitor,
                monitorMetadata = metadata,
                sourceIndex = sourceIndex,
                sourceIndexFieldLimit = 1000L,
                updatedProperties = mutableMapOf(),
                indexTimeout = TimeValue.timeValueSeconds(30)
            )
        }

        assertTrue("Expected update mapping response to be acknowledged", response.isAcknowledged)
        assertEquals("Expected target index to remain unchanged", aliasName, targetIndex)
        assertEquals(
            "getWriteIndexNameForAlias should NOT be called when deleteQueryIndexInEveryRun is false",
            0,
            testableQueries.getWriteIndexNameForAliasCallCount
        )
    }

    fun `test updateQueryIndexMappings throws AlertingException when getWriteIndexNameForAlias returns null`() {
        val aliasName = "query-index-alias"
        val monitor = createMonitor(queryIndexAlias = aliasName, deleteQueryIndexInEveryRun = true)
        val sourceIndex = "test-source-index"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = null
        )

        val testableQueries = TestableDocLevelMonitorQueries(
            client,
            clusterService,
            resolvedIndexName = null
        )

        val exception = assertFailsWith<AlertingException> {
            runBlocking {
                testableQueries.updateQueryIndexMappings(
                    monitor = monitor,
                    monitorMetadata = metadata,
                    sourceIndex = sourceIndex,
                    sourceIndexFieldLimit = 1000L,
                    updatedProperties = mutableMapOf(),
                    indexTimeout = TimeValue.timeValueSeconds(30)
                )
            }
        }

        assertTrue(
            "Exception message should mention failure to get write index for alias",
            exception.message?.contains("Failed to get write index for queryIndex alias:$aliasName") == true
        )
        assertEquals(
            "getWriteIndexNameForAlias should have been attempted once",
            1,
            testableQueries.getWriteIndexNameForAliasCallCount
        )
    }
}
