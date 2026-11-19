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
import org.opensearch.transport.RemoteTransportException
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
        var resolvedIndexName: String? = "concrete-query-index-000001",
        var rolledOverIndexName: String = "concrete-query-index-000002"
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

        // Override rolloverQueryIndex to avoid calling monitorCtx (null in unit tests).
        // Returns a configurable new index name for test isolation.
        override suspend fun rolloverQueryIndex(monitor: Monitor): String {
            return rolledOverIndexName
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
        assertEquals(
            "adjustMaxFieldLimitForQueryIndex should receive the concrete index, not the alias",
            existingConcreteIndex,
            testableQueries.lastPutConcreteQueryIndex
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
        assertEquals(
            "adjustMaxFieldLimitForQueryIndex should receive the resolved concrete index, not the alias",
            concreteResolvedIndex,
            testableQueries.lastPutConcreteQueryIndex
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
        assertEquals(
            "adjustMaxFieldLimitForQueryIndex should receive the resolved concrete index, not the alias",
            concreteResolvedIndex,
            testableQueries.lastPutConcreteQueryIndex
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
        assertEquals(
            "adjustMaxFieldLimitForQueryIndex should receive the concrete index, not the alias",
            existingConcreteIndex,
            testableQueries.lastPutConcreteQueryIndex
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
        assertEquals(
            "adjustMaxFieldLimitForQueryIndex should receive the concrete index, not the alias",
            existingConcreteIndex,
            testableQueries.lastPutConcreteQueryIndex
        )
    }

    fun `test updateQueryIndexMappings when deleteQueryIndexInEveryRun false and targetQueryIndex equals alias does not re-resolve`() {
        // Accepted trade-off: when deleteQueryIndexInEveryRun==false and the alias name is stored in metadata,
        // the stale alias is never corrected in this code path. OpenSearch routes PutMapping against an alias to its
        // write index, so issuing a PutMappingRequest with the alias name is a valid (if non-ideal) operation.
        // The alias-corruption is only fixed when deleteQueryIndexInEveryRun==true triggers re-resolution.
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
        // The alias name is passed through to adjustMaxFieldLimitForQueryIndex in this accepted trade-off path.
        // OpenSearch routes the resulting UpdateSettings call to the alias's write index, so this is valid.
        assertEquals(
            "adjustMaxFieldLimitForQueryIndex receives alias name in the accepted trade-off path",
            aliasName,
            testableQueries.lastPutConcreteQueryIndex
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

    // The null branch fires independently of deleteQueryIndexInEveryRun.
    // This is distinct from tests that have a non-null concrete index stored with flag=true/false.
    fun `test updateQueryIndexMappings when targetQueryIndex is null and deleteQueryIndexInEveryRun false still resolves concrete index`() {
        val aliasName = "query-index-alias"
        val monitor = createMonitor(queryIndexAlias = aliasName, deleteQueryIndexInEveryRun = false)
        val sourceIndex = "test-source-index"
        // No entry in metadata — targetQueryIndex will be null on first run regardless of deleteQueryIndexInEveryRun
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = null
        )

        val concreteResolvedIndex = "concrete-query-index-000001"
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
        assertEquals("Expected target index to be the resolved concrete index", concreteResolvedIndex, targetIndex)
        assertEquals(
            "getWriteIndexNameForAlias should be called once when targetQueryIndex is null, regardless of deleteQueryIndexInEveryRun",
            1,
            testableQueries.getWriteIndexNameForAliasCallCount
        )
        assertEquals("Alias queried should match monitor queryIndex alias", aliasName, testableQueries.lastAliasQueried)
        assertEquals(
            "Metadata mapping should be set to resolved concrete index",
            concreteResolvedIndex,
            metadata.sourceToQueryIndexMapping[sourceIndex + monitor.id]
        )
        assertEquals(
            "adjustMaxFieldLimitForQueryIndex should receive the resolved concrete index, not the alias",
            concreteResolvedIndex,
            testableQueries.lastPutConcreteQueryIndex
        )
    }

    // getWriteIndexNameForAlias must return null when the alias exists but no write index is designated.
    fun `test getWriteIndexNameForAlias returns null when alias exists but has no write index designated`() {
        val aliasName = "test-query-alias"
        val concreteIndex = "concrete-index-000001"

        // writeIndex(false) means this backing index is explicitly NOT the write index
        val indexMetadata = IndexMetadata.builder(concreteIndex)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder(aliasName).writeIndex(false).build())
            .build()
        val metadata = Metadata.builder().put(indexMetadata, false).build()
        val clusterState = ClusterState.builder(ClusterName("test-cluster")).metadata(metadata).build()
        whenever(clusterService.state()).thenReturn(clusterState)

        val docLevelMonitorQueries = DocLevelMonitorQueries(client, clusterService)
        val writeIndex = docLevelMonitorQueries.getWriteIndexNameForAlias(aliasName)

        assertNull(
            "Expected null when alias has no write index designated (writeIndex=false on all backing indices)",
            writeIndex
        )
    }

    // putMapping throws field-limit exception, rollover is triggered, returned targetIndex is the new rolled-over name.
    fun `test updateQueryIndexMappings when putMapping throws field-limit exception triggers rollover and returns new index`() {
        val aliasName = "query-index-alias"
        val monitor = createMonitor(queryIndexAlias = aliasName, deleteQueryIndexInEveryRun = true)
        val sourceIndex = "test-source-index"
        val initialConcreteIndex = "concrete-query-index-000001"
        val rolledOverIndex = "concrete-query-index-000002"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = initialConcreteIndex
        )

        val testableQueries = TestableDocLevelMonitorQueries(
            client,
            clusterService,
            resolvedIndexName = initialConcreteIndex,
            rolledOverIndexName = rolledOverIndex
        )

        // First putMapping call triggers field-limit failure; second (post-rollover) call succeeds.
        var putMappingCallCount = 0
        Mockito.doAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<AcknowledgedResponse>
            putMappingCallCount++
            if (putMappingCallCount == 1) {
                // Wrap in RemoteTransportException to simulate what OpenSearch sends back over the wire
                listener.onFailure(RemoteTransportException("field-limit-error", RuntimeException("Limit of total fields [1000] has been exceeded")))
            } else {
                listener.onResponse(AcknowledgedResponse(true))
            }
            null
        }.`when`(indicesAdminClient).putMapping(any(PutMappingRequest::class.java), any())

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

        assertTrue("Expected update mapping response to be acknowledged after rollover", response.isAcknowledged)
        assertEquals("Expected target index to be the new rolled-over index after rollover", rolledOverIndex, targetIndex)
        assertEquals(
            "Metadata should be updated to the new rolled-over index",
            rolledOverIndex,
            metadata.sourceToQueryIndexMapping[sourceIndex + monitor.id]
        )
        assertEquals(
            "adjustMaxFieldLimitForQueryIndex should have been called last with the rolled-over index",
            rolledOverIndex,
            testableQueries.lastPutConcreteQueryIndex
        )
    }

    // putMapping throws a non-field-limit exception with deleteQueryIndexInEveryRun=false — expect AlertingException.
    fun `test updateQueryIndexMappings when putMapping throws non-field-limit exception and deleteQueryIndexInEveryRun false throws AlertingException`() {
        val aliasName = "query-index-alias"
        val monitor = createMonitor(queryIndexAlias = aliasName, deleteQueryIndexInEveryRun = false)
        val sourceIndex = "test-source-index"
        val initialConcreteIndex = "concrete-query-index-000001"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = initialConcreteIndex
        )

        val testableQueries = TestableDocLevelMonitorQueries(
            client,
            clusterService,
            resolvedIndexName = initialConcreteIndex
        )

        // putMapping throws a non-field-limit exception
        Mockito.doAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<AcknowledgedResponse>
            listener.onFailure(RemoteTransportException("generic-error", RuntimeException("Some unexpected mapping error")))
            null
        }.`when`(indicesAdminClient).putMapping(any(PutMappingRequest::class.java), any())

        assertFailsWith<AlertingException>(
            message = "Expected AlertingException when putMapping fails with a non-field-limit error and deleteQueryIndexInEveryRun is false"
        ) {
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
    }
}
