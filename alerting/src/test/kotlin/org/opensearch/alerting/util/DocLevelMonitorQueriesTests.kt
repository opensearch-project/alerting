/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import kotlinx.coroutines.runBlocking
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyBoolean
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.opensearch.Version
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.parser
import org.opensearch.alerting.randomDocumentLevelMonitor
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.AliasMetadata
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.routing.RoutingTable
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.MonitorMetadata
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient
import java.util.concurrent.atomic.AtomicInteger

/**
 * Unit tests for [DocLevelMonitorQueries].
 *
 * Covers:
 *  - [DocLevelMonitorQueries.deepCopyMap] — deep isolation of nested maps and lists of maps.
 *  - [DocLevelMonitorQueries.findConflictingFields] — cross-index type conflict detection,
 *    alias target path divergence detection across rollover boundaries, and transitive
 *    propagation of target field conflicts to alias fields.
 *  - [DocLevelMonitorQueries.buildLeafNodeProcessor] — per-index vs shared suffixing for
 *    both regular fields and alias fields + their `path` targets, including nested aliases.
 *  - [DocLevelMonitorQueries.indexDocLevelQueries] — bounded single-retry guard when
 *    `deleteQueryIndexInEveryRun` is enabled and `putMapping` persistently fails.
 */
class DocLevelMonitorQueriesTests : OpenSearchTestCase() {

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private fun makeInstance(): DocLevelMonitorQueries =
        DocLevelMonitorQueries(mock(Client::class.java), mock(ClusterService::class.java))

    // -------------------------------------------------------------------------
    // deepCopyMap — structural correctness
    // -------------------------------------------------------------------------

    fun `test deepCopyMap returns an equal but distinct map reference`() {
        val original = mutableMapOf<String, Any>("key" to "value")

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        assertEquals(original, copy)
        assertNotSame(original, copy)
    }

    fun `test deepCopyMap deep copies nested maps so mutations are isolated`() {
        val nested = mutableMapOf<String, Any>("inner" to "original")
        val original = mutableMapOf<String, Any>("outer" to nested)

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        // Mutate the nested map in the copy.
        @Suppress("UNCHECKED_CAST")
        (copy["outer"] as MutableMap<String, Any>)["inner"] = "mutated"

        // Original nested map must be unaffected.
        assertEquals("original", nested["inner"])
    }

    fun `test deepCopyMap copies nested map references independently`() {
        val nested = mutableMapOf<String, Any>("inner" to "value")
        val original = mutableMapOf<String, Any>("outer" to nested)

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        assertNotSame(nested, copy["outer"])
    }

    fun `test deepCopyMap copies list of scalars as a new mutable list`() {
        val list = mutableListOf("a", "b", "c")
        val original = mutableMapOf<String, Any>("items" to list)

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        @Suppress("UNCHECKED_CAST")
        val copiedList = copy["items"] as MutableList<String>
        copiedList.add("d")

        // The list in the copy is a distinct mutable list; original list unchanged.
        assertEquals(3, list.size)
        assertEquals(4, copiedList.size)
        assertNotSame(list, copiedList)
    }

    fun `test deepCopyMap preserves scalar values`() {
        val original = mutableMapOf<String, Any>(
            "strVal" to "hello",
            "intVal" to 42,
            "boolVal" to true,
            "longVal" to 9_999_999_999L
        )

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        assertEquals("hello", copy["strVal"])
        assertEquals(42, copy["intVal"])
        assertEquals(true, copy["boolVal"])
        assertEquals(9_999_999_999L, copy["longVal"])
    }

    fun `test deepCopyMap handles empty map`() {
        val copy = DocLevelMonitorQueries.deepCopyMap(emptyMap())

        assertTrue(copy.isEmpty())
    }

    fun `test deepCopyMap handles multiply-nested structure`() {
        val original = mutableMapOf<String, Any>(
            "level1" to mutableMapOf<String, Any>(
                "level2" to mutableMapOf<String, Any>(
                    "level3" to "leaf"
                )
            )
        )

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        @Suppress("UNCHECKED_CAST")
        val l1 = copy["level1"] as MutableMap<String, Any>
        @Suppress("UNCHECKED_CAST")
        val l2 = l1["level2"] as MutableMap<String, Any>
        assertEquals("leaf", l2["level3"])

        // Mutate deep copy — original must be unaffected.
        l2["level3"] = "changed"
        @Suppress("UNCHECKED_CAST")
        val origL2 = (original["level1"] as Map<String, Any>)["level2"] as Map<String, Any>
        assertEquals("leaf", origL2["level3"])
    }

    fun `test deepCopyMap deep copies map elements inside lists`() {
        val innerMap = mutableMapOf<String, Any>("nested_key" to "original_value")
        val original = mutableMapOf<String, Any>(
            "items" to mutableListOf<Any>(innerMap, "plain_string")
        )

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        @Suppress("UNCHECKED_CAST")
        val copiedList = copy["items"] as MutableList<Any>
        @Suppress("UNCHECKED_CAST")
        val copiedInnerMap = copiedList[0] as MutableMap<String, Any>

        // Mutate the map element inside the copied list.
        copiedInnerMap["nested_key"] = "mutated_value"

        // The original inner map must be unaffected — deep copy isolated it.
        assertEquals(
            "Map element inside list must be deep-copied so mutations do not affect the original",
            "original_value",
            innerMap["nested_key"]
        )
        assertNotSame(innerMap, copiedInnerMap)
    }

    // -------------------------------------------------------------------------
    // findConflictingFields — real conflict detection and alias propagation
    // -------------------------------------------------------------------------

    fun `test findConflictingFields detects text-to-keyword type change as conflict`() {
        val queries = makeInstance()
        val indexA = mapOf<String, Any>(
            "host" to mapOf<String, Any>(
                "properties" to mapOf<String, Any>(
                    "name" to mapOf<String, Any>("type" to "text")
                )
            ),
            "event_id" to mapOf<String, Any>("type" to "keyword")
        )
        val indexB = mapOf<String, Any>(
            "host" to mapOf<String, Any>(
                "properties" to mapOf<String, Any>(
                    "name" to mapOf<String, Any>("type" to "keyword")
                )
            ),
            "event_id" to mapOf<String, Any>("type" to "keyword")
        )

        val conflictingFields = queries.findConflictingFields(listOf(indexA, indexB))

        assertTrue(
            "host.name must be flagged as conflicting because its type changed from text to keyword",
            conflictingFields.contains("host.name")
        )
        assertFalse(
            "event_id must not be flagged as conflicting because it is identical in both indices",
            conflictingFields.contains("event_id")
        )
    }

    fun `test findConflictingFields detects alias path change across rollover as conflict`() {
        val queries = makeInstance()
        val indexA = mapOf<String, Any>(
            "ts" to mapOf<String, Any>("type" to "date"),
            "timestamp" to mapOf<String, Any>("type" to "alias", "path" to "ts")
        )
        val indexB = mapOf<String, Any>(
            "@timestamp" to mapOf<String, Any>("type" to "date"),
            "timestamp" to mapOf<String, Any>("type" to "alias", "path" to "@timestamp")
        )

        val conflictingFields = queries.findConflictingFields(listOf(indexA, indexB))

        assertTrue(
            "timestamp alias with differing path targets across rollover must be flagged as conflicting",
            conflictingFields.contains("timestamp")
        )
        assertFalse("ts target field must not be flagged as conflicting", conflictingFields.contains("ts"))
        assertFalse("@timestamp target field must not be flagged as conflicting", conflictingFields.contains("@timestamp"))
    }

    fun `test findConflictingFields propagates target field conflict to alias field`() {
        val queries = makeInstance()
        val indexA = mapOf<String, Any>(
            "resource" to mapOf<String, Any>(
                "properties" to mapOf<String, Any>(
                    "host" to mapOf<String, Any>(
                        "properties" to mapOf<String, Any>(
                            "name" to mapOf<String, Any>("type" to "text")
                        )
                    )
                )
            ),
            "host" to mapOf<String, Any>(
                "properties" to mapOf<String, Any>(
                    "name" to mapOf<String, Any>("type" to "alias", "path" to "resource.host.name")
                )
            )
        )
        val indexB = mapOf<String, Any>(
            "resource" to mapOf<String, Any>(
                "properties" to mapOf<String, Any>(
                    "host" to mapOf<String, Any>(
                        "properties" to mapOf<String, Any>(
                            "name" to mapOf<String, Any>("type" to "keyword")
                        )
                    )
                )
            ),
            "host" to mapOf<String, Any>(
                "properties" to mapOf<String, Any>(
                    "name" to mapOf<String, Any>("type" to "alias", "path" to "resource.host.name")
                )
            )
        )

        val conflictingFields = queries.findConflictingFields(listOf(indexA, indexB))

        assertTrue(
            "resource.host.name target field must be flagged as conflicting",
            conflictingFields.contains("resource.host.name")
        )
        assertTrue(
            "host.name alias must inherit conflict from its conflicting target resource.host.name",
            conflictingFields.contains("host.name")
        )
    }

    fun `test findConflictingFields does not flag identical alias or concrete fields`() {
        val queries = makeInstance()
        val indexA = mapOf<String, Any>(
            "message" to mapOf<String, Any>("type" to "text"),
            "event" to mapOf<String, Any>(
                "properties" to mapOf<String, Any>(
                    "start" to mapOf<String, Any>("type" to "date")
                )
            ),
            "timestamp" to mapOf<String, Any>("type" to "alias", "path" to "event.start")
        )
        val indexB = mapOf<String, Any>(
            "message" to mapOf<String, Any>("type" to "text"),
            "event" to mapOf<String, Any>(
                "properties" to mapOf<String, Any>(
                    "start" to mapOf<String, Any>("type" to "date")
                )
            ),
            "timestamp" to mapOf<String, Any>("type" to "alias", "path" to "event.start")
        )

        val conflictingFields = queries.findConflictingFields(listOf(indexA, indexB))

        assertTrue("Identical concrete and alias mappings must produce no conflicts", conflictingFields.isEmpty())
    }

    // -------------------------------------------------------------------------
    // buildLeafNodeProcessor — alias and conflict suffixing via real traversal
    // -------------------------------------------------------------------------

    fun `test buildLeafNodeProcessor suffixes alias path with updatedIndexName when target is not conflicting`() {
        val queries = makeInstance()
        val properties = mutableMapOf<String, Any>(
            "@timestamp" to mutableMapOf<String, Any>("type" to "date"),
            "timestamp" to mutableMapOf<String, Any>("type" to "alias", "path" to "@timestamp")
        )
        val processor = queries.buildLeafNodeProcessor(
            monitorId = "mon1",
            concreteIndexName = "logs-000002",
            updatedIndexName = "logs",
            conflictingFields = setOf("timestamp")
        )
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        queries.traverseMappingsAndUpdate(properties, "", processor, flattenPaths)

        assertFalse("Original alias key must be renamed", properties.containsKey("timestamp"))
        assertTrue("Conflicting alias must be suffixed with concreteIndexName", properties.containsKey("timestamp_logs-000002_mon1"))
        val aliasProps = properties["timestamp_logs-000002_mon1"] as Map<*, *>
        assertEquals("alias", aliasProps["type"])
        assertEquals("@timestamp_logs_mon1", aliasProps["path"])
        assertTrue("Non-conflicting target must be suffixed with updatedIndexName", properties.containsKey("@timestamp_logs_mon1"))
    }

    fun `test buildLeafNodeProcessor suffixes both alias and path with concreteIndexName when target is conflicting`() {
        val queries = makeInstance()
        val properties = mutableMapOf<String, Any>(
            "resource" to mutableMapOf<String, Any>(
                "properties" to mutableMapOf<String, Any>(
                    "host" to mutableMapOf<String, Any>(
                        "properties" to mutableMapOf<String, Any>(
                            "name" to mutableMapOf<String, Any>("type" to "keyword")
                        )
                    )
                )
            ),
            "host" to mutableMapOf<String, Any>(
                "properties" to mutableMapOf<String, Any>(
                    "name" to mutableMapOf<String, Any>("type" to "alias", "path" to "resource.host.name")
                )
            )
        )
        val processor = queries.buildLeafNodeProcessor(
            monitorId = "mon1",
            concreteIndexName = "logs-000002",
            updatedIndexName = "logs",
            conflictingFields = setOf("resource.host.name", "host.name")
        )
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        queries.traverseMappingsAndUpdate(properties, "", processor, flattenPaths)

        val hostProps = (properties["host"] as Map<*, *>)["properties"] as Map<*, *>
        assertFalse("Original nested alias leaf must be renamed", hostProps.containsKey("name"))
        assertTrue("Conflicting alias leaf must use concreteIndexName suffix", hostProps.containsKey("name_logs-000002_mon1"))
        val aliasProps = hostProps["name_logs-000002_mon1"] as Map<*, *>
        assertEquals("alias", aliasProps["type"])
        assertEquals("resource.host.name_logs-000002_mon1", aliasProps["path"])

        val resourceProps = (properties["resource"] as Map<*, *>)["properties"] as Map<*, *>
        val resourceHostProps = (resourceProps["host"] as Map<*, *>)["properties"] as Map<*, *>
        assertTrue(
            "Conflicting target leaf must use concreteIndexName suffix",
            resourceHostProps.containsKey("name_logs-000002_mon1")
        )
    }

    fun `test buildLeafNodeProcessor preserves nested alias fields with valid suffixed path`() {
        val queries = makeInstance()
        val properties = mutableMapOf<String, Any>(
            "alias" to mutableMapOf<String, Any>(
                "properties" to mutableMapOf<String, Any>(
                    "some" to mutableMapOf<String, Any>(
                        "properties" to mutableMapOf<String, Any>(
                            "fff" to mutableMapOf<String, Any>("type" to "alias", "path" to "test_field.some_other_field")
                        )
                    )
                )
            ),
            "test_field" to mutableMapOf<String, Any>(
                "properties" to mutableMapOf<String, Any>(
                    "some_other_field" to mutableMapOf<String, Any>("type" to "keyword")
                )
            )
        )
        val processor = queries.buildLeafNodeProcessor(
            monitorId = "mon1",
            concreteIndexName = "logs-000001",
            updatedIndexName = "logs",
            conflictingFields = emptySet()
        )
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        queries.traverseMappingsAndUpdate(properties, "", processor, flattenPaths)

        assertTrue(flattenPaths.containsKey("alias.some.fff"))
        assertTrue(flattenPaths.containsKey("test_field.some_other_field"))

        val aliasPropsParent = (properties["alias"] as Map<*, *>)["properties"] as Map<*, *>
        val someProps = (aliasPropsParent["some"] as Map<*, *>)["properties"] as Map<*, *>
        assertTrue("Nested alias leaf must be renamed with monitor suffix", someProps.containsKey("fff_logs_mon1"))
        val aliasProps = someProps["fff_logs_mon1"] as Map<*, *>
        assertFalse("Nested alias mapping must not be empty", aliasProps.isEmpty())
        assertEquals("alias", aliasProps["type"])
        assertEquals("test_field.some_other_field_logs_mon1", aliasProps["path"])

        val testFieldProps = (properties["test_field"] as Map<*, *>)["properties"] as Map<*, *>
        assertTrue("Target field must exist with matching suffixed name", testFieldProps.containsKey("some_other_field_logs_mon1"))
    }

    // -------------------------------------------------------------------------
    // indexDocLevelQueries / updateQueryIndexMappings — bounded retry guard
    // -------------------------------------------------------------------------

    fun `test indexDocLevelQueries bounds delete-and-recreate retry to once on persistent putMapping failure`() {
        val queryIndexAlias = "custom-query-index"
        val sourceIndexContent = "{\"logs-000001\":{\"settings\":{\"index\":{\"creation_date\":\"1558407515699\"," +
            "\"number_of_shards\":\"1\",\"number_of_replicas\":\"0\",\"uuid\":\"t-VBBW6aR6KpJ3XP5iISOA\"," +
            "\"version\":{\"created\":\"${Version.CURRENT.id}\"},\"provided_name\":\"logs-000001\"}}," +
            "\"mapping_version\":1,\"settings_version\":1,\"aliases_version\":1," +
            "\"mappings\":{\"_doc\":{\"properties\":{\"message\":{\"type\":\"text\"}}}}}}"
        val sourceIndexMetadata = IndexMetadata.fromXContent(parser(sourceIndexContent))

        fun buildQueryIndexMetadata(indexName: String): IndexMetadata = IndexMetadata.builder(indexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder(queryIndexAlias).writeIndex(true))
            .build()

        val routingTable = mock(RoutingTable::class.java)
        `when`(routingTable.hasIndex("logs-000001")).thenReturn(true)

        fun stateWithQueryIndex(queryIndexName: String?): ClusterState {
            val metadataBuilder = Metadata.builder().put(sourceIndexMetadata, false)
            if (queryIndexName != null) {
                metadataBuilder.put(buildQueryIndexMetadata(queryIndexName), false)
            }
            return ClusterState.builder(ClusterName.DEFAULT)
                .metadata(metadataBuilder.build())
                .routingTable(routingTable)
                .build()
        }

        var currentState = stateWithQueryIndex("$queryIndexAlias-000001")
        val clusterService = mock(ClusterService::class.java)
        `when`(clusterService.state()).thenAnswer { currentState }

        val resolver = mock(IndexNameExpressionResolver::class.java)
        `when`(resolver.concreteIndexNames(any(), any(), anyBoolean(), anyString()))
            .thenReturn(arrayOf("logs-000001"))

        val indicesAdmin = mock(IndicesAdminClient::class.java)
        val adminClient = mock(AdminClient::class.java)
        `when`(adminClient.indices()).thenReturn(indicesAdmin)
        val client = mock(Client::class.java)
        `when`(client.admin()).thenReturn(adminClient)

        val getSettingsResponse = mock(GetSettingsResponse::class.java)
        doAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<GetSettingsResponse>
            listener.onResponse(getSettingsResponse)
            null
        }.`when`(indicesAdmin).getSettings(any(GetSettingsRequest::class.java), any())

        doAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<AcknowledgedResponse>
            listener.onResponse(AcknowledgedResponse(true))
            null
        }.`when`(indicesAdmin).updateSettings(any(UpdateSettingsRequest::class.java), any())

        val putMappingCalls = AtomicInteger(0)
        doAnswer { invocation ->
            putMappingCalls.incrementAndGet()
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<AcknowledgedResponse>
            listener.onFailure(IllegalArgumentException("persistent mapper conflict"))
            null
        }.`when`(indicesAdmin).putMapping(any(PutMappingRequest::class.java), any())

        val deleteCalls = AtomicInteger(0)
        doAnswer { invocation ->
            deleteCalls.incrementAndGet()
            currentState = stateWithQueryIndex(null)
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<AcknowledgedResponse>
            listener.onResponse(AcknowledgedResponse(true))
            null
        }.`when`(indicesAdmin).delete(any(DeleteIndexRequest::class.java), any())

        val createCalls = AtomicInteger(0)
        val createIndexResponse = mock(CreateIndexResponse::class.java)
        `when`(createIndexResponse.isAcknowledged).thenReturn(true)
        doAnswer { invocation ->
            createCalls.incrementAndGet()
            currentState = stateWithQueryIndex("$queryIndexAlias-000002")
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<CreateIndexResponse>
            listener.onResponse(createIndexResponse)
            null
        }.`when`(indicesAdmin).create(any(CreateIndexRequest::class.java), any())

        val queries = DocLevelMonitorQueries(client, clusterService)
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(
                DocLevelMonitorInput(
                    description = "desc",
                    indices = listOf("logs*"),
                    queries = listOf(
                        DocLevelQuery(
                            id = "q1",
                            query = "message:error",
                            name = "q1",
                            fields = emptyList(),
                            tags = emptyList()
                        )
                    )
                )
            ),
            triggers = emptyList(),
            dataSources = DataSources(queryIndex = queryIndexAlias)
        ).copy(id = "mon1", deleteQueryIndexInEveryRun = true)
        val monitorMetadata = MonitorMetadata(
            id = "mon1-metadata",
            seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            monitorId = monitor.id,
            lastActionExecutionTimes = emptyList(),
            lastRunContext = emptyMap(),
            sourceToQueryIndexMapping = mutableMapOf()
        )

        val originalMonitorCtx = MonitorRunnerService.monitorCtx
        try {
            MonitorRunnerService.monitorCtx = originalMonitorCtx.copy(
                clusterService = clusterService,
                indexNameExpressionResolver = resolver,
                docLevelMonitorQueries = queries
            )

            val thrown = expectThrows(AlertingException::class.java) {
                runBlocking {
                    queries.indexDocLevelQueries(
                        monitor = monitor,
                        monitorId = monitor.id,
                        monitorMetadata = monitorMetadata,
                        indexTimeout = TimeValue.timeValueSeconds(30)
                    )
                }
            }
            assertTrue(
                "Exception message should propagate underlying putMapping error",
                thrown.message?.contains("persistent mapper conflict") == true
            )
            assertEquals("putMapping should be attempted exactly twice (initial + 1 retry)", 2, putMappingCalls.get())
            assertEquals("Query index should be deleted at most once", 1, deleteCalls.get())
            assertEquals("Query index should be recreated at most once", 1, createCalls.get())
        } finally {
            MonitorRunnerService.monitorCtx = originalMonitorCtx
        }
    }
}
