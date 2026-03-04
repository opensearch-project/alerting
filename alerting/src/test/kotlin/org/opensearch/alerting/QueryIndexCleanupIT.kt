/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery

class QueryIndexCleanupIT : AlertingRestTestCase() {

    fun `test query index cleanup settings exist`() {
        val settings = """
            {
                "persistent": {
                    "${AlertingSettings.QUERY_INDEX_CLEANUP_ENABLED.key}": true,
                    "${AlertingSettings.QUERY_INDEX_CLEANUP_PERIOD.key}": "1d"
                }
            }
        """.trimIndent()

        val response = client().makeRequest(
            "PUT",
            "_cluster/settings",
            emptyMap(),
            StringEntity(settings, ContentType.APPLICATION_JSON)
        )

        assertEquals("Setting update should succeed", 200, response.restStatus().status)
    }

    fun `test query index created for doc level monitor`() {
        val testIndex = createTestIndex()

        val docQuery = DocLevelQuery(query = "test_field:\"test\"", name = "test-query", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val createdMonitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))

        executeMonitor(createdMonitor.id)

        val retrievedMonitor = getMonitor(monitorId = createdMonitor.id)
        val queryIndexName = (retrievedMonitor.dataSources as DataSources).queryIndex

        assertTrue("Query index should exist after monitor execution", indexExists(queryIndexName))
    }

    fun `test cleanup disabled by setting`() {
        val disableSettings = """
            {
                "persistent": {
                    "${AlertingSettings.QUERY_INDEX_CLEANUP_ENABLED.key}": false
                }
            }
        """.trimIndent()

        val response = client().makeRequest(
            "PUT",
            "_cluster/settings",
            emptyMap(),
            StringEntity(disableSettings, ContentType.APPLICATION_JSON)
        )

        assertEquals("Disable setting should succeed", 200, response.restStatus().status)

        val enableSettings = """
            {
                "persistent": {
                    "${AlertingSettings.QUERY_INDEX_CLEANUP_ENABLED.key}": true
                }
            }
        """.trimIndent()

        client().makeRequest(
            "PUT",
            "_cluster/settings",
            emptyMap(),
            StringEntity(enableSettings, ContentType.APPLICATION_JSON)
        )
    }

    fun `test write index is never deleted`() {
        val testIndex = createTestIndex()

        val docQuery = DocLevelQuery(query = "test_field:\"test\"", name = "test-query", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))

        executeMonitor(monitor.id)

        val retrievedMonitor = getMonitor(monitorId = monitor.id)
        val queryIndexName = (retrievedMonitor.dataSources as DataSources).queryIndex

        assertTrue("Query index should exist", indexExists(queryIndexName))
    }

    fun `test query index retained with active source indices`() {
        val testIndex = createTestIndex()

        val docQuery = DocLevelQuery(query = "test_field:\"test\"", name = "test-query", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))

        executeMonitor(monitor.id)

        val retrievedMonitor = getMonitor(monitorId = monitor.id)
        val queryIndexName = (retrievedMonitor.dataSources as DataSources).queryIndex

        assertTrue("Query index should exist", indexExists(queryIndexName))

        setCleanupPeriod("1s")
        Thread.sleep(5000)

        assertTrue("Query index should be retained with active source", indexExists(queryIndexName))
        assertTrue("Source index should still exist", indexExists(testIndex))
    }

    fun `test cleanup period can be configured`() {
        val settings = """
            {
                "persistent": {
                    "${AlertingSettings.QUERY_INDEX_CLEANUP_PERIOD.key}": "5m"
                }
            }
        """.trimIndent()

        val response = client().makeRequest(
            "PUT",
            "_cluster/settings",
            emptyMap(),
            StringEntity(settings, ContentType.APPLICATION_JSON)
        )

        assertEquals("Period setting should succeed", 200, response.restStatus().status)

        val getResponse = client().makeRequest("GET", "_cluster/settings")
        val responseBody = getResponse.entity.content.readBytes().toString(Charsets.UTF_8)
        assertTrue("Period should be set", responseBody.contains("5m"))
    }

    fun `test cleanup deletes non-write backing indices when source deleted`() {
        val testIndex = createTestIndex()

        val docQuery = DocLevelQuery(query = "test_field:\"test\"", name = "test-query", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))

        executeMonitor(monitor.id)

        val retrievedMonitor = getMonitor(monitorId = monitor.id)
        val queryIndexAlias = (retrievedMonitor.dataSources as DataSources).queryIndex

        assertTrue("Query index should exist before cleanup", indexExists(queryIndexAlias))

        val firstBackingIndex = getFirstBackingIndex(queryIndexAlias)
        val nextIndexName = createNextBackingIndex(queryIndexAlias, firstBackingIndex)

        // Verify we now have 2 backing indices
        val aliasResponse2 = client().makeRequest("GET", "/_cat/aliases/$queryIndexAlias?format=json")
        val aliasBody2 = aliasResponse2.entity.content.readBytes().toString(Charsets.UTF_8)
        assertTrue("Should have 2 backing indices", aliasBody2.contains(firstBackingIndex) && aliasBody2.contains(nextIndexName))

        // Verify metadata has mapping before deletion
        val mappingBefore = getSourceToQueryIndexMapping(monitor.id)
        assertTrue("Metadata should have mapping entries before cleanup", mappingBefore.isNotEmpty())

        // Delete source index
        deleteIndex(testIndex)

        setCleanupPeriod("1s")
        Thread.sleep(10000)

        // Verify old backing index is deleted but write index remains
        assertFalse("Old backing index should be deleted", indexExists(firstBackingIndex))
        assertTrue("Write index should be retained", indexExists(nextIndexName))
        assertTrue("Alias should still exist", indexExists(queryIndexAlias))

        // Verify metadata mapping was cleaned up for deleted source index
        val mappingAfter = getSourceToQueryIndexMapping(monitor.id)
        for ((sourceKey, _) in mappingBefore) {
            val sourceIndex = sourceKey.removeSuffix(monitor.id)
            if (!indexExists(sourceIndex)) {
                assertFalse(
                    "Mapping for deleted source $sourceIndex should be removed",
                    mappingAfter.containsKey(sourceKey)
                )
            }
        }
    }

    fun `test cleanup retains indices when only one backing index exists`() {
        val testIndex = createTestIndex()

        val docQuery = DocLevelQuery(query = "test_field:\"test\"", name = "test-query", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))

        executeMonitor(monitor.id)

        val retrievedMonitor = getMonitor(monitorId = monitor.id)
        val queryIndexAlias = (retrievedMonitor.dataSources as DataSources).queryIndex

        assertTrue("Query index should exist before cleanup", indexExists(queryIndexAlias))

        deleteIndex(testIndex)

        setCleanupPeriod("1s")
        Thread.sleep(10000)

        assertTrue("Single backing index should be retained", indexExists(queryIndexAlias))
    }

    fun `test cleanup removes dead source entries but retains alive ones in metadata`() {
        // Create two concrete source indices (simulating timeseries rollover)
        val sourceIndex1 = createTestIndex(index = "log-test-000001")
        val sourceIndex2 = createTestIndex(index = "log-test-000002")

        // Create monitor targeting both concrete indices
        val docQuery = DocLevelQuery(query = "test_field:\"test\"", name = "test-query", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(sourceIndex1, sourceIndex2), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))

        executeMonitor(monitor.id)

        // Verify metadata has entries for both source indices
        val mappingBefore = getSourceToQueryIndexMapping(monitor.id)
        assertEquals("Should have 2 mapping entries", 2, mappingBefore.size)

        val key1 = "$sourceIndex1${monitor.id}"
        val key2 = "$sourceIndex2${monitor.id}"
        assertTrue("Should have entry for source index 1", mappingBefore.containsKey(key1))
        assertTrue("Should have entry for source index 2", mappingBefore.containsKey(key2))

        // Delete only source index 1, keep source index 2 alive
        deleteIndex(sourceIndex1)

        setCleanupPeriod("1s")
        Thread.sleep(10000)

        // Verify metadata: dead source entry removed, alive source entry retained
        val mappingAfter = getSourceToQueryIndexMapping(monitor.id)
        assertFalse(
            "Mapping for deleted source $sourceIndex1 should be removed",
            mappingAfter.containsKey(key1)
        )
        assertTrue(
            "Mapping for alive source $sourceIndex2 should be retained",
            mappingAfter.containsKey(key2)
        )

        // Source index 2 still exists
        assertTrue("Source index 2 should still exist", indexExists(sourceIndex2))
    }

    // --- Helper methods ---

    @Suppress("UNCHECKED_CAST")
    private fun getSourceToQueryIndexMapping(monitorId: String): Map<String, String> {
        val metadataId = "$monitorId-metadata"
        val searchRequest = """
            {
                "query": {
                    "term": {
                        "_id": "$metadataId"
                    }
                }
            }
        """.trimIndent()

        val response = client().makeRequest(
            "GET",
            "/.opendistro-alerting-config/_search",
            emptyMap(),
            StringEntity(searchRequest, ContentType.APPLICATION_JSON)
        )

        val responseBody = response.entity.content.readBytes().toString(Charsets.UTF_8)
        val parser = org.opensearch.common.xcontent.json.JsonXContent.jsonXContent
            .createParser(
                org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                org.opensearch.common.xcontent.LoggingDeprecationHandler.INSTANCE,
                responseBody
            )
        val responseMap = parser.map()
        val hits = (responseMap["hits"] as Map<String, Any>)["hits"] as List<Map<String, Any>>
        if (hits.isEmpty()) return emptyMap()

        val source = hits[0]["_source"] as Map<String, Any>
        val metadata = source["metadata"] as Map<String, Any>
        return metadata.getOrDefault("source_to_query_index_mapping", emptyMap<String, String>()) as Map<String, String>
    }

    private fun getFirstBackingIndex(queryIndexAlias: String): String {
        val aliasResponse = client().makeRequest("GET", "/_cat/aliases/$queryIndexAlias?format=json")
        val aliasBody = aliasResponse.entity.content.readBytes().toString(Charsets.UTF_8)
        return aliasBody.substringAfter("\"index\":\"").substringBefore("\"")
    }

    private fun createNextBackingIndex(queryIndexAlias: String, firstBackingIndex: String): String {
        val nextIndexNumber = firstBackingIndex.substringAfterLast("-").toInt() + 1
        val nextIndexName = "$queryIndexAlias-" + String.format(java.util.Locale.ROOT, "%06d", nextIndexNumber)

        client().makeRequest(
            "PUT",
            "/$nextIndexName",
            emptyMap(),
            StringEntity(
                """{"aliases": {"$queryIndexAlias": {"is_write_index": true}}}""",
                ContentType.APPLICATION_JSON
            )
        )

        client().makeRequest(
            "POST",
            "/_aliases",
            emptyMap(),
            StringEntity(
                """{"actions": [{"add": {"index": "$firstBackingIndex", "alias": "$queryIndexAlias", "is_write_index": false}}]}""",
                ContentType.APPLICATION_JSON
            )
        )

        return nextIndexName
    }

    private fun setCleanupPeriod(period: String) {
        client().makeRequest(
            "PUT",
            "_cluster/settings",
            emptyMap(),
            StringEntity(
                """{"persistent": {"${AlertingSettings.QUERY_INDEX_CLEANUP_PERIOD.key}": "$period"}}""",
                ContentType.APPLICATION_JSON
            )
        )
    }
}
