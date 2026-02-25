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

        val shortPeriod = """
            {
                "persistent": {
                    "${AlertingSettings.QUERY_INDEX_CLEANUP_PERIOD.key}": "1s"
                }
            }
        """.trimIndent()

        client().makeRequest(
            "PUT",
            "_cluster/settings",
            emptyMap(),
            StringEntity(shortPeriod, ContentType.APPLICATION_JSON)
        )

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

        // Get the first backing index
        val aliasResponse1 = client().makeRequest("GET", "/_cat/aliases/$queryIndexAlias?format=json")
        val aliasBody1 = aliasResponse1.entity.content.readBytes().toString(Charsets.UTF_8)
        val firstBackingIndex = aliasBody1.substringAfter("\"index\":\"").substringBefore("\"")

        // Force a rollover by creating a new backing index manually
        val nextIndexNumber = firstBackingIndex.substringAfterLast("-").toInt() + 1
        val nextIndexName = "$queryIndexAlias-" + String.format(java.util.Locale.ROOT, "%06d", nextIndexNumber)

        val createIndexRequest = """
            {
                "aliases": {
                    "$queryIndexAlias": {
                        "is_write_index": true
                    }
                }
            }
        """.trimIndent()

        client().makeRequest(
            "PUT",
            "/$nextIndexName",
            emptyMap(),
            StringEntity(createIndexRequest, ContentType.APPLICATION_JSON)
        )

        // Update the old index to not be write index
        val updateAliasRequest = """
            {
                "actions": [
                    {
                        "add": {
                            "index": "$firstBackingIndex",
                            "alias": "$queryIndexAlias",
                            "is_write_index": false
                        }
                    }
                ]
            }
        """.trimIndent()

        client().makeRequest(
            "POST",
            "/_aliases",
            emptyMap(),
            StringEntity(updateAliasRequest, ContentType.APPLICATION_JSON)
        )

        // Verify we now have 2 backing indices
        val aliasResponse2 = client().makeRequest("GET", "/_cat/aliases/$queryIndexAlias?format=json")
        val aliasBody2 = aliasResponse2.entity.content.readBytes().toString(Charsets.UTF_8)
        assertTrue("Should have 2 backing indices", aliasBody2.contains(firstBackingIndex) && aliasBody2.contains(nextIndexName))

        // Delete source index but keep monitor running
        deleteIndex(testIndex)

        // Trigger cleanup
        val shortPeriod = """
            {
                "persistent": {
                    "${AlertingSettings.QUERY_INDEX_CLEANUP_PERIOD.key}": "1s"
                }
            }
        """.trimIndent()

        client().makeRequest(
            "PUT",
            "_cluster/settings",
            emptyMap(),
            StringEntity(shortPeriod, ContentType.APPLICATION_JSON)
        )

        Thread.sleep(10000)

        // Verify old backing index is deleted but write index remains
        assertFalse("Old backing index should be deleted", indexExists(firstBackingIndex))
        assertTrue("Write index should be retained", indexExists(nextIndexName))
        assertTrue("Alias should still exist", indexExists(queryIndexAlias))
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
        // Don't delete monitor - just delete source index

        // Don't delete anything - just verify cleanup runs and doesn't delete active indices
        val shortPeriod = """
            {
                "persistent": {
                    "${AlertingSettings.QUERY_INDEX_CLEANUP_PERIOD.key}": "1s"
                }
            }
        """.trimIndent()

        client().makeRequest(
            "PUT",
            "_cluster/settings",
            emptyMap(),
            StringEntity(shortPeriod, ContentType.APPLICATION_JSON)
        )

        Thread.sleep(10000)

        assertTrue("Single backing index should be retained", indexExists(queryIndexAlias))
    }
}
