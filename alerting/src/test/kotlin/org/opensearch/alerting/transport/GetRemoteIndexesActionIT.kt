/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.action.GetRemoteIndexesRequest.Companion.INCLUDE_MAPPINGS_FIELD
import org.opensearch.alerting.action.GetRemoteIndexesRequest.Companion.INDEXES_FIELD
import org.opensearch.alerting.action.GetRemoteIndexesRequest.Companion.INVALID_PATTERN_MESSAGE
import org.opensearch.alerting.action.GetRemoteIndexesResponse.ClusterIndexes
import org.opensearch.alerting.action.GetRemoteIndexesResponse.ClusterIndexes.ClusterIndex.Companion.INDEX_HEALTH_FIELD
import org.opensearch.alerting.action.GetRemoteIndexesResponse.ClusterIndexes.ClusterIndex.Companion.INDEX_NAME_FIELD
import org.opensearch.alerting.action.GetRemoteIndexesResponse.ClusterIndexes.ClusterIndex.Companion.MAPPINGS_FIELD
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.resthandler.RestGetRemoteIndexesAction
import org.opensearch.alerting.settings.AlertingSettings.Companion.CROSS_CLUSTER_MONITORING_ENABLED
import org.opensearch.client.Response
import org.opensearch.client.ResponseException
import org.opensearch.cluster.health.ClusterHealthStatus
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.alerting.util.string
import org.opensearch.core.rest.RestStatus
import java.util.*

@Suppress("UNCHECKED_CAST")
class GetRemoteIndexesActionIT : AlertingRestTestCase() {
    private var remoteMonitoringEnabled = false
    private var remoteClusters = listOf<String>()

    private val mappingFieldToTypePairs1 = listOf(
        "timestamp" to "date",
        "color" to "keyword",
        "message" to "text",
    )

    private val mappingFieldToTypePairs2 = listOf(
        "timestamp" to "date",
        "message" to "text",
    )

    fun `test with remote monitoring disabled`() {
        // Disable remote monitoring if not already disabled
        toggleRemoteMonitoring(false)
        try {
            getRemoteIndexes("$INDEXES_FIELD=*,*:*&$INCLUDE_MAPPINGS_FIELD=false")
            fail("Expected 403 Method FORBIDDEN response.")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.FORBIDDEN, e.response.restStatus())
            assertEquals(
                "Remote monitoring is not enabled.",
                (e.response.asMap()["error"] as Map<String, Any>)["reason"]
            )
        }
    }

    fun `test with blank indexes param`() {
        // Enable remote monitoring if not already enabled
        toggleRemoteMonitoring(true)
        try {
            getRemoteIndexes("$INCLUDE_MAPPINGS_FIELD=false")
            fail("Expected 400 Method BAD_REQUEST response.")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.BAD_REQUEST, e.response.restStatus())
            assertEquals(
                INVALID_PATTERN_MESSAGE,
                (e.response.asMap()["error"] as Map<String, Any>)["reason"]
            )
        }
    }

    fun `test with blank include_mappings param`() {
        // Enable remote monitoring if not already enabled
        toggleRemoteMonitoring(true)

        // Create test indexes
        val index1 = createTestIndex(
            index = randomAlphaOfLength(10).lowercase(Locale.ROOT),
            mapping = formatMappingsJson(mappingFieldToTypePairs1)
        )

        val index2 = createTestIndex(
            index = randomAlphaOfLength(10).lowercase(Locale.ROOT),
            mapping = formatMappingsJson(mappingFieldToTypePairs2)
        )

        val expectedNames = listOf(index1, index2)

        // Call API
        val response = getRemoteIndexes("$INDEXES_FIELD=*,*:*")
        assertEquals(RestStatus.OK, response.restStatus())

        val responseMap = response.asMap() as Map<String, Map<String, Any>>
        responseMap.entries.forEach { (clusterName, clusterDetails) ->
            // Validate cluster-level response details
            assertEquals(clusterName, clusterDetails[ClusterIndexes.CLUSTER_NAME_FIELD])
            assertClusterHealth(clusterDetails[ClusterIndexes.CLUSTER_HEALTH_FIELD] as String)
            assertTrue(clusterDetails[ClusterIndexes.HUB_CLUSTER_FIELD] is Boolean)
            assertTrue(clusterDetails[ClusterIndexes.INDEX_LATENCY_FIELD] is Number)

            assertNotNull(clusterDetails[ClusterIndexes.INDEXES_FIELD])
            val indexes = clusterDetails[ClusterIndexes.INDEXES_FIELD] as Map<String, Map<String, Any>>


            // Validate index-level response details
            expectedNames.forEach { indexName ->
                assertNotNull(indexes[indexName])
                val indexDetails = indexes[indexName]!!
                assertEquals(indexName, indexDetails[INDEX_NAME_FIELD])
                assertClusterHealth(indexDetails[INDEX_HEALTH_FIELD] as String)
                assertTrue((indexDetails[MAPPINGS_FIELD] as Map<String, Any>).isEmpty())
            }
        }

        // Delete test indexes
        deleteIndex(index1)
        deleteIndex(index2)
    }

    fun `test with FALSE include_mappings param`() {
        // Enable remote monitoring if not already enabled
        toggleRemoteMonitoring(true)

        // Create test indexes
        val index1 = createTestIndex(
            index = randomAlphaOfLength(10).lowercase(Locale.ROOT),
            mapping = formatMappingsJson(mappingFieldToTypePairs1)
        )

        val index2 = createTestIndex(
            index = randomAlphaOfLength(10).lowercase(Locale.ROOT),
            mapping = formatMappingsJson(mappingFieldToTypePairs2)
        )

        val expectedNames = listOf(index1, index2)

        // Call API
        val response = getRemoteIndexes("$INDEXES_FIELD=*,*:*&$INCLUDE_MAPPINGS_FIELD=false")
        assertEquals(RestStatus.OK, response.restStatus())

        val responseMap = response.asMap() as Map<String, Map<String, Any>>
        responseMap.entries.forEach { (clusterName, clusterDetails) ->
            // Validate cluster-level response details
            assertEquals(clusterName, clusterDetails[ClusterIndexes.CLUSTER_NAME_FIELD])
            assertClusterHealth(clusterDetails[ClusterIndexes.CLUSTER_HEALTH_FIELD] as String)
            assertTrue(clusterDetails[ClusterIndexes.HUB_CLUSTER_FIELD] is Boolean)
            assertTrue(clusterDetails[ClusterIndexes.INDEX_LATENCY_FIELD] is Number)

            assertNotNull(clusterDetails[ClusterIndexes.INDEXES_FIELD])
            val indexes = clusterDetails[ClusterIndexes.INDEXES_FIELD] as Map<String, Map<String, Any>>


            // Validate index-level response details
            expectedNames.forEach { indexName ->
                assertNotNull(indexes[indexName])
                val indexDetails = indexes[indexName]!!
                assertEquals(indexName, indexDetails[INDEX_NAME_FIELD])
                assertClusterHealth(indexDetails[INDEX_HEALTH_FIELD] as String)
                assertTrue((indexDetails[MAPPINGS_FIELD] as Map<String, Any>).isEmpty())
            }
        }

        // Delete test indexes
        deleteIndex(index1)
        deleteIndex(index2)
    }

    fun `test with TRUE include_mappings param`() {
        // Enable remote monitoring if not already enabled
        toggleRemoteMonitoring(true)

        // Create test indexes
        val index1 = createTestIndex(
            index = randomAlphaOfLength(10).lowercase(Locale.ROOT),
            mapping = formatMappingsJson(mappingFieldToTypePairs1)
        )

        val index2 = createTestIndex(
            index = randomAlphaOfLength(10).lowercase(Locale.ROOT),
            mapping = formatMappingsJson(mappingFieldToTypePairs2)
        )

        val expectedNames = listOf(index1, index2)

        // Call API
        val response = getRemoteIndexes("$INDEXES_FIELD=*,*:*&$INCLUDE_MAPPINGS_FIELD=true")
        assertEquals(RestStatus.OK, response.restStatus())

        val responseMap = response.asMap() as Map<String, Map<String, Any>>
        responseMap.entries.forEach { (clusterName, clusterDetails) ->
            // Validate cluster-level response details
            assertEquals(clusterName, clusterDetails[ClusterIndexes.CLUSTER_NAME_FIELD])
            assertClusterHealth(clusterDetails[ClusterIndexes.CLUSTER_HEALTH_FIELD] as String)
            assertTrue(clusterDetails[ClusterIndexes.HUB_CLUSTER_FIELD] is Boolean)
            assertTrue(clusterDetails[ClusterIndexes.INDEX_LATENCY_FIELD] is Number)

            assertNotNull(clusterDetails[ClusterIndexes.INDEXES_FIELD])
            val indexes = clusterDetails[ClusterIndexes.INDEXES_FIELD] as Map<String, Map<String, Any>>

            // Validate index-level response details
            expectedNames.forEach { indexName ->
                assertNotNull(indexes[indexName])
                val indexDetails = indexes[indexName]!!
                assertEquals(indexName, indexDetails[INDEX_NAME_FIELD])
                assertClusterHealth(indexDetails[INDEX_HEALTH_FIELD] as String)

                // Validate index mappings
                val mappings = (indexDetails[MAPPINGS_FIELD] as Map<String, Any>)["properties"] as Map<String, Map<String, String>>
                if (indexName == index1) {
                    mappingFieldToTypePairs1.forEach {
                        assertNotNull(mappings[it.first])
                        assertEquals(it.second, mappings[it.first]!!["type"])
                    }
                } else {
                    mappingFieldToTypePairs2.forEach {
                        assertNotNull(mappings[it.first])
                        assertEquals(it.second, mappings[it.first]!!["type"])
                    }
                }
            }
        }

        // Delete test indexes
        deleteIndex(index1)
        deleteIndex(index2)
    }

    fun `test with specific index name`() {
        // Enable remote monitoring if not already enabled
        toggleRemoteMonitoring(true)

        // Create test indexes
        val index1 = createTestIndex(
            index = randomAlphaOfLength(10).lowercase(Locale.ROOT),
            mapping = formatMappingsJson(mappingFieldToTypePairs1)
        )

        val index2 = createTestIndex(
            index = randomAlphaOfLength(10).lowercase(Locale.ROOT),
            mapping = formatMappingsJson(mappingFieldToTypePairs2)
        )

        val expectedNames = listOf(index1)

        // Call API
        val response = getRemoteIndexes("$INDEXES_FIELD=$index1:*&$INCLUDE_MAPPINGS_FIELD=true")
        assertEquals(RestStatus.OK, response.restStatus())

        val responseMap = response.asMap() as Map<String, Map<String, Any>>
        responseMap.entries.forEach { (clusterName, clusterDetails) ->
            // Validate cluster-level response details
            assertEquals(clusterName, clusterDetails[ClusterIndexes.CLUSTER_NAME_FIELD])
            assertClusterHealth(clusterDetails[ClusterIndexes.CLUSTER_HEALTH_FIELD] as String)
            assertTrue(clusterDetails[ClusterIndexes.HUB_CLUSTER_FIELD] is Boolean)
            assertTrue(clusterDetails[ClusterIndexes.INDEX_LATENCY_FIELD] is Number)

            assertNotNull(clusterDetails[ClusterIndexes.INDEXES_FIELD])
            val indexes = clusterDetails[ClusterIndexes.INDEXES_FIELD] as Map<String, Map<String, Any>>
            assertEquals(expectedNames.size, indexes.keys.size)

            // Validate index-level response details
            expectedNames.forEach { indexName ->
                assertNotNull(indexes[indexName])
                val indexDetails = indexes[indexName]!!
                assertEquals(indexName, indexDetails[INDEX_NAME_FIELD])
                assertClusterHealth(indexDetails[INDEX_HEALTH_FIELD] as String)
                val mappings = (indexDetails[MAPPINGS_FIELD] as Map<String, Any>)["properties"] as Map<String, Map<String, String>>

                // Validate index mappings
                mappingFieldToTypePairs1.forEach {
                    assertNotNull(mappings[it.first])
                    assertEquals(it.second, mappings[it.first]!!["type"])
                }
            }
        }

        // Delete test indexes
        deleteIndex(index1)
        deleteIndex(index2)
    }

    private fun getRemoteIndexes(params: String): Response {
        return client().makeRequest("GET", "${RestGetRemoteIndexesAction.ROUTE}?$params")
    }

    private fun toggleRemoteMonitoring(setting: Boolean) {
        if (remoteMonitoringEnabled != setting) {
            client().updateSettings(CROSS_CLUSTER_MONITORING_ENABLED.key, setting)

            val settings = client().getSettings()
            val updatedSetting = getEnabledSetting(settings)

            if (setting) assertTrue(updatedSetting)
            else assertFalse(updatedSetting)

            remoteMonitoringEnabled = updatedSetting

            compileRemoteClustersList(settings)
        }
    }

    private fun compileRemoteClustersList(settings: Map<String, Any>) {
        if (remoteClusters.isEmpty()) {
            val remotes = settings["persistent.cluster.remote"] as Map<String, Any>?
            remoteClusters = remotes?.keys?.toList() ?: emptyList()
        }
    }

    private fun getEnabledSetting(settings: Map<String, Any>): Boolean {
        val persistentSettings = settings["persistent"] as Map<String, Any>
        val updatedSetting = persistentSettings[CROSS_CLUSTER_MONITORING_ENABLED.key]
        assertNotNull(updatedSetting)
        return (updatedSetting as String).toBoolean()
    }

    private fun formatMappingsJson(fieldToTypePairs: List<Pair<String, String>>): String {
        val builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
        fieldToTypePairs.forEach {
            builder.startObject(it.first)
                .field("type", it.second)
                .endObject()
        }
        builder.endObject().endObject()
        val mappingsJson = builder.string()
        return mappingsJson.substring(1, mappingsJson.lastIndex)
    }

    private fun assertClusterHealth(health: String) {
        try {
            ClusterHealthStatus.fromString(health)
        } catch (e: IllegalArgumentException) {
            fail("Should not throw IllegalArgumentException.")
        }
    }
}
