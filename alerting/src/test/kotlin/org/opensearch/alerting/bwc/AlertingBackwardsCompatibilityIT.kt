/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.bwc

import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.opensearch.alerting.ALERTING_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.model.Monitor
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder

class AlertingBackwardsCompatibilityIT : AlertingRestTestCase() {

    companion object {
        private val CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.bwcsuite"))
        private val CLUSTER_NAME = System.getProperty("tests.clustername")
    }

    override fun preserveIndicesUponCompletion(): Boolean = true

    override fun preserveReposUponCompletion(): Boolean = true

    override fun preserveTemplatesUponCompletion(): Boolean = true

    override fun preserveODFEIndicesAfterTest(): Boolean = true

    override fun restClientSettings(): Settings {
        return Settings.builder()
            .put(super.restClientSettings())
            // increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(CLIENT_SOCKET_TIMEOUT, "90s")
            .build()
    }

    @Throws(Exception::class)
    @Suppress("UNCHECKED_CAST")
    fun `test backwards compatibility`() {
        val uri = getPluginUri()
        val responseMap = getAsMap(uri)["nodes"] as Map<String, Map<String, Any>>
        for (response in responseMap.values) {
            val plugins = response["plugins"] as List<Map<String, Any>>
            val pluginNames = plugins.map { plugin -> plugin["name"] }.toSet()
            when (CLUSTER_TYPE) {
                ClusterType.OLD -> {
                    assertTrue(pluginNames.contains("opensearch-alerting"))
                    createBasicMonitor()
                }
                ClusterType.MIXED -> {
                    assertTrue(pluginNames.contains("opensearch-alerting"))
                    verifyMonitorExists(ALERTING_BASE_URI)
                    // TODO: Need to move the base URI being used here into a constant and rename ALERTING_BASE_URI to
                    //  MONITOR_BASE_URI
                    verifyMonitorStats("/_plugins/_alerting")
                }
                ClusterType.UPGRADED -> {
                    assertTrue(pluginNames.contains("opensearch-alerting"))
                    verifyMonitorExists(ALERTING_BASE_URI)
                    // TODO: Change the next execution time of the Monitor manually instead since this inflates
                    //  the test execution by a lot (might have to wait for Job Scheduler plugin integration first)
                    // Waiting a minute to ensure the Monitor ran again at least once before checking if the job is running
                    // on time
                    Thread.sleep(60000)
                    verifyMonitorStats("/_plugins/_alerting")
                }
            }
            break
        }
    }

    private enum class ClusterType {
        OLD,
        MIXED,
        UPGRADED;

        companion object {
            fun parse(value: String): ClusterType {
                return when (value) {
                    "old_cluster" -> OLD
                    "mixed_cluster" -> MIXED
                    "upgraded_cluster" -> UPGRADED
                    else -> throw AssertionError("Unknown cluster type: $value")
                }
            }
        }
    }

    private fun getPluginUri(): String {
        return when (CLUSTER_TYPE) {
            ClusterType.OLD -> "_nodes/$CLUSTER_NAME-0/plugins"
            ClusterType.MIXED -> {
                when (System.getProperty("tests.rest.bwcsuite_round")) {
                    "second" -> "_nodes/$CLUSTER_NAME-1/plugins"
                    "third" -> "_nodes/$CLUSTER_NAME-2/plugins"
                    else -> "_nodes/$CLUSTER_NAME-0/plugins"
                }
            }
            ClusterType.UPGRADED -> "_nodes/plugins"
        }
    }

    @Throws(Exception::class)
    private fun createBasicMonitor() {
        val indexName = "test_bwc_index"
        val bwcMonitorString = """
            {
              "type": "monitor",
              "name": "test_bwc_monitor",
              "enabled": true,
              "schedule": {
                "period": {
                  "interval": 1,
                  "unit": "MINUTES"
                }
              },
              "inputs": [{
                "search": {
                  "indices": ["$indexName"],
                  "query": {
                    "size": 0,
                    "aggregations": {},
                    "query": {
                      "match_all": {}
                    }
                  }
                }
              }],
              "triggers": [{
                "name": "abc",
                "severity": "1",
                "condition": {
                  "script": {
                    "source": "ctx.results[0].hits.total.value > 100000",
                    "lang": "painless"
                  }
                },
                "actions": []
              }]
            }
        """.trimIndent()
        createIndex(indexName, Settings.EMPTY)

        val createResponse = client().makeRequest(
            method = "POST",
            endpoint = "$ALERTING_BASE_URI?refresh=true",
            params = emptyMap(),
            entity = StringEntity(bwcMonitorString, APPLICATION_JSON)
        )

        assertEquals("Create monitor failed", RestStatus.CREATED, createResponse.restStatus())
        val responseBody = createResponse.asMap()
        val createdId = responseBody["_id"] as String
        val createdVersion = responseBody["_version"] as Int
        assertNotEquals("Create monitor response is missing id", Monitor.NO_ID, createdId)
        assertTrue("Create monitor response has incorrect version", createdVersion > 0)
    }

    @Throws(Exception::class)
    @Suppress("UNCHECKED_CAST")
    private fun verifyMonitorExists(uri: String) {
        val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()
        val searchResponse = client().makeRequest(
            "GET",
            "$uri/_search",
            emptyMap(),
            StringEntity(search, APPLICATION_JSON)
        )
        assertEquals("Search monitor failed", RestStatus.OK, searchResponse.restStatus())
        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("Unexpected number of Monitors returned", 1, numberDocsFound)
    }

    @Throws(Exception::class)
    @Suppress("UNCHECKED_CAST")
    /**
     * Monitor stats will check if the Monitor scheduled job is running on time but does not necessarily mean that the
     * Monitor execution itself did not fail.
     */
    private fun verifyMonitorStats(uri: String) {
        val statsResponse = client().makeRequest(
            "GET",
            "$uri/stats",
            emptyMap()
        )
        assertEquals("Monitor stats failed", RestStatus.OK, statsResponse.restStatus())
        val xcp = createParser(XContentType.JSON.xContent(), statsResponse.entity.content)
        val responseMap = xcp.map()
        val nodesCount = responseMap["_nodes"]!! as Map<String, Any>
        val totalNodes = nodesCount["total"]
        val successfulNodes = nodesCount["successful"]
        val nodesOnSchedule = responseMap["nodes_on_schedule"]!!
        assertEquals("Incorrect number of total nodes", 3, totalNodes)
        assertEquals("Some nodes in stats response failed", totalNodes, successfulNodes)
        assertEquals("Not all nodes are on schedule", totalNodes, nodesOnSchedule)
    }
}
