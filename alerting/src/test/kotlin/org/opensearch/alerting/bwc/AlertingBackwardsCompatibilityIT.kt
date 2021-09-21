/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.alerting.bwc

import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.opensearch.alerting.ALERTING_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.LEGACY_OPENDISTRO_ALERTING_BASE_URI
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
                    assertTrue(pluginNames.contains("opendistro-alerting"))
                    createBasicMonitor()
                }
                ClusterType.MIXED -> {
                    assertTrue(pluginNames.contains("opensearch-alerting"))
                    verifyMonitor(LEGACY_OPENDISTRO_ALERTING_BASE_URI)
                }
                ClusterType.UPGRADED -> {
                    assertTrue(pluginNames.contains("opensearch-alerting"))
                    verifyMonitor(ALERTING_BASE_URI)
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
        val legacyMonitorString = """
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
              "inputs": [
                {
                  "search": {
                    "indices": [
                      "$indexName"
                    ],
                    "query": {
                      "size": 0,
                      "query": {
                        "match_all": {}
                      }
                    }
                  }
                }
              ],
              "triggers": [
                {
                  "name": "abc",
                  "severity": "1",
                  "condition": {
                    "script": {
                      "source": "ctx.results[0].hits.total.value > 100000",
                      "lang": "painless"
                    }
                  },
                  "actions": []
                }
              ]
            }
        """.trimIndent()
        createIndex(indexName, Settings.EMPTY)

        val createResponse = client().makeRequest(
            method = "POST",
            endpoint = "$LEGACY_OPENDISTRO_ALERTING_BASE_URI?refresh=true",
            params = emptyMap(),
            entity = StringEntity(legacyMonitorString, APPLICATION_JSON)
        )

        assertEquals("Create monitor failed", RestStatus.CREATED, createResponse.restStatus())
        val responseBody = createResponse.asMap()
        val createdId = responseBody["_id"] as String
        val createdVersion = responseBody["_version"] as Int
        assertNotEquals("Create monitor response is missing id", Monitor.NO_ID, createdId)
        assertTrue("Create monitor reponse has incorrect version", createdVersion > 0)
    }

    @Throws(Exception::class)
    @Suppress("UNCHECKED_CAST")
    private fun verifyMonitor(uri: String) {
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
}
