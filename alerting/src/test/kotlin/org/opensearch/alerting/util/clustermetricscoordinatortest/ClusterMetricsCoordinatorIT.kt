/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.clustermetricscoordinatortest

import org.apache.http.entity.ContentType
import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.junit.After
import org.junit.Before
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.model.ClusterMetricsDataPoint
import org.opensearch.alerting.opensearchapi.string
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.ClusterMetricsVisualizationIndex
import org.opensearch.client.Response
import org.opensearch.client.ResponseException
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.common.xcontent.XContentType
import org.opensearch.rest.RestStatus
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*
import kotlin.collections.ArrayList
class ClusterMetricsCoordinatorIT : AlertingRestTestCase() {
    @Before
    fun setup() {
        // When setting up the tests, change the execution frequency and history max age settings to 1 minute and 10 minutes from
        // 15 minutes and 7 days.
        generateData()
        val response = getSettings()
        val persistentMap = response["persistent"] as Map<String, Any>
        val executionFrequency = persistentMap["plugins.alerting.cluster_metrics.execution_frequency"].toString()
        val storageTime = persistentMap["plugins.alerting.cluster_metrics.metrics_history_max_age"].toString()
        assertEquals(executionFrequency, "1m")
        assertEquals(storageTime, "10m")
    }

    fun `test check name of index`() {
        // WHEN + THEN, check whether the created index exists and has the name '.opendistro-alerting-cluster-metrics'
        val index = ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX
        val response = client().makeRequest("HEAD", index)
        assertEquals("Index $index does not exist.", RestStatus.OK, response.restStatus())
    }

    fun `test check that number of documents is correct`() {
        // Check that the total number of documents found is divisible by the total number of metric types.
        val response = getResponse()
        val hits = getHits(response)
        val numberOfDocsFound = (hits["total"]?.get("value") as Int)
        val size = ClusterMetricsDataPoint.MetricType.values().size
        assertEquals((numberOfDocsFound.mod(size)), 0)
        val docs = hits["hits"] as ArrayList<Map<String, Any>>

        // check that each of the metric types has a unique timestamp, and number of timestamps must be equal to total docs divided by 7
        // expect that there should only be one document created for each metric type at a time.
        val mapCheck = hashMapOf<String, Set<String>>()
        ClusterMetricsDataPoint.MetricType.values().forEach { mapCheck[it.metricName] = mutableSetOf() }
        for (doc in docs) {
            val source = doc["_source"] as Map<String, Map<String, Any>>
            val metricType = source.keys.first()
            try {
                ClusterMetricsDataPoint.MetricType.valueOf(metricType.uppercase(Locale.getDefault()))
                mapCheck[metricType] = mapCheck[metricType]?.plus(source[metricType]?.get("timestamp")) as Set<String>
            } catch (e: java.lang.IllegalArgumentException) {
                logger.info("Key does not exist in the enum class.")
            }
        }
        assertEquals(mapCheck.values.toSet().size, 1)
    }

    fun `test deleting docs from index`() {
        val time = getTime()
        createDoc(time)
        Thread.sleep(60000)
        val response = getResponse()
        val hits = getHits(response)
        var flag = false
        val docs = hits["hits"] as ArrayList<Map<String, Any>>

        for (doc in docs) {
            val source = doc["_source"] as Map<String, Map<String, Any>>
            val metricType = source.keys.first()
            if (metricType == "cluster_status") {
                if (source[metricType]?.get("timestamp") == time) {
                    assertTrue(flag)
                    return
                }
            }
        }
        flag = true
        assertTrue(flag)
    }

    fun `test execution frequency of job`() {
        client().updateSettings("plugins.alerting.cluster_metrics.execution_frequency", "2m")
        Thread.sleep(300000)

        val response = getResponse()
        val hits = getHits(response)
        val docs = hits["hits"] as ArrayList<Map<String, Any>>
        val times = mutableSetOf<String>()

        for (doc in docs) {
            val source = doc["_source"] as Map<String, Map<String, Any>>
            val metricType = source.keys.first()
            times.add(source[metricType]?.get("timestamp").toString())
        }
        logger.info("this is times $times")
        val time1 = Instant.parse(times.elementAt(times.size - 1))
        val time2 = Instant.parse(times.elementAt(times.size - 2))
        val diff = time2.until(time1, ChronoUnit.MINUTES)
        assertEquals(diff, 2)
    }

    fun `test update storage time to less than minimum storage time`() {
        try {
            client().updateSettings("plugins.alerting.cluster_metrics.metrics_history_max_age", "30s")
        } catch (t: ResponseException) {
            val responseMap = t.response.asMap()
            val errMap = responseMap["error"] as Map<String, Any>
            assertEquals("illegal_argument_exception", errMap["type"])
        }
    }

    fun `test update frequency to less than minimum frequency`() {
        try {
            client().updateSettings("plugins.alerting.cluster_metrics.execution_frequency", "1ms")
        } catch (t: ResponseException) {
            val responseMap = t.response.asMap()
            val errMap = responseMap["error"] as Map<String, Any>
            assertEquals("illegal_argument_exception", errMap["type"])
        }
    }

    fun `test update execution frequency greater than storage time`() {
        try {
            client().updateSettings("plugins.alerting.cluster_metrics.metrics_history_max_age", "20m")
            client().updateSettings("plugins.alerting.cluster_metrics.execution_frequency", "25m")
        } catch (t: ResponseException) {
            val responseMap = t.response.asMap()
            val errMap = responseMap["error"] as Map<String, Any>
            assertEquals("illegal_argument_exception", errMap["type"])
        }
    }

    fun `test successful client setting update`() {
        client().updateSettings("plugins.alerting.cluster_metrics.metrics_history_max_age", "400m")
        client().updateSettings("plugins.alerting.cluster_metrics.execution_frequency", "12m")
        val response = getSettings()
        val persistentMap = response["persistent"] as Map<String, Any>
        val executionFrequency = persistentMap["plugins.alerting.cluster_metrics.execution_frequency"].toString()
        val storageTime = persistentMap["plugins.alerting.cluster_metrics.metrics_history_max_age"].toString()
        assertEquals(executionFrequency, "12m")
        assertEquals(storageTime, "400m")
    }

    fun `test simultaneously changing execution frequency and storage time where execution less than storage time`() {
        val settings = jsonBuilder()
            .startObject()
            .startObject("persistent")
            .field("plugins.alerting.cluster_metrics.execution_frequency", "5m")
            .field("plugins.alerting.cluster_metrics.metrics_history_max_age", "10m")
            .endObject()
            .endObject()
            .string()
        val response = client().makeRequest("PUT", "_cluster/settings", StringEntity(settings, APPLICATION_JSON))
        assertEquals(RestStatus.OK, response.restStatus())
    }
    fun `test simultaneously changing execution frequency and storage time where execution greater than storage time`() {
        val settings = jsonBuilder()
            .startObject()
            .startObject("persistent")
            .field("plugins.alerting.cluster_metrics.execution_frequency", "10m")
            .field("plugins.alerting.cluster_metrics.metrics_history_max_age", "5m")
            .endObject()
            .endObject()
            .string()
        try {
            client().makeRequest("PUT", "_cluster/settings", StringEntity(settings, APPLICATION_JSON))
        } catch (t: ResponseException) {
            val responseMap = t.response.asMap()
            val errMap = responseMap["error"] as Map<String, Any>
            assertEquals("illegal_argument_exception", errMap["type"])
        }
    }

    @After
    // Reset the settings back to default, delete the created index.
    fun cleanup() {
        // reset settings
        client().updateSettings(
            "plugins.alerting.cluster_metrics.metrics_history_max_age",
            AlertingSettings.METRICS_STORE_TIME_DEFAULT_VALUE
        )
        client().updateSettings(
            "plugins.alerting.cluster_metrics.execution_frequency",
            AlertingSettings.METRICS_EXECUTION_FREQUENCY_DEFAULT_VALUE
        )
        client().makeRequest("DELETE", ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX)
    }
    private fun generateData() {
        client().updateSettings("plugins.alerting.cluster_metrics.execution_frequency", "1s")
        client().updateSettings("plugins.alerting.cluster_metrics.metrics_history_max_age", "10m")
        Thread.sleep(60000)
        client().updateSettings("plugins.alerting.cluster_metrics.execution_frequency", "1m")
    }

    private fun getResponse(): Response {
        val settings = jsonBuilder()
            .startObject()
            .field("size", 10000)
            .endObject()
            .string()
        return client().makeRequest(
            "GET",
            ".opendistro-alerting-cluster-metrics/_search",
            StringEntity(settings, ContentType.APPLICATION_JSON)
        )
    }

    private fun createDoc(time: Instant?) {
        val doc = jsonBuilder()
            .startObject()
            .startObject("cluster_status")
            .field("timestamp", time.toString())
            .field("value", "yellow")
            .endObject()
            .endObject()
            .string()
        client().makeRequest(
            "POST",
            ".opendistro-alerting-cluster-metrics/_doc",
            StringEntity(doc, ContentType.APPLICATION_JSON)
        )
    }
    private fun getTime(): Instant? {
        return Instant.now().minus(10, ChronoUnit.MINUTES).minus(1, ChronoUnit.MINUTES)
    }

    private fun getHits(response: Response): Map<String, Map<String, Any>> {
        val xcp = createParser(XContentType.JSON.xContent(), response.entity.content)
        return xcp.map()["hits"]!! as Map<String, Map<String, Any>>
    }

    private fun getSettings(): Map<String, Any> {
        return client().makeRequest(
            "GET",
            "_cluster/settings?flat_settings=true"
        ).asMap()
    }
}
