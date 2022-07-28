/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.clustermetricscoordinatortest

import org.junit.Before
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.model.ClusterMetricsDataPoint
import org.opensearch.alerting.util.ClusterMetricsVisualizationIndex
import org.opensearch.common.xcontent.XContentType
import org.opensearch.rest.RestStatus

class ClusterMetricsCoordinatorIT : AlertingRestTestCase() {
    /*
    2. Check that the total number of documents in the index is divisible by 7.
    Additionally want to check that the number of individual metrics documents == total number of docs in index/7
    3, Ingest a really old document, older than the stated deletion date, and make sure that it is properly deleted.
    4. Adjust the execution frequency setting to 1 minute, and make sure that the timestamps between the datapoints are 1 minute apart.
    Check 2 minute apart after
     */
    @Before
    fun setup() {
        // When setting up the tests, change the execution frequency and history max age settings to 1 minute and 10 minutes from
        // 15 minutes and 7 days.
        generateData()
        val response = client().makeRequest(
            "GET",
            "_cluster/settings?flat_settings=true"
        ).asMap()
        val persistentMap = response["persistent"] as Map<String, Any>
        val executionFrequency = persistentMap["plugins.alerting.cluster_metrics.execution_frequency"].toString()
        val storageTime = persistentMap["plugins.alerting.cluster_metrics.metrics_history_max_age"].toString()
        assertEquals(executionFrequency, "1m")
        assertEquals(storageTime, "10m")
    }

    fun `test checkName`() {
        // WHEN + THEN, check whether the created index exists and has the name '.opendistro-alerting-cluster-metrics'
        val index = ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX
        val response = client().makeRequest("HEAD", index)
        assertEquals("Index $index does not exist.", RestStatus.OK, response.restStatus())
    }
    fun `test numberDocs`() {
        // Check that the total number of documents found is divisible by the total number of metric types.
        val response = client().makeRequest(
            "GET",
            ".opendistro-alerting-cluster-metrics/_search"
        )
        val xcp = createParser(XContentType.JSON.xContent(), response.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberOfDocsFound = (hits["total"]?.get("value") as Int)
        val size = ClusterMetricsDataPoint.MetricType.values().size
        assertEquals((numberOfDocsFound.mod(size)), 0)
        logger.info("this is the hits $hits")

        // check that each of the metric types has a unique timestamp, and number of timestamps must be equal to total docs divided by 7
        // expect that there only document created for each metric type.
        var mapCheck = hashMapOf<String, Set<String>>()
        ClusterMetricsDataPoint.MetricType.values().forEach { mapCheck[it.metricName] = setOf() }
        logger.info("this is mapCheck $mapCheck")
//        val docs = hits["hits"]!!
//        for (doc in docs) {
//
//        }
    }

    private fun generateData() {
        client().updateSettings("plugins.alerting.cluster_metrics.execution_frequency", "1s")
        client().updateSettings("plugins.alerting.cluster_metrics.metrics_history_max_age", "10m")
        Thread.sleep(60000)
        client().updateSettings("plugins.alerting.cluster_metrics.execution_frequency", "1m")
    }
}
