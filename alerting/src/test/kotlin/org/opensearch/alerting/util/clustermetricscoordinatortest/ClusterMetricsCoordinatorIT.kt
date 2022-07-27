/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.clustermetricscoordinatortest

import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.util.ClusterMetricsVisualizationIndex
import org.opensearch.rest.RestStatus

class ClusterMetricsCoordinatorIT : AlertingRestTestCase() {
    /*
    Want to test these five things:
    1. Check that the index has been created, and that the name of the index is ".opendistro-alerting-cluster-metrics".
    2. Check that the total number of documents in the index is divisible by 7.
    Additionally want to check that the number of individual metrics documents == total number of docs in index/7
    3, Ingest a really old document, older than the stated deletion date, and make sure that it is properly deleted.
    4. Adjust the execution frequency setting to 1 minute, and make sure that the timestamps between the datapoints are 1 minute apart.
     */
    fun `check name`() {
        client().updateSettings("plugins.alerting.cluster_metrics.execution_frequency", "1m")
        Thread.sleep(90000)
        logger.info("CHECK NAME METHOD PLEASE WORK")
        val index = ClusterMetricsVisualizationIndex.CLUSTER_METRIC_VISUALIZATION_INDEX
        val response = client().makeRequest("HEAD", index)
        assertEquals("Index $index does not exist.", RestStatus.OK, response.restStatus())
    }
}
