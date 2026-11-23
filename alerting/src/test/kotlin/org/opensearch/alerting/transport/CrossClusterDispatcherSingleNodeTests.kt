/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.opensearch.alerting.action.GetRemoteIndexesAction
import org.opensearch.alerting.action.GetRemoteIndexesRequest
import org.opensearch.alerting.randomClusterMetricsMonitor
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.model.ClusterMetricsInput
import java.util.concurrent.TimeUnit

/**
 * The cross cluster paths run on a dedicated thread per call, so each call must stop its thread when it is done.
 */
class CrossClusterDispatcherSingleNodeTests : AlertingSingleNodeTestCase() {

    // The generated test cluster name does not pass the monitor's cluster name validation
    override fun nodeSettings(): Settings = Settings.builder().put(super.nodeSettings()).put("cluster.name", CLUSTER).build()

    fun `test get remote indexes stops its thread after each call`() {
        repeat(CALLS) {
            client().execute(GetRemoteIndexesAction.INSTANCE, GetRemoteIndexesRequest(listOf("*"), false)).actionGet()
        }

        assertNoLiveThread("GetRemoteIndexesActionThread")
    }

    fun `test a cluster metrics run against named clusters stops its thread`() {
        val input = ClusterMetricsInput(ClusterMetricsInput.ClusterMetricType.CLUSTER_HEALTH.defaultPath, "", "", listOf(CLUSTER))
        val monitor = randomClusterMetricsMonitor(inputs = listOf(input), triggers = listOf(), enabled = false)

        repeat(CALLS) {
            executeMonitor(monitor, null)
        }

        assertNoLiveThread("ClusterMetricsMonitorThread")
    }

    private fun assertNoLiveThread(name: String) {
        // The executor stops its thread asynchronously after shutdown, so give it a moment
        assertBusy(
            {
                val live = Thread.getAllStackTraces().keys.count { it.name == name && it.isAlive }
                assertEquals("live threads named $name", 0, live)
            },
            10, TimeUnit.SECONDS
        )
    }

    private companion object {
        const val CALLS = 5
        const val CLUSTER = "dispatcher-test"
    }
}
