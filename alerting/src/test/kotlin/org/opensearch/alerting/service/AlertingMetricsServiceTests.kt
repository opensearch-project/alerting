/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Test
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.opensearch.commons.alerting.model.MonitorRunResult
import org.opensearch.commons.alerting.model.TriggerRunResult
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest
import java.time.Instant

class AlertingMetricsServiceTests {

    @After
    fun tearDown() {
        AlertingMetricsService.close()
    }

    private fun successRun(): MonitorRunResult<TriggerRunResult> =
        MonitorRunResult("monitor", Instant.now(), Instant.now())

    private fun failedRun(): MonitorRunResult<TriggerRunResult> =
        MonitorRunResult("monitor", Instant.now(), Instant.now(), RuntimeException("boom"))

    private fun dim(d: MetricDatum, name: String): String? =
        d.dimensions().firstOrNull { it.name() == name }?.value()

    @Test
    fun `disabled service is a no-op`() {
        AlertingMetricsService.resetForTest()
        AlertingMetricsService.recordRun("app-1", successRun())
        AlertingMetricsService.recordExecutionFailure("app-1")
        AlertingMetricsService.flush()
    }

    @Test
    fun `emits per-app and per-cell rollup with correct dimensions`() {
        val client = mock(CloudWatchClient::class.java)
        AlertingMetricsService.initializeForTest(client, "Test/Alerting", "beta", "us-west-2", "cell-1")

        // app-A: 2 executions (1 failed); app-B: 1 execution (success)
        AlertingMetricsService.recordRun("app-A", successRun())
        AlertingMetricsService.recordRun("app-A", failedRun())
        AlertingMetricsService.recordRun("app-B", successRun())

        AlertingMetricsService.flush()

        val captor = ArgumentCaptor.forClass(PutMetricDataRequest::class.java)
        verify(client).putMetricData(captor.capture())
        val req = captor.value
        assertEquals("Test/Alerting", req.namespace())

        val byKey = HashMap<String, Double>()
        for (d in req.metricData()) {
            // Every datum carries Stage/Region/CellId.
            assertEquals("beta", dim(d, AlertingMetricsService.DIMENSION_STAGE))
            assertEquals("us-west-2", dim(d, AlertingMetricsService.DIMENSION_REGION))
            assertEquals("cell-1", dim(d, AlertingMetricsService.DIMENSION_CELL_ID))
            val app = dim(d, AlertingMetricsService.DIMENSION_APPLICATION_ID) ?: "CELL"
            byKey["${d.metricName()}|$app"] = d.value()
        }

        // Per-application series.
        assertEquals(2.0, byKey["${AlertingMetricsService.METRIC_EXECUTIONS}|app-A"])
        assertEquals(1.0, byKey["${AlertingMetricsService.METRIC_EXECUTION_FAILURES}|app-A"])
        assertEquals(1.0, byKey["${AlertingMetricsService.METRIC_EXECUTIONS}|app-B"])
        assertEquals(0.0, byKey["${AlertingMetricsService.METRIC_EXECUTION_FAILURES}|app-B"])

        // Per-cell rollup = sum across apps.
        assertEquals(3.0, byKey["${AlertingMetricsService.METRIC_EXECUTIONS}|CELL"])
        assertEquals(1.0, byKey["${AlertingMetricsService.METRIC_EXECUTION_FAILURES}|CELL"])

        // Counters reset after flush: a second flush emits nothing.
        AlertingMetricsService.flush()
        verify(client).putMetricData(captor.capture())
    }

    @Test
    fun `thrown failure counts an execution and a failure`() {
        val client = mock(CloudWatchClient::class.java)
        AlertingMetricsService.initializeForTest(client, "Test/Alerting", "beta", "us-west-2", "cell-1")

        AlertingMetricsService.recordExecutionFailure("app-X")
        AlertingMetricsService.flush()

        val captor = ArgumentCaptor.forClass(PutMetricDataRequest::class.java)
        verify(client).putMetricData(captor.capture())
        val executions = captor.value.metricData().first {
            it.metricName() == AlertingMetricsService.METRIC_EXECUTIONS &&
                dim(it, AlertingMetricsService.DIMENSION_APPLICATION_ID) == "app-X"
        }
        val failures = captor.value.metricData().first {
            it.metricName() == AlertingMetricsService.METRIC_EXECUTION_FAILURES &&
                dim(it, AlertingMetricsService.DIMENSION_APPLICATION_ID) == "app-X"
        }
        assertEquals(1.0, executions.value(), 0.0)
        assertEquals(1.0, failures.value(), 0.0)
    }

    @Test
    fun `null applicationId is bucketed under unknown`() {
        val client = mock(CloudWatchClient::class.java)
        AlertingMetricsService.initializeForTest(client, "Test/Alerting", "beta", "us-west-2", "cell-1")

        AlertingMetricsService.recordRun(null, successRun())
        AlertingMetricsService.flush()

        val captor = ArgumentCaptor.forClass(PutMetricDataRequest::class.java)
        verify(client).putMetricData(captor.capture())
        val apps = captor.value.metricData().mapNotNull { dim(it, AlertingMetricsService.DIMENSION_APPLICATION_ID) }
        assertEquals(true, apps.contains("unknown"))
    }

    @Test
    fun `log-only mode records and flushes without a client`() {
        // No CloudWatch client is created in log-only mode; flush logs instead and still resets.
        AlertingMetricsService.initializeForTestLogOnly("Test/Alerting", "beta", "us-west-2", "cell-1")

        AlertingMetricsService.recordRun("app-A", failedRun())
        assertEquals(1L, AlertingMetricsService.peekExecutionsForTest("app-A"))

        AlertingMetricsService.flush()

        // Counters reset after a (log-only) flush, proving the emit path ran end-to-end.
        assertEquals(0L, AlertingMetricsService.peekExecutionsForTest("app-A"))
    }
}
