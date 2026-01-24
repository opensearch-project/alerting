/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.junit.Assert
import org.opensearch.alerting.randomBucketLevelMonitorRunResult
import org.opensearch.alerting.randomQueryLevelMonitorRunResult
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.Test

class ExecuteMonitorResponseTests : OpenSearchTestCase() {
    @Test
    fun `test exec query-level monitor response`() {
        val req = ExecuteMonitorResponse(randomQueryLevelMonitorRunResult())
        Assert.assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = ExecuteMonitorResponse(sin)
        assertNotNull(newReq.monitorRunResult)
        assertEquals("test-monitor", newReq.monitorRunResult.monitorName)
        assertNotNull(newReq.monitorRunResult.inputResults)
    }

    @Test
    fun `test exec bucket-level monitor response`() {
        val req = ExecuteMonitorResponse(randomBucketLevelMonitorRunResult())
        Assert.assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = ExecuteMonitorResponse(sin)
        assertNotNull(newReq.monitorRunResult)
        assertEquals("test-monitor", newReq.monitorRunResult.monitorName)
        assertNotNull(newReq.monitorRunResult.inputResults)
    }
}
