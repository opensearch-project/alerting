/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.alerting.modelv2.MonitorV2RunResult
import org.opensearch.alerting.modelv2.PPLSQLMonitorRunResult
import org.opensearch.alerting.modelv2.PPLSQLTriggerRunResult
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class ExecuteMonitorV2ResponseTests : OpenSearchTestCase() {
    // fails because trigger writeMap fails
    fun `test execute monitor response`() {
        val monitorRunResult = PPLSQLMonitorRunResult(
            monitorName = "some-monitor",
            error = IllegalArgumentException("some-error"),
            triggerResults = mapOf(
                "some-trigger" to PPLSQLTriggerRunResult(
                    triggerName = "some-trigger",
                    triggered = true,
                    error = IllegalArgumentException("some-error")
                )
            ),
            pplQueryResults = mapOf("some-result" to mapOf("some-field" to "some-value"))
        ) as MonitorV2RunResult<PPLSQLTriggerRunResult>
        val response = ExecuteMonitorV2Response(monitorRunResult)
        assertNotNull(response)

        val out = BytesStreamOutput()
        response.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newResponse = ExecuteMonitorV2Response(sin)

        assertEquals(response.monitorV2RunResult.monitorName, newResponse.monitorV2RunResult.monitorName)
        assertEquals(response.monitorV2RunResult.error, newResponse.monitorV2RunResult.error)
        assertEquals(response.monitorV2RunResult.triggerResults, newResponse.monitorV2RunResult.triggerResults)
    }

    // succeeds
    fun `test ppl sql trigger run result as stream`() {
        val runResult = PPLSQLTriggerRunResult(
            triggerName = "some-trigger",
            triggered = true,
            error = IllegalArgumentException("some-error")
        )
        val out = BytesStreamOutput()
        runResult.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRunResult = PPLSQLTriggerRunResult(sin)
        assertEquals(runResult.triggerName, newRunResult.triggerName)
    }

    // fails on call to wroteMap()
    fun `test writeMap with PPLSQLTriggerRunResult directly`() {
        val trigger = PPLSQLTriggerRunResult(
            triggerName = "test",
            triggered = true,
            error = null
        )

        val map = mapOf("trigger-1" to trigger)
        val out = BytesStreamOutput()

        out.writeMap(map)
    }

    // fails because trigger writeMap fails
    fun `test ppl sql monitor run result as stream`() {
        val monitorRunResult = PPLSQLMonitorRunResult(
            monitorName = "some-monitor",
            error = IllegalArgumentException("some-error"),
            triggerResults = mapOf(
                "some-trigger" to PPLSQLTriggerRunResult(
                    triggerName = "some-trigger",
                    triggered = true,
                    error = IllegalArgumentException("some-error")
                )
            ),
            pplQueryResults = mapOf("some-result" to mapOf("some-field" to "some-value"))
        ) as MonitorV2RunResult<PPLSQLTriggerRunResult>

        val out = BytesStreamOutput()
        MonitorV2RunResult.writeTo(out, monitorRunResult)
//        monitorRunResult.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newMonitorRunResult = MonitorV2RunResult.readFrom(sin)
        assertEquals(monitorRunResult, newMonitorRunResult)
    }
}
