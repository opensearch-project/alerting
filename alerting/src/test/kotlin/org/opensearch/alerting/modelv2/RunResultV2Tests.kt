/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.modelv2

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class RunResultV2Tests : OpenSearchTestCase() {
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

    fun `test ppl sql monitor run result as monitor v2 run result as stream`() {
        val monitorRunResult = PPLSQLMonitorRunResult(
            monitorName = "some-monitor",
            error = IllegalArgumentException("some-error"),
            triggerResults = mapOf(
                "some-trigger-id" to PPLSQLTriggerRunResult(
                    triggerName = "some-trigger",
                    triggered = true,
                    error = IllegalArgumentException("some-error")
                )
            ),
            pplQueryResults = mapOf("some-result" to mapOf("some-field" to 2))
        ) as MonitorV2RunResult<PPLSQLTriggerRunResult>

        val out = BytesStreamOutput()
        MonitorV2RunResult.writeTo(out, monitorRunResult)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newMonitorRunResult = MonitorV2RunResult.readFrom(sin)
        assertEquals(monitorRunResult.monitorName, newMonitorRunResult.monitorName)
        assertEquals(monitorRunResult.error?.message, newMonitorRunResult.error?.message)
        assert(monitorRunResult.triggerResults.containsKey("some-trigger-id"))
        assert(newMonitorRunResult.triggerResults.containsKey("some-trigger-id"))
        assertEquals(
            monitorRunResult.triggerResults["some-trigger-id"]!!.triggerName,
            newMonitorRunResult.triggerResults["some-trigger-id"]!!.triggerName
        )
        assertEquals(
            monitorRunResult.triggerResults["some-trigger-id"]!!.triggered,
            newMonitorRunResult.triggerResults["some-trigger-id"]!!.triggered
        )
        assertEquals(
            monitorRunResult.triggerResults["some-trigger-id"]!!.error?.message,
            newMonitorRunResult.triggerResults["some-trigger-id"]!!.error?.message
        )
    }
}
