/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.alerting.modelv2.PPLSQLMonitorRunResult
import org.opensearch.alerting.modelv2.PPLSQLTriggerRunResult
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class ExecuteMonitorV2ResponseTests : OpenSearchTestCase() {
    fun `test execute monitor response`() {
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
            pplQueryResults = mapOf("some-result" to mapOf("some-field" to 3))
        )
        val response = ExecuteMonitorV2Response(monitorRunResult)
        assertNotNull(response)

        val out = BytesStreamOutput()
        response.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newResponse = ExecuteMonitorV2Response(sin)

        assertEquals(response.monitorV2RunResult.monitorName, newResponse.monitorV2RunResult.monitorName)
        assertEquals(response.monitorV2RunResult.error?.message, newResponse.monitorV2RunResult.error?.message)
        assert(response.monitorV2RunResult.triggerResults.containsKey("some-trigger-id"))
        assert(newResponse.monitorV2RunResult.triggerResults.containsKey("some-trigger-id"))
        assertEquals(
            response.monitorV2RunResult.triggerResults["some-trigger-id"]!!.triggerName,
            newResponse.monitorV2RunResult.triggerResults["some-trigger-id"]!!.triggerName
        )
        assertEquals(
            response.monitorV2RunResult.triggerResults["some-trigger-id"]!!.triggered,
            newResponse.monitorV2RunResult.triggerResults["some-trigger-id"]!!.triggered
        )
        assertEquals(
            response.monitorV2RunResult.triggerResults["some-trigger-id"]!!.error?.message,
            newResponse.monitorV2RunResult.triggerResults["some-trigger-id"]!!.error?.message
        )
        assertEquals(
            (response.monitorV2RunResult as PPLSQLMonitorRunResult).pplQueryResults,
            (newResponse.monitorV2RunResult as PPLSQLMonitorRunResult).pplQueryResults
        )
    }
}
