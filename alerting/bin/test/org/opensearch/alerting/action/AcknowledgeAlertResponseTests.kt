/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.junit.Assert
import org.opensearch.alerting.alerts.AlertError
import org.opensearch.alerting.model.ActionExecutionResult
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.randomUser
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant

class AcknowledgeAlertResponseTests : OpenSearchTestCase() {

    fun `test acknowledge alert response`() {

        val acknowledged = mutableListOf(
            Alert(
                "1234", 0L, 1, "monitor-1234", "test-monitor", 0L, randomUser(),
                "trigger-14", "test-trigger", ArrayList(), ArrayList(), Alert.State.ACKNOWLEDGED,
                Instant.now(), Instant.now(), Instant.now(), Instant.now(), null, ArrayList(),
                "sev-2", ArrayList(), null
            )
        )
        val failed = mutableListOf(
            Alert(
                "1234", 0L, 1, "monitor-1234", "test-monitor", 0L, randomUser(),
                "trigger-14", "test-trigger", ArrayList(), ArrayList(), Alert.State.ERROR, Instant.now(), Instant.now(),
                Instant.now(), Instant.now(), null, mutableListOf(AlertError(Instant.now(), "Error msg")),
                "sev-2", mutableListOf(ActionExecutionResult("7890", null, 0)), null
            )
        )
        val missing = mutableListOf("1", "2", "3", "4")

        val req = AcknowledgeAlertResponse(acknowledged, failed, missing)
        Assert.assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = AcknowledgeAlertResponse(sin)
        Assert.assertEquals(1, newReq.acknowledged.size)
        Assert.assertEquals(1, newReq.failed.size)
        Assert.assertEquals(4, newReq.missing.size)
    }
}
