/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.alerting.modelv2.AlertV2
import org.opensearch.alerting.modelv2.TriggerV2
import org.opensearch.alerting.randomUser
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.util.string
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant
import java.util.Collections

class GetAlertsV2ResponseTests : OpenSearchTestCase() {
    fun `test get alerts response with no alerts`() {
        val req = GetAlertsV2Response(Collections.emptyList(), 0)
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetAlertsV2Response(sin)
        assertTrue(newReq.alertV2s.isEmpty())
        assertEquals(0, newReq.totalAlertV2s)
    }

    fun `test get alerts response with alerts`() {
        val alert = AlertV2(
            monitorId = "id",
            monitorName = "name",
            monitorVersion = AlertV2.NO_VERSION,
            monitorUser = randomUser(),
            triggerId = "triggerId",
            triggerName = "triggerNamer",
            query = "source = some_index",
            queryResults = mapOf(),
            triggeredTime = Instant.now(),
            errorMessage = null,
            severity = TriggerV2.Severity.LOW,
            executionId = "executionId"
        )
        val res = GetAlertsV2Response(listOf(alert), 1)
        assertNotNull(res)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = GetAlertsV2Response(sin)
        assertEquals(1, newRes.alertV2s.size)
        assertEquals(alert, newRes.alertV2s[0])
        assertEquals(1, newRes.totalAlertV2s)
    }

    fun `test toXContent for get alerts response`() {
        val now = Instant.now()
        val alert = AlertV2(
            monitorId = "id",
            monitorName = "name",
            monitorVersion = AlertV2.NO_VERSION,
            monitorUser = randomUser(),
            triggerId = "triggerId",
            triggerName = "triggerName",
            query = "source = some_index",
            queryResults = mapOf(),
            triggeredTime = now,
            errorMessage = null,
            severity = TriggerV2.Severity.LOW,
            executionId = "executionId"
        )

        val req = GetAlertsV2Response(listOf(alert), 1)
        var actualXContentString = req.toXContent(
            XContentBuilder.builder(XContentType.JSON.xContent()),
            ToXContent.EMPTY_PARAMS
        ).string()
        val expectedXContentString = "{\"alerts_v2\":[{\"id\":\"\",\"version\":-1,\"monitor_v2_id\":\"id\",\"schema_version\":0," +
            "\"monitor_v2_version\":-1,\"monitor_v2_name\":\"name\",\"execution_id\":\"executionId\",\"trigger_v2_id\":\"triggerId\"," +
            "\"trigger_v2_name\":\"triggerName\",\"query\":\"source = some_index\",\"query_results\":{},\"error_message\":null," +
            "\"severity\":\"low\",\"triggered_time\":${now.toEpochMilli()}}],\"total_alerts_v2\":1}"

        logger.info("expected: $expectedXContentString")
        logger.info("actual: $actualXContentString")

        assertEquals(expectedXContentString, actualXContentString)
    }
}
