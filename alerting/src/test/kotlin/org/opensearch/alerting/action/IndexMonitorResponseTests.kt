/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.alerting.core.model.CronSchedule
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.randomUser
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant
import java.time.ZoneId

class IndexMonitorResponseTests : OpenSearchTestCase() {

    fun `test index monitor response with monitor`() {
        val cronExpression = "31 * * * *" // Run at minute 31.
        val testInstance = Instant.ofEpochSecond(1538164858L)

        val cronSchedule = CronSchedule(cronExpression, ZoneId.of("Asia/Kolkata"), testInstance)
        val monitor = Monitor(
            id = "123",
            version = 0L,
            name = "test-monitor",
            enabled = true,
            schedule = cronSchedule,
            lastUpdateTime = Instant.now(),
            enabledTime = Instant.now(),
            monitorType = Monitor.MonitorType.QUERY_LEVEL_MONITOR,
            user = randomUser(),
            schemaVersion = 0,
            inputs = mutableListOf(),
            triggers = mutableListOf(),
            uiMetadata = mutableMapOf()
        )
        val req = IndexMonitorResponse("1234", 1L, 2L, 0L, RestStatus.OK, monitor)
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = IndexMonitorResponse(sin)
        assertEquals("1234", newReq.id)
        assertEquals(1L, newReq.version)
        assertEquals(RestStatus.OK, newReq.status)
        assertNotNull(newReq.monitor)
    }
}
