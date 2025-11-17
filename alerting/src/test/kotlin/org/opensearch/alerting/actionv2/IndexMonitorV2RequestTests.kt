/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.randomPPLMonitor
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.rest.RestRequest
import org.opensearch.test.OpenSearchTestCase

class IndexMonitorV2RequestTests : OpenSearchTestCase() {
    fun `test index monitor v2 request as stream`() {
        val req = IndexMonitorV2Request(
            monitorId = "abc",
            seqNo = 1L,
            primaryTerm = 1L,
            refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE,
            method = RestRequest.Method.POST,
            monitorV2 = randomPPLMonitor() as MonitorV2,
            rbacRoles = listOf("role-a", "role-b")
        )
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = IndexMonitorV2Request(sin)

        assertEquals(req.monitorId, newReq.monitorId)
        assertEquals(req.seqNo, newReq.seqNo)
        assertEquals(req.primaryTerm, newReq.primaryTerm)
        assertEquals(req.refreshPolicy, newReq.refreshPolicy)
        assertEquals(req.method, newReq.method)
        assertEquals(req.monitorV2, newReq.monitorV2)
        assertEquals(req.rbacRoles, newReq.rbacRoles)
    }
}
