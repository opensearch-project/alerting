/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.commons.alerting.model.Table
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class GetAlertsV2RequestTests : OpenSearchTestCase() {
    fun `test get alerts request as stream`() {
        val table = Table("asc", "sortString", null, 1, 0, "")

        val req = GetAlertsV2Request(
            table = table,
            severityLevel = "1",
            monitorV2Ids = listOf("1", "2"),
        )
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetAlertsV2Request(sin)

        assertEquals("1", newReq.severityLevel)
        assertEquals(table, newReq.table)
        assertTrue(newReq.monitorV2Ids!!.contains("1"))
        assertTrue(newReq.monitorV2Ids!!.contains("2"))
    }

    fun `test get alerts request with filter as stream`() {
        val table = Table("asc", "sortString", null, 1, 0, "")
        val req = GetAlertsV2Request(table, "1", null)
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetAlertsV2Request(sin)

        assertEquals("1", newReq.severityLevel)
        assertNull(newReq.monitorV2Ids)
        assertEquals(table, newReq.table)
    }
}
