/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.alerting.model.Table
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class GetAlertsRequestTests : OpenSearchTestCase() {

    fun `test get alerts request`() {

        val table = Table("asc", "sortString", null, 1, 0, "")

        val req = GetAlertsRequest(table, "1", "active", null, null)
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetAlertsRequest(sin)

        assertEquals("1", newReq.severityLevel)
        assertEquals("active", newReq.alertState)
        assertNull(newReq.monitorId)
        assertEquals(table, newReq.table)
    }

    fun `test get alerts request with filter`() {

        val table = Table("asc", "sortString", null, 1, 0, "")
        val req = GetAlertsRequest(table, "1", "active", null, null)
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetAlertsRequest(sin)

        assertEquals("1", newReq.severityLevel)
        assertEquals("active", newReq.alertState)
        assertNull(newReq.monitorId)
        assertEquals(table, newReq.table)
    }

    fun `test validate returns null`() {
        val table = Table("asc", "sortString", null, 1, 0, "")

        val req = GetAlertsRequest(table, "1", "active", null, null)
        assertNotNull(req)
        assertNull(req.validate())
    }
}
