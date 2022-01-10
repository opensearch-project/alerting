/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.alerting.model.Table
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.test.OpenSearchTestCase

class GetDestinationsRequestTests : OpenSearchTestCase() {

    fun `test get destination request`() {

        val table = Table("asc", "sortString", null, 1, 0, "")
        val req = GetDestinationsRequest("1234", 1L, FetchSourceContext.FETCH_SOURCE, table, "slack")
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetDestinationsRequest(sin)
        assertEquals("1234", newReq.destinationId)
        assertEquals(1L, newReq.version)
        assertEquals(FetchSourceContext.FETCH_SOURCE, newReq.srcContext)
        assertEquals(table, newReq.table)
        assertEquals("slack", newReq.destinationType)
    }

    fun `test get destination request without src context`() {

        val table = Table("asc", "sortString", null, 1, 0, "")
        val req = GetDestinationsRequest("1234", 1L, null, table, "slack")
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetDestinationsRequest(sin)
        assertEquals("1234", newReq.destinationId)
        assertEquals(1L, newReq.version)
        assertEquals(null, newReq.srcContext)
        assertEquals(table, newReq.table)
        assertEquals("slack", newReq.destinationType)
    }

    fun `test get destination request without destinationId`() {

        val table = Table("asc", "sortString", null, 1, 0, "")
        val req = GetDestinationsRequest(null, 1L, FetchSourceContext.FETCH_SOURCE, table, "slack")
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetDestinationsRequest(sin)
        assertEquals(null, newReq.destinationId)
        assertEquals(1L, newReq.version)
        assertEquals(FetchSourceContext.FETCH_SOURCE, newReq.srcContext)
        assertEquals(table, newReq.table)
        assertEquals("slack", newReq.destinationType)
    }

    fun `test get destination request with filter`() {

        val table = Table("asc", "sortString", null, 1, 0, "")
        val req = GetDestinationsRequest(null, 1L, FetchSourceContext.FETCH_SOURCE, table, "slack")
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetDestinationsRequest(sin)
        assertEquals(null, newReq.destinationId)
        assertEquals(1L, newReq.version)
        assertEquals(FetchSourceContext.FETCH_SOURCE, newReq.srcContext)
        assertEquals(table, newReq.table)
        assertEquals("slack", newReq.destinationType)
    }
}
