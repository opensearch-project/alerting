/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.rest.RestRequest
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase

class IndexMonitorRequestTests : OpenSearchTestCase() {

    fun `test index monitor post request`() {

        val req = IndexMonitorRequest(
            "1234", 1L, 2L, WriteRequest.RefreshPolicy.IMMEDIATE, RestRequest.Method.POST,
            randomQueryLevelMonitor().copy(inputs = listOf(SearchInput(emptyList(), SearchSourceBuilder())))
        )
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = IndexMonitorRequest(sin)
        assertEquals("1234", newReq.monitorId)
        assertEquals(1L, newReq.seqNo)
        assertEquals(2L, newReq.primaryTerm)
        assertEquals(RestRequest.Method.POST, newReq.method)
        assertNotNull(newReq.monitor)
    }

    fun `test index monitor put request`() {

        val req = IndexMonitorRequest(
            "1234", 1L, 2L, WriteRequest.RefreshPolicy.IMMEDIATE, RestRequest.Method.PUT,
            randomQueryLevelMonitor().copy(inputs = listOf(SearchInput(emptyList(), SearchSourceBuilder())))
        )
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = IndexMonitorRequest(sin)
        assertEquals("1234", newReq.monitorId)
        assertEquals(1L, newReq.seqNo)
        assertEquals(2L, newReq.primaryTerm)
        assertEquals(RestRequest.Method.PUT, newReq.method)
        assertNotNull(newReq.monitor)
    }
}
