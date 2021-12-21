/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.randomEmailAccount
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.rest.RestRequest
import org.opensearch.test.OpenSearchTestCase

class IndexEmailAccountRequestTests : OpenSearchTestCase() {

    fun `test index email account post request`() {

        val req = IndexEmailAccountRequest(
            "1234", 1L, 2L, WriteRequest.RefreshPolicy.IMMEDIATE, RestRequest.Method.POST,
            randomEmailAccount()
        )
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = IndexEmailAccountRequest(sin)
        assertEquals("1234", newReq.emailAccountID)
        assertEquals(1L, newReq.seqNo)
        assertEquals(2L, newReq.primaryTerm)
        assertEquals(RestRequest.Method.POST, newReq.method)
        assertNotNull(newReq.emailAccount)
    }

    fun `test index email account put request`() {

        val req = IndexEmailAccountRequest(
            "1234", 1L, 2L, WriteRequest.RefreshPolicy.IMMEDIATE, RestRequest.Method.PUT,
            randomEmailAccount()
        )
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = IndexEmailAccountRequest(sin)
        assertEquals("1234", newReq.emailAccountID)
        assertEquals(1L, newReq.seqNo)
        assertEquals(2L, newReq.primaryTerm)
        assertEquals(RestRequest.Method.PUT, newReq.method)
        assertNotNull(newReq.emailAccount)
    }
}
