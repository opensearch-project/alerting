/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.rest.RestRequest
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.test.OpenSearchTestCase

class GetEmailAccountRequestTests : OpenSearchTestCase() {

    fun `test get email account request`() {

        val req = GetEmailAccountRequest("1234", 1L, RestRequest.Method.GET, FetchSourceContext.FETCH_SOURCE)
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetEmailAccountRequest(sin)
        assertEquals("1234", newReq.emailAccountID)
        assertEquals(1L, newReq.version)
        assertEquals(RestRequest.Method.GET, newReq.method)
        assertEquals(FetchSourceContext.FETCH_SOURCE, newReq.srcContext)
    }

    fun `test head email account request`() {

        val req = GetEmailAccountRequest("1234", 2L, RestRequest.Method.HEAD, FetchSourceContext.FETCH_SOURCE)
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetEmailAccountRequest(sin)
        assertEquals("1234", newReq.emailAccountID)
        assertEquals(2L, newReq.version)
        assertEquals(RestRequest.Method.HEAD, newReq.method)
        assertEquals(FetchSourceContext.FETCH_SOURCE, newReq.srcContext)
    }
}
