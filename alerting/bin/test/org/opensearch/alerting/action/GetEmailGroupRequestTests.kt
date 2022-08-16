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

class GetEmailGroupRequestTests : OpenSearchTestCase() {

    fun `test get email group request`() {

        val req = GetEmailGroupRequest("1234", 1L, RestRequest.Method.GET, FetchSourceContext.FETCH_SOURCE)
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetEmailGroupRequest(sin)
        assertEquals("1234", newReq.emailGroupID)
        assertEquals(1L, newReq.version)
        assertEquals(RestRequest.Method.GET, newReq.method)
        assertEquals(FetchSourceContext.FETCH_SOURCE, newReq.srcContext)
    }

    fun `test head email group request`() {

        val req = GetEmailGroupRequest("1234", 1L, RestRequest.Method.HEAD, FetchSourceContext.FETCH_SOURCE)
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetEmailGroupRequest(sin)
        assertEquals("1234", newReq.emailGroupID)
        assertEquals(1L, newReq.version)
        assertEquals(RestRequest.Method.HEAD, newReq.method)
        assertEquals(FetchSourceContext.FETCH_SOURCE, newReq.srcContext)
    }
}
