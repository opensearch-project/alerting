/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.support.WriteRequest
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class DeleteEmailAccountRequestTests : OpenSearchTestCase() {

    fun `test delete email account request`() {
        val req = DeleteEmailAccountRequest("1234", WriteRequest.RefreshPolicy.IMMEDIATE)
        assertNotNull(req)
        assertEquals("1234", req.emailAccountID)
        assertEquals("true", req.refreshPolicy.value)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = DeleteEmailAccountRequest(sin)
        assertEquals("1234", newReq.emailAccountID)
        assertEquals("true", newReq.refreshPolicy.value)
    }
}
