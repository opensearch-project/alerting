/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.support.WriteRequest
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class DeleteEmailGroupRequestTests : OpenSearchTestCase() {

    fun `test delete email group request`() {
        val req = DeleteEmailGroupRequest("1234", WriteRequest.RefreshPolicy.IMMEDIATE)
        assertNotNull(req)
        assertEquals("1234", req.emailGroupID)
        assertEquals("true", req.refreshPolicy.value)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = DeleteEmailGroupRequest(sin)
        assertEquals("1234", newReq.emailGroupID)
        assertEquals("true", newReq.refreshPolicy.value)
    }
}
