/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.junit.Assert
import org.opensearch.action.support.WriteRequest
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class AcknowledgeAlertRequestTests : OpenSearchTestCase() {

    fun `test acknowledge alert request`() {
        val req = AcknowledgeAlertRequest("1234", mutableListOf("1", "2", "3", "4"), WriteRequest.RefreshPolicy.IMMEDIATE)
        Assert.assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = AcknowledgeAlertRequest(sin)
        Assert.assertEquals("1234", newReq.monitorId)
        Assert.assertEquals(4, newReq.alertIds.size)
        Assert.assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, newReq.refreshPolicy)
    }
}
