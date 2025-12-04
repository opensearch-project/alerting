/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.action.support.WriteRequest
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.Test

class DeleteMonitorV2RequestTests : OpenSearchTestCase() {
    @Test
    fun `test get monitor v2 request as stream`() {
        val req =
            DeleteMonitorV2Request(
                monitorV2Id = "abc",
                refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE,
            )
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = DeleteMonitorV2Request(sin)

        assertEquals(req.monitorV2Id, newReq.monitorV2Id)
        assertEquals(req.refreshPolicy, newReq.refreshPolicy)
    }
}
