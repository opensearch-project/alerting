/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class DeleteMonitorV2ResponseTests : OpenSearchTestCase() {
    fun `test get monitor v2 request as stream`() {
        val req = DeleteMonitorV2Response(
            id = "abc",
            version = 3L
        )
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = DeleteMonitorV2Response(sin)

        assertEquals(req.id, newReq.id)
        assertEquals(req.version, newReq.version)
    }
}
