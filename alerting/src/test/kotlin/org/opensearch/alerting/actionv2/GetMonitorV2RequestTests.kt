/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.test.OpenSearchTestCase

class GetMonitorV2RequestTests : OpenSearchTestCase() {
    fun `test get monitor v2 request as stream`() {
        val req = GetMonitorV2Request(
            monitorV2Id = "abc",
            version = 2L,
            srcContext = FetchSourceContext.FETCH_SOURCE
        )
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetMonitorV2Request(sin)

        assertEquals(req.monitorV2Id, newReq.monitorV2Id)
        assertEquals(req.version, newReq.version)
        assertEquals(req.srcContext, newReq.srcContext)
    }
}
