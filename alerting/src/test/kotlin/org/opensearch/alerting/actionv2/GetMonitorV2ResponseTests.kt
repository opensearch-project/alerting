package org.opensearch.alerting.actionv2

import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.randomPPLMonitor
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class GetMonitorV2ResponseTests : OpenSearchTestCase() {
    fun `test get monitor v2 response as stream`() {
        val req = GetMonitorV2Response(
            id = "abc",
            version = 2L,
            seqNo = 1L,
            primaryTerm = 2L,
            monitorV2 = randomPPLMonitor() as MonitorV2
        )
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetMonitorV2Response(sin)

        assertEquals(req.id, newReq.id)
        assertEquals(req.version, newReq.version)
        assertEquals(req.seqNo, newReq.seqNo)
        assertEquals(req.primaryTerm, newReq.primaryTerm)
        assertEquals(req.monitorV2, newReq.monitorV2)
    }
}
