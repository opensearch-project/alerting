/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.alerting.assertPplMonitorsEqual
import org.opensearch.alerting.modelv2.PPLSQLMonitor
import org.opensearch.alerting.randomPPLMonitor
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class ExecuteMonitorV2RequestTests : OpenSearchTestCase() {
    fun `test execute monitor v2 request`() {
        val req = ExecuteMonitorV2Request(
            dryrun = true,
            manual = false,
            monitorV2Id = "abc",
            monitorV2 = randomPPLMonitor(),
            requestEnd = TimeValue.timeValueMinutes(30L)
        )
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = ExecuteMonitorV2Request(sin)

        assertEquals(req.dryrun, newReq.dryrun)
        assertEquals(req.manual, newReq.manual)
        assertEquals(req.monitorV2Id, newReq.monitorV2Id)
        assertPplMonitorsEqual(req.monitorV2 as PPLSQLMonitor, newReq.monitorV2 as PPLSQLMonitor)
        assertEquals(req.requestEnd, newReq.requestEnd)
    }
}
