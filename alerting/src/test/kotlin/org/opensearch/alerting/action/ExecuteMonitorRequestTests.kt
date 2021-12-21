/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.unit.TimeValue
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase

class ExecuteMonitorRequestTests : OpenSearchTestCase() {

    fun `test execute monitor request with id`() {

        val req = ExecuteMonitorRequest(false, TimeValue.timeValueSeconds(100L), "1234", null)
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = ExecuteMonitorRequest(sin)
        assertEquals("1234", newReq.monitorId)
        assertEquals(false, newReq.dryrun)
        assertNull(newReq.monitor)
        assertEquals(req.monitor, newReq.monitor)
    }

    fun `test execute monitor request with monitor`() {
        val monitor = randomQueryLevelMonitor().copy(inputs = listOf(SearchInput(emptyList(), SearchSourceBuilder())))
        val req = ExecuteMonitorRequest(false, TimeValue.timeValueSeconds(100L), null, monitor)
        assertNotNull(req.monitor)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = ExecuteMonitorRequest(sin)
        assertNull(newReq.monitorId)
        assertEquals(false, newReq.dryrun)
        assertNotNull(newReq.monitor)
        assertEquals(req.monitor, newReq.monitor)
    }
}
