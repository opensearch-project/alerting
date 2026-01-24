/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.commons.alerting.action.GetFindingsRequest
import org.opensearch.commons.alerting.model.Table
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.Test

class GetFindingsRequestTests : OpenSearchTestCase() {
    @Test
    fun `test get findings request`() {
        val table = Table("asc", "sortString", null, 1, 0, "")

        val req =
            GetFindingsRequest(
                "2121",
                table,
                "1",
                "finding_index_name",
                boolQueryBuilder = BoolQueryBuilder(),
            )
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetFindingsRequest(sin)

        assertEquals("1", newReq.monitorId)
        assertEquals("2121", newReq.findingId)
        assertEquals("finding_index_name", newReq.findingIndex)
        assertEquals(table, newReq.table)
    }

    @Test
    fun `test validate returns null`() {
        val table = Table("asc", "sortString", null, 1, 0, "")

        val req = GetFindingsRequest("2121", table, "1", "active")
        assertNotNull(req)
        assertNull(req.validate())
    }
}
