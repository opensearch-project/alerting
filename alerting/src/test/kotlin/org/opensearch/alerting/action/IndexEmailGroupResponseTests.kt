/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.alerting.randomEmailGroup
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase

class IndexEmailGroupResponseTests : OpenSearchTestCase() {

    fun `test index email group response with email group`() {

        val testEmailGroup = randomEmailGroup(name = "test-email-group")
        val res = IndexEmailGroupResponse("1234", 1L, 1L, 2L, RestStatus.OK, testEmailGroup)
        assertNotNull(res)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = IndexEmailGroupResponse(sin)
        assertEquals("1234", newRes.id)
        assertEquals(1L, newRes.version)
        assertEquals(1L, newRes.seqNo)
        assertEquals(RestStatus.OK, newRes.status)
        assertNotNull(newRes.emailGroup)
        assertEquals("test-email-group", newRes.emailGroup.name)
    }
}
