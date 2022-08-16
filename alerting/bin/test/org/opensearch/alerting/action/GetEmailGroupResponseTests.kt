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

class GetEmailGroupResponseTests : OpenSearchTestCase() {

    fun `test get email group response`() {

        val res = GetEmailGroupResponse("1234", 1L, 2L, 0L, RestStatus.OK, null)
        assertNotNull(res)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = GetEmailGroupResponse(sin)
        assertEquals("1234", newRes.id)
        assertEquals(1L, newRes.version)
        assertEquals(RestStatus.OK, newRes.status)
        assertEquals(null, newRes.emailGroup)
    }

    fun `test get email group with email group`() {

        val emailGroup = randomEmailGroup(name = "test-email-group")
        val res = GetEmailGroupResponse("1234", 1L, 2L, 0L, RestStatus.OK, emailGroup)
        assertNotNull(res)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = GetEmailGroupResponse(sin)
        assertEquals("1234", newRes.id)
        assertEquals(1L, newRes.version)
        assertEquals(RestStatus.OK, newRes.status)
        assertNotNull(newRes.emailGroup)
        assertEquals("test-email-group", newRes.emailGroup?.name)
    }
}
