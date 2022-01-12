/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.alerting.randomEmailAccount
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase

class GetEmailAccountResponseTests : OpenSearchTestCase() {

    fun `test get email account response`() {

        val res = GetEmailAccountResponse("1234", 1L, 2L, 0L, RestStatus.OK, null)
        assertNotNull(res)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = GetEmailAccountResponse(sin)
        assertEquals("1234", newRes.id)
        assertEquals(1L, newRes.version)
        assertEquals(RestStatus.OK, newRes.status)
        assertEquals(null, newRes.emailAccount)
    }

    fun `test get email account with email account`() {

        val emailAccount = randomEmailAccount(name = "test_email_account")
        val res = GetEmailAccountResponse("1234", 1L, 2L, 0L, RestStatus.OK, emailAccount)
        assertNotNull(res)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = GetEmailAccountResponse(sin)
        assertEquals("1234", newRes.id)
        assertEquals(1L, newRes.version)
        assertEquals(RestStatus.OK, newRes.status)
        assertNotNull(newRes.emailAccount)
        assertEquals("test_email_account", newRes.emailAccount?.name)
    }
}
