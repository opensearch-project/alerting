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

class IndexEmailAccountResponseTests : OpenSearchTestCase() {

    fun `test index email account response with email account`() {

        val testEmailAccount = randomEmailAccount(name = "test_email_account")
        val res = IndexEmailAccountResponse("1234", 1L, 1L, 2L, RestStatus.OK, testEmailAccount)
        assertNotNull(res)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = IndexEmailAccountResponse(sin)
        assertEquals("1234", newRes.id)
        assertEquals(1L, newRes.version)
        assertEquals(1L, newRes.seqNo)
        assertEquals(RestStatus.OK, newRes.status)
        assertNotNull(newRes.emailAccount)
        assertEquals("test_email_account", newRes.emailAccount.name)
    }
}
