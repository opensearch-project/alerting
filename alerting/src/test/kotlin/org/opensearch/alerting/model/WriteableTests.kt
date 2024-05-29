/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.model.destination.email.EmailGroup
import org.opensearch.alerting.randomEmailAccount
import org.opensearch.alerting.randomEmailGroup
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase

class WriteableTests : OpenSearchTestCase() {

    fun `test searchinput as stream`() {
        val input = SearchInput(emptyList(), SearchSourceBuilder())
        val out = BytesStreamOutput()
        input.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newInput = SearchInput(sin)
        assertEquals("Round tripping MonitorRunResult doesn't work", input, newInput)
    }

    fun `test emailaccount as stream`() {
        val emailAccount = randomEmailAccount()
        val out = BytesStreamOutput()
        emailAccount.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newEmailAccount = EmailAccount.readFrom(sin)
        assertEquals("Round tripping EmailAccount doesn't work", emailAccount, newEmailAccount)
    }

    fun `test emailgroup as stream`() {
        val emailGroup = randomEmailGroup()
        val out = BytesStreamOutput()
        emailGroup.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newEmailGroup = EmailGroup.readFrom(sin)
        assertEquals("Round tripping EmailGroup doesn't work", emailGroup, newEmailGroup)
    }
}
