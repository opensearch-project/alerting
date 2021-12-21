/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.alerting.model.destination.Chime
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.randomUser
import org.opensearch.alerting.util.DestinationType
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant

class IndexDestinationResponseTests : OpenSearchTestCase() {

    fun `test index destination response`() {

        val req = IndexDestinationResponse(
            "1234", 0L, 1L, 2L, RestStatus.CREATED,
            Destination(
                "1234", 0L, 1, 1, 1, DestinationType.CHIME, "TestChimeDest",
                randomUser(), Instant.now(), Chime("test.com"), null, null, null
            )
        )
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = IndexDestinationResponse(sin)
        assertEquals("1234", newReq.id)
        assertEquals(0, newReq.version)
        assertEquals(1, newReq.seqNo)
        assertEquals(2, newReq.primaryTerm)
        assertEquals(RestStatus.CREATED, newReq.status)
        assertNotNull(newReq.destination)
        assertEquals("1234", newReq.destination.id)
    }
}
