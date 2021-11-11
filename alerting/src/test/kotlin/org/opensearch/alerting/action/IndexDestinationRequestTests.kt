/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.model.destination.Chime
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.randomUser
import org.opensearch.alerting.util.DestinationType
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.rest.RestRequest
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant

class IndexDestinationRequestTests : OpenSearchTestCase() {

    fun `test index destination post request`() {

        val req = IndexDestinationRequest(
            "1234",
            0L,
            1L,
            WriteRequest.RefreshPolicy.IMMEDIATE,
            RestRequest.Method.POST,
            Destination(
                "1234",
                0L,
                1,
                1,
                1,
                DestinationType.CHIME,
                "TestChimeDest",
                randomUser(),
                Instant.now(),
                Chime("test.com"),
                null,
                null,
                null
            )
        )
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = IndexDestinationRequest(sin)
        assertEquals("1234", newReq.destinationId)
        assertEquals(0, newReq.seqNo)
        assertEquals(1, newReq.primaryTerm)
        assertEquals("true", newReq.refreshPolicy.value)
        assertEquals(RestRequest.Method.POST, newReq.method)
        assertNotNull(newReq.destination)
        assertEquals("1234", newReq.destination.id)
    }

    fun `test index destination put request`() {

        val req = IndexDestinationRequest(
            "1234",
            0L,
            1L,
            WriteRequest.RefreshPolicy.IMMEDIATE,
            RestRequest.Method.PUT,
            Destination(
                "1234",
                0L,
                1,
                1,
                1,
                DestinationType.CHIME,
                "TestChimeDest",
                randomUser(),
                Instant.now(),
                Chime("test.com"),
                null,
                null,
                null
            )
        )
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = IndexDestinationRequest(sin)
        assertEquals("1234", newReq.destinationId)
        assertEquals(0, newReq.seqNo)
        assertEquals(1, newReq.primaryTerm)
        assertEquals("true", newReq.refreshPolicy.value)
        assertEquals(RestRequest.Method.PUT, newReq.method)
        assertNotNull(newReq.destination)
        assertEquals("1234", newReq.destination.id)
    }
}
