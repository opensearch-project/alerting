/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.alerting.action

import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.model.destination.Slack
import org.opensearch.alerting.util.DestinationType
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant
import java.util.Collections

class GetDestinationsResponseTests : OpenSearchTestCase() {

    fun `test get destination response with no destinations`() {
        val req = GetDestinationsResponse(RestStatus.OK, 0, Collections.emptyList())
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetDestinationsResponse(sin)
        assertEquals(0, newReq.totalDestinations)
        assertTrue(newReq.destinations.isEmpty())
        assertEquals(RestStatus.OK, newReq.status)
    }

    fun `test get destination response with a destination`() {
        val slack = Slack("url")
        val destination = Destination(
                "id",
                0L,
                0,
                0,
                0,
                DestinationType.SLACK,
                "name",
                null,
                Instant.MIN,
                null,
                slack,
                null,
                null)

        val req = GetDestinationsResponse(RestStatus.OK, 1, listOf(destination))
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetDestinationsResponse(sin)
        assertEquals(1, newReq.totalDestinations)
        assertEquals(destination, newReq.destinations[0])
        assertEquals(RestStatus.OK, newReq.status)
    }
}
