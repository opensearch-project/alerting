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

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.rest.RestRequest
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.test.OpenSearchTestCase

class GetMonitorRequestTests : OpenSearchTestCase() {

    fun `test get monitor request`() {

        val req = GetMonitorRequest("1234", 1L, RestRequest.Method.GET, FetchSourceContext.FETCH_SOURCE)
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetMonitorRequest(sin)
        assertEquals("1234", newReq.monitorId)
        assertEquals(1L, newReq.version)
        assertEquals(RestRequest.Method.GET, newReq.method)
        assertEquals(FetchSourceContext.FETCH_SOURCE, newReq.srcContext)
    }

    fun `test get monitor request without src context`() {

        val req = GetMonitorRequest("1234", 1L, RestRequest.Method.GET, null)
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetMonitorRequest(sin)
        assertEquals("1234", newReq.monitorId)
        assertEquals(1L, newReq.version)
        assertEquals(RestRequest.Method.GET, newReq.method)
        assertEquals(null, newReq.srcContext)
    }

    fun `test head monitor request`() {

        val req = GetMonitorRequest("1234", 2L, RestRequest.Method.HEAD, FetchSourceContext.FETCH_SOURCE)
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetMonitorRequest(sin)
        assertEquals("1234", newReq.monitorId)
        assertEquals(2L, newReq.version)
        assertEquals(RestRequest.Method.HEAD, newReq.method)
        assertEquals(FetchSourceContext.FETCH_SOURCE, newReq.srcContext)
    }
}
