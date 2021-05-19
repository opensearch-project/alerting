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

import org.opensearch.action.search.SearchRequest
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.unit.TimeValue
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.rest.OpenSearchRestTestCase
import java.util.concurrent.TimeUnit

class SearchMonitorRequestTests : OpenSearchTestCase() {

    fun `test search monitors request`() {
        val searchSourceBuilder = SearchSourceBuilder().from(0).size(100).timeout(TimeValue(60, TimeUnit.SECONDS))
        val searchRequest = SearchRequest().indices(OpenSearchRestTestCase.randomAlphaOfLength(10)).source(searchSourceBuilder)
        val searchMonitorRequest = SearchMonitorRequest(searchRequest)
        assertNotNull(searchMonitorRequest)

        val out = BytesStreamOutput()
        searchMonitorRequest.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = SearchMonitorRequest(sin)

        assertNotNull(newReq.searchRequest)
        assertEquals(1, newReq.searchRequest.indices().size)
    }
}
