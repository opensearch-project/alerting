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

import org.junit.Assert
import org.opensearch.alerting.builder
import org.opensearch.alerting.elasticapi.string
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.randomUser
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant
import java.util.Collections

class GetAlertsResponseTests : OpenSearchTestCase() {

    fun `test get alerts response with no alerts`() {
        val req = GetAlertsResponse(Collections.emptyList(), 0)
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetAlertsResponse(sin)
        Assert.assertTrue(newReq.alerts.isEmpty())
        assertEquals(0, newReq.totalAlerts)
    }

    fun `test get alerts response with alerts`() {
        val alert = Alert(
            "id",
            0L,
            0,
            "monitorId",
            "monitorName",
            0L,
            randomUser(),
            "triggerId",
            "triggerName",
            Alert.State.ACKNOWLEDGED,
            Instant.MIN,
            null,
            null,
            null,
            null,
            Collections.emptyList(),
            "severity",
            Collections.emptyList(),
            null
        )
        val req = GetAlertsResponse(listOf(alert), 1)
        assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetAlertsResponse(sin)
        assertEquals(1, newReq.alerts.size)
        assertEquals(alert, newReq.alerts[0])
        assertEquals(1, newReq.totalAlerts)
    }

    fun `test toXContent for get alerts response`() {
        val now = Instant.now()

        val alert = Alert(
            "id",
            0L,
            0,
            "monitorId",
            "monitorName",
            0L,
            null,
            "triggerId",
            "triggerName",
            Alert.State.ACKNOWLEDGED,
            now,
            null,
            null,
            null,
            null,
            Collections.emptyList(),
            "severity",
            Collections.emptyList(),
            null
        )
        val req = GetAlertsResponse(listOf(alert), 1)
        var actualXContentString = req.toXContent(builder(), ToXContent.EMPTY_PARAMS).string()
        val expectedXContentString = "{\"alerts\":[{\"id\":\"id\",\"version\":0,\"monitor_id\":\"monitorId\"," +
            "\"schema_version\":0,\"monitor_version\":0,\"monitor_name\":\"monitorName\"," +
            "\"trigger_id\":\"triggerId\"," +
            "\"trigger_name\":\"triggerName\",\"state\":\"ACKNOWLEDGED\",\"error_message\":null,\"alert_history\":[]," +
            "\"severity\":\"severity\",\"action_execution_results\":[],\"start_time\":" + now.toEpochMilli() +
            ",\"last_notification_time\":null,\"end_time\":null,\"acknowledged_time\":null}],\"totalAlerts\":1}"
        assertEquals(expectedXContentString, actualXContentString)
    }
}
