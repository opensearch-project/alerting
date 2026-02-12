/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.opensearch.alerting.AlertingPlugin.Companion.MONITOR_V2_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.TEST_INDEX_MAPPINGS
import org.opensearch.alerting.TEST_INDEX_NAME
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.randomPPLMonitor
import org.opensearch.client.ResponseException
import org.opensearch.common.UUIDs
import org.opensearch.common.settings.Settings
import org.opensearch.core.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging

/***
 * Tests Alerting V2 CRUD and validations
 *
 * Gradle command to run this suite:
 * ./gradlew :alerting:integTest -Dhttps=true -Dsecurity=true -Duser=admin -Dpassword=admin \
 * --tests "org.opensearch.alerting.resthandler.MonitorV2RestApiIT"
 */
@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class MonitorV2RestApiIT : AlertingRestTestCase() {

    /* Simple Case Tests */
    fun `test create ppl monitor`() {
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        val pplMonitor = randomPPLMonitor()

        val response = client().makeRequest("POST", MONITOR_V2_BASE_URI, emptyMap(), pplMonitor.toHttpEntity())
        assertEquals("Unable to create a new monitor v2", RestStatus.OK, response.restStatus())

        val responseBody = response.asMap()
        val createdId = responseBody["_id"] as String
        val createdVersion = responseBody["_version"] as Int
        assertNotEquals("response is missing Id", MonitorV2.NO_ID, createdId)
        assertEquals("incorrect version", 1, createdVersion)
    }

    /* Validation Tests */
    fun `test update nonexistent ppl monitor fails`() {
        // the random monitor query searches index TEST_INDEX_NAME,
        // so we need to create that first to ensure at least the request body is valid
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)

        val monitorV2 = randomPPLMonitor()
        val randomId = UUIDs.base64UUID()

        try {
            client().makeRequest("PUT", "$MONITOR_V2_BASE_URI/$randomId", emptyMap(), monitorV2.toHttpEntity())
            fail("Expected request to fail with NOT_FOUND but it succeeded")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
