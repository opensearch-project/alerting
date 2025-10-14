package org.opensearch.alerting.resthandler

import org.opensearch.alerting.AlertingPlugin.Companion.MONITOR_V2_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.core.modelv2.PPLMonitor
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.randomPPLMonitor
import org.opensearch.alerting.resthandler.MonitorRestApiIT.Companion.USE_TYPED_KEYS
import org.opensearch.client.ResponseException
import org.opensearch.common.UUIDs
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class MonitorV2RestApiIT : AlertingRestTestCase() {

    companion object {
        const val TIMESTAMP_FIELD = "timestamp"
        const val TEST_INDEX_NAME = "index"
        const val TEST_INDEX_MAPPINGS = """
            "properties":{"timestamp":{"type":"date"},"abc":{"type":"keyword"},"number":{"type":"integer"}}
        """
    }

    /* simple case tests */
    fun `test create monitor v2, get monitor v2, then parsing monitor v2 as a scheduled job`() {
        // this util function calls the create monitor API, then the get
        // monitor API so it can return the PPLMonitor object created.
        // the util function already contains assertions for the create
        // monitor flow. If its execution is successful, it means
        // it was also able to successfully get the monitor it created.
        // this test explicitly tests create monitor and get monitor
        // for completeness
        val monitorV2 = createRandomPPLMonitor()

        val builder = monitorV2.toXContentWithUser(XContentBuilder.builder(XContentType.JSON.xContent()), USE_TYPED_KEYS)
        val string = BytesReference.bytes(builder).utf8ToString()
        val xcp = createParser(XContentType.JSON.xContent(), string)
        val scheduledJob = ScheduledJob.parse(xcp, monitorV2.id, monitorV2.version)
        assertEquals(monitorV2, scheduledJob)
    }

    fun `test updating monitor v2`() {
        val originalMonitor = createRandomPPLMonitor()

        val newMonitor = randomPPLMonitor()

        val updateResponse = client().makeRequest(
            "PUT",
            "$MONITOR_V2_BASE_URI/${originalMonitor.id}",
            emptyMap(), newMonitor.toHttpEntity()
        )

        assertEquals("Update monitor failed", RestStatus.OK, updateResponse.restStatus())
        val responseBody = updateResponse.asMap()
        assertEquals("Updated monitor id doesn't match", originalMonitor.id, responseBody["_id"] as String)
        assertEquals("Version not incremented", (originalMonitor.version + 1).toInt(), responseBody["_version"] as Int)

        val updatedMonitor = getMonitorV2(originalMonitor.id) as PPLMonitor
        assertEquals("Monitor enabled field was not updated", newMonitor.enabled, updatedMonitor.enabled)
        assertEquals("Monitor schedule was not updated", newMonitor.schedule, updatedMonitor.schedule)
        assertEquals("Monitor lookback window was not updated", newMonitor.lookBackWindow, updatedMonitor.lookBackWindow)
        assertEquals("Monitor timestamp field was not updated", newMonitor.timestampField, updatedMonitor.timestampField)
        assertEquals("Monitor query language was not updated", newMonitor.queryLanguage, updatedMonitor.queryLanguage)
        assertEquals("Monitor query was not updated", newMonitor.query, updatedMonitor.query)
        assertEquals("Number of triggers in monitor was not updated", newMonitor.triggers.size, updatedMonitor.triggers.size)
        for (i in 0 until newMonitor.triggers.size) {
            assertEquals(
                "Monitor trigger ${newMonitor.triggers[i].id} name was not updated",
                newMonitor.triggers[i].name,
                updatedMonitor.triggers[i].name
            )
            assertEquals(
                "Monitor trigger ${newMonitor.triggers[i].id} severity was not updated",
                newMonitor.triggers[i].severity,
                updatedMonitor.triggers[i].severity
            )
            assertEquals(
                "Monitor trigger ${newMonitor.triggers[i].id} suppress duration was not updated",
                newMonitor.triggers[i].suppressDuration,
                updatedMonitor.triggers[i].suppressDuration
            )
            assertEquals(
                "Monitor trigger ${newMonitor.triggers[i].id} expire duration was not updated",
                newMonitor.triggers[i].expireDuration,
                updatedMonitor.triggers[i].expireDuration
            )
            assertEquals(
                "Monitor trigger ${newMonitor.triggers[i].id} mode was not updated",
                newMonitor.triggers[i].mode,
                updatedMonitor.triggers[i].mode
            )
            assertEquals(
                "Monitor trigger ${newMonitor.triggers[i].id} condition type was not updated",
                newMonitor.triggers[i].conditionType,
                updatedMonitor.triggers[i].conditionType
            )
            assertEquals(
                "Monitor trigger ${newMonitor.triggers[i].id} number_of_results condition was not updated",
                newMonitor.triggers[i].numResultsCondition,
                updatedMonitor.triggers[i].numResultsCondition
            )
            assertEquals(
                "Monitor trigger ${newMonitor.triggers[i].id} number_of_results value was not updated",
                newMonitor.triggers[i].numResultsValue,
                updatedMonitor.triggers[i].numResultsValue
            )
            assertEquals(
                "Monitor trigger ${newMonitor.triggers[i].id} custom condition was not updated",
                newMonitor.triggers[i].customCondition,
                updatedMonitor.triggers[i].customCondition
            )
        }
    }

    /* validation tests */
    fun `test update nonexistent monitor v2 fails`() {
        // the random monitor query searches index TEST_INDEX_NAME,
        // so we need to create that first to ensure at least the request body is valid
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)

        val monitorV2 = randomPPLMonitor()
        val randomId = UUIDs.base64UUID()

        try {
            client().makeRequest("PUT", "$MONITOR_V2_BASE_URI/$randomId", emptyMap(), monitorV2.toHttpEntity())
            fail("Expected request to fail with NOT_FOUND but it succeeded")
        } catch (e: ResponseException) {
            logger.info("response: ${e.response}")
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
