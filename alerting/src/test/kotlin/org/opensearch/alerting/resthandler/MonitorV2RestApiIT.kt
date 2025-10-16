package org.opensearch.alerting.resthandler

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.opensearch.alerting.AlertingPlugin.Companion.MONITOR_V2_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.core.modelv2.MonitorV2
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
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class MonitorV2RestApiIT : AlertingRestTestCase() {

    companion object {
        const val TIMESTAMP_FIELD = "timestamp"
        const val TEST_INDEX_NAME = "index"
        const val TEST_INDEX_MAPPINGS =
            """"properties":{"timestamp":{"type":"date"},"abc":{"type":"keyword"},"number":{"type":"integer"}}"""
    }

    /* Simple Case Tests */
    fun `test create ppl monitor`() {
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        val pplMonitor = randomPPLMonitor()

        val response = client().makeRequest("POST", MONITOR_V2_BASE_URI, emptyMap(), pplMonitor.toHttpEntity())
        assertEquals("Unable to create a new monitor v2", RestStatus.OK, response.restStatus())

        val responseBody = response.asMap()
        logger.info("response body: $responseBody")
        val createdId = responseBody["_id"] as String
        val createdVersion = responseBody["_version"] as Int
        assertNotEquals("response is missing Id", MonitorV2.NO_ID, createdId)
        assertEquals("incorrect version", 1, createdVersion)
    }

    fun `test update ppl monitor`() {
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
        assertPplMonitorsEqual(newMonitor, updatedMonitor)
    }

    fun `test get ppl monitor`() {
        val submittedPplMonitor = createRandomPPLMonitor()
        val response = client().makeRequest("GET", "$MONITOR_V2_BASE_URI/${submittedPplMonitor.id}")
        assertEquals("Unable to get monitorV2 ${submittedPplMonitor.id}", RestStatus.OK, response.restStatus())

        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser)

        lateinit var id: String
        var version: Long = 0
        lateinit var storedPplMonitor: PPLMonitor

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                "_id" -> id = parser.text()
                "_version" -> version = parser.longValue()
                "monitorV2" -> storedPplMonitor = MonitorV2.parse(parser) as PPLMonitor
            }
        }

        assertEquals(
            "Monitor V2 ID from Get Monitor doesn't match one from Create Monitor response",
            submittedPplMonitor.id, id
        )
        assertEquals(
            "Monitor V2 version from Get Monitor doesn't match one from Create Monitor response",
            submittedPplMonitor.version, version
        )
        assertPplMonitorsEqual(submittedPplMonitor, storedPplMonitor)
    }

    fun `test search ppl monitor with GET`() {
        val pplMonitor = createRandomPPLMonitor()

        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", pplMonitor.id)).toString()
        val searchResponse = client().makeRequest(
            "GET", "$MONITOR_V2_BASE_URI/_search",
            emptyMap(), StringEntity(search, ContentType.APPLICATION_JSON)
        )

        assertEquals("Search monitor failed", RestStatus.OK, searchResponse.restStatus())
        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("PPL Monitor not found during search", 1, numberDocsFound)
    }

    fun `test search ppl monitor with POST`() {
        val pplMonitor = createRandomPPLMonitor()

        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", pplMonitor.id)).toString()
        val searchResponse = client().makeRequest(
            "POST", "$MONITOR_V2_BASE_URI/_search",
            emptyMap(), StringEntity(search, ContentType.APPLICATION_JSON)
        )

        assertEquals("Search monitor failed", RestStatus.OK, searchResponse.restStatus())
        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("PPL Monitor not found during search", 1, numberDocsFound)
    }

    fun `test delete ppl monitor`() {
        val pplMonitor = createRandomPPLMonitor()

        val deleteResponse = client().makeRequest("DELETE", "$MONITOR_V2_BASE_URI/${pplMonitor.id}")
        assertEquals("Delete failed", RestStatus.OK, deleteResponse.restStatus())

        val getResponse = client().makeRequest("HEAD", "$MONITOR_V2_BASE_URI/${pplMonitor.id}")
        assertEquals("Deleted monitor still exists", RestStatus.NOT_FOUND, getResponse.restStatus())
    }

    fun `test parsing ppl monitor as a scheduled job`() {
        val monitorV2 = createRandomPPLMonitor()

        val builder = monitorV2.toXContentWithUser(XContentBuilder.builder(XContentType.JSON.xContent()), USE_TYPED_KEYS)
        val string = BytesReference.bytes(builder).utf8ToString()
        val xcp = createParser(XContentType.JSON.xContent(), string)
        val scheduledJob = ScheduledJob.parse(xcp, monitorV2.id, monitorV2.version)
        assertEquals(monitorV2, scheduledJob)
    }

    /* Validation Tests */
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

    /* Utils */
    private fun assertPplMonitorsEqual(pplMonitor1: PPLMonitor, pplMonitor2: PPLMonitor) {
        assertEquals("Monitor enabled fields not equal", pplMonitor1.enabled, pplMonitor2.enabled)
        assertEquals("Monitor schedules not equal", pplMonitor1.schedule, pplMonitor2.schedule)
        assertEquals("Monitor lookback windows not equal", pplMonitor1.lookBackWindow, pplMonitor2.lookBackWindow)
        assertEquals("Monitor timestamp fields not equal", pplMonitor1.timestampField, pplMonitor2.timestampField)
        assertEquals("Monitor query languages not equal", pplMonitor1.queryLanguage, pplMonitor2.queryLanguage)
        assertEquals("Monitor queries not equal", pplMonitor1.query, pplMonitor2.query)
        assertEquals("Number of triggers in monitor not equal", pplMonitor1.triggers.size, pplMonitor2.triggers.size)

        val sortedTriggers1 = pplMonitor1.triggers.sortedBy { it.id }
        val sortedTriggers2 = pplMonitor2.triggers.sortedBy { it.id }
        for (i in sortedTriggers1.indices) {
            assertEquals(
                "Monitor trigger IDs not equal",
                sortedTriggers1[i].id,
                sortedTriggers2[i].id
            )

            val id = sortedTriggers1[i].id

            assertEquals(
                "Monitor trigger $id names not equal",
                sortedTriggers1[i].name,
                sortedTriggers2[i].name
            )
            assertEquals(
                "Monitor trigger $id severities not equal",
                sortedTriggers1[i].severity,
                sortedTriggers2[i].severity
            )
            assertEquals(
                "Monitor trigger $id suppress durations not equal",
                sortedTriggers1[i].suppressDuration,
                sortedTriggers2[i].suppressDuration
            )
            assertEquals(
                "Monitor trigger $id expire durations not equal",
                sortedTriggers1[i].expireDuration,
                sortedTriggers2[i].expireDuration
            )
            assertEquals(
                "Monitor trigger $id modes not equal",
                sortedTriggers1[i].mode,
                sortedTriggers2[i].mode
            )
            assertEquals(
                "Monitor trigger $id condition types not equal",
                sortedTriggers1[i].conditionType,
                sortedTriggers2[i].conditionType
            )
            assertEquals(
                "Monitor trigger $id number_of_results conditions not equal",
                sortedTriggers1[i].numResultsCondition,
                sortedTriggers2[i].numResultsCondition
            )
            assertEquals(
                "Monitor trigger $id number_of_results values not equal",
                sortedTriggers1[i].numResultsValue,
                sortedTriggers2[i].numResultsValue
            )
            assertEquals(
                "Monitor trigger $id custom conditions not equal",
                sortedTriggers1[i].customCondition,
                sortedTriggers2[i].customCondition
            )
        }
    }
}
