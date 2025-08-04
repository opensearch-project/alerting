/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.junit.Before
import org.opensearch.alerting.AlertingPlugin.Companion.MONITOR_V2_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.TEST_INDEX_MAPPINGS
import org.opensearch.alerting.TEST_INDEX_NAME
import org.opensearch.alerting.assertPplMonitorsEqual
import org.opensearch.alerting.core.settings.AlertingV2Settings
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.modelv2.PPLSQLMonitor
import org.opensearch.alerting.modelv2.PPLSQLTrigger.ConditionType
import org.opensearch.alerting.randomAction
import org.opensearch.alerting.randomPPLMonitor
import org.opensearch.alerting.randomPPLTrigger
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.alerting.randomTemplateScript
import org.opensearch.alerting.resthandler.MonitorRestApiIT.Companion.USE_TYPED_KEYS
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_V2_MAX_EXPIRE_DURATION
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_V2_MAX_LOOK_BACK_WINDOW
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_V2_MAX_MONITORS
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_V2_MAX_QUERY_LENGTH
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_V2_MAX_THROTTLE_DURATION
import org.opensearch.alerting.settings.AlertingSettings.Companion.NOTIFICATION_MESSAGE_SOURCE_MAX_LENGTH
import org.opensearch.alerting.settings.AlertingSettings.Companion.NOTIFICATION_SUBJECT_SOURCE_MAX_LENGTH
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
import java.time.temporal.ChronoUnit.MINUTES

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
    @Before
    fun enableAlertingV2() {
        client().updateSettings(AlertingV2Settings.ALERTING_V2_ENABLED.key, "true")
    }

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

    fun `test update ppl monitor`() {
        val originalMonitor = createRandomPPLMonitor()

        val newMonitorConfig = randomPPLMonitor()

        val updateResponse = client().makeRequest(
            "PUT",
            "$MONITOR_V2_BASE_URI/${originalMonitor.id}",
            emptyMap(), newMonitorConfig.toHttpEntity()
        )

        assertEquals("Update monitor failed", RestStatus.OK, updateResponse.restStatus())
        val responseBody = updateResponse.asMap()
        assertEquals("Updated monitor id doesn't match", originalMonitor.id, responseBody["_id"] as String)
        assertEquals("Version not incremented", (originalMonitor.version + 1).toInt(), responseBody["_version"] as Int)

        val updatedMonitor = getMonitorV2(originalMonitor.id) as PPLSQLMonitor
        assertPplMonitorsEqual(newMonitorConfig, updatedMonitor)
    }

    fun `test get ppl monitor`() {
        // first create the monitor
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        val pplMonitor = randomPPLMonitor()

        val createResponse = client().makeRequest("POST", MONITOR_V2_BASE_URI, emptyMap(), pplMonitor.toHttpEntity())
        assertEquals("Unable to create a new monitor v2", RestStatus.OK, createResponse.restStatus())

        val responseBody = createResponse.asMap()
        val pplMonitorId = responseBody["_id"] as String
        val pplMonitorVersion = (responseBody["_version"] as Int).toLong()

        // then attempt to get it
        val response = client().makeRequest("GET", "$MONITOR_V2_BASE_URI/$pplMonitorId")
        assertEquals("Unable to get monitorV2 $pplMonitorId", RestStatus.OK, response.restStatus())

        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser)

        lateinit var id: String
        var version: Long = 0
        lateinit var storedPplMonitor: PPLSQLMonitor

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                "_id" -> id = parser.text()
                "_version" -> version = parser.longValue()
                "monitorV2" -> storedPplMonitor = MonitorV2.parse(parser) as PPLSQLMonitor
            }
        }

        assertEquals(
            "Monitor V2 ID from Get Monitor doesn't match one from Create Monitor response",
            pplMonitorId, id
        )
        assertEquals(
            "Monitor V2 version from Get Monitor doesn't match one from Create Monitor response",
            pplMonitorVersion, version
        )
        assertPplMonitorsEqual(pplMonitor, storedPplMonitor)
    }

    fun `test head ppl monitor`() {
        val submittedPplMonitor = createRandomPPLMonitor()
        val response = client().makeRequest("HEAD", "$MONITOR_V2_BASE_URI/${submittedPplMonitor.id}")
        assertEquals("Unable to get monitorV2 ${submittedPplMonitor.id}", RestStatus.NO_CONTENT, response.restStatus())
    }

    fun `test search ppl monitor with GET and match_all`() {
        createRandomPPLMonitor()

        val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()
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

    fun `test search ppl monitor with POST and term query on ID`() {
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
    fun `test create ppl monitor that queries nonexistent index fails`() {
        val pplMonitorConfig = randomPPLMonitor(
            query = "source = nonexistent_index | head 10"
        )

        // ensure the request fails
        try {
            createRandomPPLMonitor(pplMonitorConfig)
            fail("Expected request to fail with BAD_REQUEST but it succeeded")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // ensure no monitor was created
        ensureNumMonitorV2s(0)
    }

    fun `test create ppl monitor with more than max allowed monitors fails`() {
        adminClient().updateSettings(ALERTING_V2_MAX_MONITORS.key, 1)

        createRandomPPLMonitor()

        // ensure the request fails
        try {
            createRandomPPLMonitor()
            fail("Expected request to fail with BAD_REQUEST but it succeeded")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // ensure no monitor was created
        ensureNumMonitorV2s(1)
    }

    fun `test create ppl monitor with throttle greater than max fails`() {
        val maxThrottleDuration = 60L
        client().updateSettings(ALERTING_V2_MAX_THROTTLE_DURATION.key, maxThrottleDuration)

        // ensure the request fails
        try {
            createRandomPPLMonitor(
                randomPPLMonitor(
                    triggers = listOf(
                        randomPPLTrigger(throttleDuration = maxThrottleDuration + 10)
                    )
                )
            )
            fail("Expected request to fail with BAD_REQUEST but it succeeded")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // ensure no monitor was created
        ensureNumMonitorV2s(0)
    }

    fun `test create ppl monitor with expire greater than max fails`() {
        val maxExpireDuration = 60L
        client().updateSettings(ALERTING_V2_MAX_EXPIRE_DURATION.key, maxExpireDuration)

        // ensure the request fails
        try {
            createRandomPPLMonitor(
                randomPPLMonitor(
                    triggers = listOf(
                        randomPPLTrigger(expireDuration = maxExpireDuration + 10)
                    )
                )
            )
            fail("Expected request to fail with BAD_REQUEST but it succeeded")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // ensure no monitor was created
        ensureNumMonitorV2s(0)
    }

    fun `test create ppl monitor with look back window greater than max fails`() {
        val maxLookBackWindow = 60L
        client().updateSettings(ALERTING_V2_MAX_LOOK_BACK_WINDOW.key, maxLookBackWindow)

        // ensure the request fails
        try {
            createRandomPPLMonitor(
                randomPPLMonitor(
                    lookBackWindow = maxLookBackWindow + 10
                )
            )
            fail("Expected request to fail with BAD_REQUEST but it succeeded")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // ensure no monitor was created
        ensureNumMonitorV2s(0)
    }

    fun `test create ppl monitor with invalid query fails`() {
        // ensure the request fails
        try {
            createRandomPPLMonitor(
                randomPPLMonitor(
                    query = "source = $TEST_INDEX_NAME | not valid ppl"
                )
            )
            fail("Expected request to fail with BAD_REQUEST but it succeeded")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // ensure no monitor was created
        ensureNumMonitorV2s(0)
    }

    fun `test create ppl monitor with query that's too long fails`() {
        adminClient().updateSettings(ALERTING_V2_MAX_QUERY_LENGTH.key, 1)

        // ensure the request fails
        try {
            createRandomPPLMonitor(
                randomPPLMonitor(
                    query = "source = $TEST_INDEX_NAME | head 10"
                )
            )
            fail("Expected request to fail with BAD_REQUEST but it succeeded")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // ensure no monitor was created
        ensureNumMonitorV2s(0)
    }

    fun `test create ppl monitor with invalid custom condition fails`() {
        // ensure the request fails
        try {
            createRandomPPLMonitor(
                randomPPLMonitor(
                    triggers = listOf(
                        randomPPLTrigger(
                            conditionType = ConditionType.CUSTOM,
                            customCondition = "not a valid PPL custom condition",
                            numResultsCondition = null,
                            numResultsValue = null
                        )
                    ),
                    query = "source = $TEST_INDEX_NAME | head 10"
                )
            )
            fail("Expected request to fail with BAD_REQUEST but it succeeded")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // ensure no monitor was created
        ensureNumMonitorV2s(0)
    }

    fun `test create ppl monitor with custom condition that evals to num not bool fails`() {
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(1, MINUTES, "abc", 1)
        indexDocFromSomeTimeAgo(2, MINUTES, "abc", 2)

        // ensure the request fails
        try {
            createRandomPPLMonitor(
                randomPPLMonitor(
                    triggers = listOf(
                        randomPPLTrigger(
                            conditionType = ConditionType.CUSTOM,
                            customCondition = "eval something = sum * 2",
                            numResultsCondition = null,
                            numResultsValue = null
                        )
                    ),
                    query = "source = $TEST_INDEX_NAME | stats sum(number) as sum by abc"
                )
            )
            fail("Expected request to fail with BAD_REQUEST but it succeeded")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // ensure no monitor was created
        ensureNumMonitorV2s(0)
    }

    fun `test create ppl monitor with notification subject source too long fails`() {
        adminClient().updateSettings(NOTIFICATION_SUBJECT_SOURCE_MAX_LENGTH.key, 100)

        var subjectTooLong = ""
        for (i in 0 until 101) {
            subjectTooLong += "a"
        }

        // ensure the request fails
        try {
            createRandomPPLMonitor(
                randomPPLMonitor(
                    triggers = listOf(
                        randomPPLTrigger(
                            actions = listOf(
                                randomAction(
                                    template = randomTemplateScript(
                                        source = "some message"
                                    ),
                                    subjectTemplate = randomTemplateScript(
                                        source = subjectTooLong
                                    )
                                )
                            )
                        )
                    )
                )
            )
            fail("Expected request to fail with BAD_REQUEST but it succeeded")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // ensure no monitor was created
        ensureNumMonitorV2s(0)
    }

    fun `test create ppl monitor with notification message source too long fails`() {
        adminClient().updateSettings(NOTIFICATION_MESSAGE_SOURCE_MAX_LENGTH.key, 1000)

        var messageTooLong = ""
        for (i in 0 until 1001) {
            messageTooLong += "a"
        }

        // ensure the request fails
        try {
            createRandomPPLMonitor(
                randomPPLMonitor(
                    triggers = listOf(
                        randomPPLTrigger(
                            actions = listOf(
                                randomAction(
                                    template = randomTemplateScript(
                                        source = messageTooLong
                                    ),
                                    subjectTemplate = randomTemplateScript(
                                        source = "some subject"
                                    )
                                )
                            )
                        )
                    )
                )
            )
            fail("Expected request to fail with BAD_REQUEST but it succeeded")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // ensure no monitor was created
        ensureNumMonitorV2s(0)
    }

    fun `test get ppl monitor with invalid monitor ID length`() {
        val badId = UUIDs.base64UUID() + "extra"
        try {
            client().makeRequest("GET", "$MONITOR_V2_BASE_URI/$badId")
            fail("Expected request to fail with BAD_REQUEST but it succeeded")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

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

    fun `test delete nonexistent ppl monitor fails`() {
        val randomId = UUIDs.base64UUID()

        try {
            client().makeRequest("DELETE", "$MONITOR_V2_BASE_URI/$randomId")
            fail("Expected request to fail with NOT_FOUND but it succeeded")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    fun `test monitor stats v1 and v2 only return stats for their respective monitors`() {
        enableScheduledJob()

        val monitorV1Id = createMonitor(randomQueryLevelMonitor(enabled = true)).id
        val monitorV2Id = createRandomPPLMonitor(randomPPLMonitor(enabled = true)).id

        val statsV1Response = getAlertingStats()
        val statsV2Response = getAlertingV2Stats()

        logger.info("v1 stats: $statsV1Response")
        logger.info("v2 stats: $statsV2Response")

        assertTrue("V1 stats does not contain V1 Monitor", isMonitorScheduled(monitorV1Id, statsV1Response))
        assertTrue("V2 stats does not contain V2 Monitor", isMonitorScheduled(monitorV2Id, statsV2Response))
        assertFalse("V2 stats contains V1 Monitor", isMonitorScheduled(monitorV1Id, statsV2Response))
        assertFalse("V1 stats contains V2 Monitor", isMonitorScheduled(monitorV2Id, statsV1Response))
    }
}
