/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.resthandler

import org.apache.http.HttpHeaders
import org.apache.http.entity.ContentType
import org.apache.http.message.BasicHeader
import org.apache.http.nio.entity.NStringEntity
import org.opensearch.alerting.ALERTING_BASE_URI
import org.opensearch.alerting.ALWAYS_RUN
import org.opensearch.alerting.ANOMALY_DETECTOR_INDEX
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.DESTINATION_BASE_URI
import org.opensearch.alerting.LEGACY_OPENDISTRO_ALERTING_BASE_URI
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.anomalyDetectorIndexMapping
import org.opensearch.alerting.core.model.CronSchedule
import org.opensearch.alerting.core.model.DocLevelMonitorInput
import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.core.settings.ScheduledJobSettings
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.DocumentLevelTrigger
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.QueryLevelTrigger
import org.opensearch.alerting.model.destination.Chime
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.randomADMonitor
import org.opensearch.alerting.randomAction
import org.opensearch.alerting.randomAlert
import org.opensearch.alerting.randomAnomalyDetector
import org.opensearch.alerting.randomAnomalyDetectorWithUser
import org.opensearch.alerting.randomBucketLevelTrigger
import org.opensearch.alerting.randomDocumentLevelMonitor
import org.opensearch.alerting.randomDocumentLevelTrigger
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.alerting.randomQueryLevelTrigger
import org.opensearch.alerting.randomThrottle
import org.opensearch.alerting.randomUser
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.toJsonString
import org.opensearch.alerting.util.DestinationType
import org.opensearch.client.ResponseException
import org.opensearch.client.WarningFailureException
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.script.Script
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.junit.annotations.TestLogging
import org.opensearch.test.rest.OpenSearchRestTestCase
import java.time.Instant
import java.time.ZoneId
import java.time.temporal.ChronoUnit

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class MonitorRestApiIT : AlertingRestTestCase() {

    val USE_TYPED_KEYS = ToXContent.MapParams(mapOf("with_type" to "true"))

    @Throws(Exception::class)
    fun `test plugin is loaded`() {
        val response = entityAsMap(OpenSearchRestTestCase.client().makeRequest("GET", "_nodes/plugins"))
        val nodesInfo = response["nodes"] as Map<String, Map<String, Any>>
        for (nodeInfo in nodesInfo.values) {
            val plugins = nodeInfo["plugins"] as List<Map<String, Any>>
            for (plugin in plugins) {
                if (plugin["name"] == "opensearch-alerting") {
                    return
                }
            }
        }
        fail("Plugin not installed")
    }

    fun `test parsing monitor as a scheduled job`() {
        val monitor = createRandomMonitor()

        val builder = monitor.toXContentWithUser(XContentBuilder.builder(XContentType.JSON.xContent()), USE_TYPED_KEYS)
        val string = BytesReference.bytes(builder).utf8ToString()
        val xcp = createParser(XContentType.JSON.xContent(), string)
        val scheduledJob = ScheduledJob.parse(xcp, monitor.id, monitor.version)
        assertEquals(monitor, scheduledJob)
    }

    @Throws(Exception::class)
    fun `test creating a monitor`() {
        val monitor = randomQueryLevelMonitor()

        val createResponse = client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())

        assertEquals("Create monitor failed", RestStatus.CREATED, createResponse.restStatus())
        val responseBody = createResponse.asMap()
        val createdId = responseBody["_id"] as String
        val createdVersion = responseBody["_version"] as Int
        assertNotEquals("response is missing Id", Monitor.NO_ID, createdId)
        assertTrue("incorrect version", createdVersion > 0)
        assertEquals("Incorrect Location header", "$ALERTING_BASE_URI/$createdId", createResponse.getHeader("Location"))
    }

    fun `test creating a monitor with legacy ODFE`() {
        val monitor = randomQueryLevelMonitor()
        val createResponse = client().makeRequest("POST", LEGACY_OPENDISTRO_ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
        assertEquals("Create monitor failed", RestStatus.CREATED, createResponse.restStatus())
        val responseBody = createResponse.asMap()
        val createdId = responseBody["_id"] as String
        val createdVersion = responseBody["_version"] as Int
        assertNotEquals("response is missing Id", Monitor.NO_ID, createdId)
        assertTrue("incorrect version", createdVersion > 0)
    }

    fun `test creating a monitor with action threshold greater than max threshold`() {
        val monitor = randomMonitorWithThrottle(100000, ChronoUnit.MINUTES)

        try {
            client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test creating a monitor with action threshold less than min threshold`() {
        val monitor = randomMonitorWithThrottle(-1)

        try {
            client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test creating a monitor with updating action threshold`() {
        adminClient().updateSettings("plugins.alerting.action_throttle_max_value", TimeValue.timeValueHours(1))

        val monitor = randomMonitorWithThrottle(2, ChronoUnit.HOURS)

        try {
            client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
        adminClient().updateSettings("plugins.alerting.action_throttle_max_value", TimeValue.timeValueHours(24))
    }

    fun `test creating a monitor with PUT fails`() {
        try {
            val monitor = randomQueryLevelMonitor()
            client().makeRequest("PUT", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
            fail("Expected 405 Method Not Allowed response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.METHOD_NOT_ALLOWED, e.response.restStatus())
        }
    }

    fun `test creating a monitor with illegal index name`() {
        try {
            val si = SearchInput(listOf("_#*IllegalIndexCharacters"), SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))
            val monitor = randomQueryLevelMonitor()
            client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.copy(inputs = listOf(si)).toHttpEntity())
        } catch (e: ResponseException) {
            // When an index with invalid name is mentioned, instead of returning invalid_index_name_exception security plugin throws security_exception.
            // Refer: https://github.com/opendistro-for-elasticsearch/security/issues/718
            // Without security plugin we get BAD_REQUEST correctly. With security_plugin we get INTERNAL_SERVER_ERROR, till above issue is fixed.
            assertTrue(
                "Unexpected status",
                listOf<RestStatus>(RestStatus.BAD_REQUEST, RestStatus.FORBIDDEN).contains(e.response.restStatus())
            )
        }
    }

    fun `test creating an AD monitor without detector index`() {
        try {
            val monitor = randomADMonitor()

            client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
        } catch (e: ResponseException) {
            // When user create AD monitor without detector index, will throw index not found exception
            assertTrue("Unexpected error", e.message!!.contains("Configured indices are not found"))
            assertTrue(
                "Unexpected status",
                listOf<RestStatus>(RestStatus.NOT_FOUND).contains(e.response.restStatus())
            )
        }
    }

    fun `test creating an AD monitor with detector index created but no detectors`() {
        createAnomalyDetectorIndex()
        try {
            val monitor = randomADMonitor()
            client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
        } catch (e: ResponseException) {
            // When user create AD monitor with no detector, will throw exception
            assertTrue("Unexpected error", e.message!!.contains("User has no available detectors"))
            assertTrue(
                "Unexpected status",
                listOf<RestStatus>(RestStatus.NOT_FOUND).contains(e.response.restStatus())
            )
        }
    }

    fun `test creating an AD monitor with no detector has monitor backend role`() {
        if (!securityEnabled()) {
            createAnomalyDetectorIndex()
            // TODO: change to REST API call to test security enabled case
            indexDoc(ANOMALY_DETECTOR_INDEX, "1", randomAnomalyDetector())
            indexDoc(ANOMALY_DETECTOR_INDEX, "2", randomAnomalyDetectorWithUser(randomAlphaOfLength(5)))
            try {
                val monitor = randomADMonitor()
                client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
            } catch (e: ResponseException) {
                // When user create AD monitor with no detector has backend role, will throw exception
                assertTrue("Unexpected error", e.message!!.contains("User has no available detectors"))
                assertTrue(
                    "Unexpected status",
                    listOf<RestStatus>(RestStatus.NOT_FOUND).contains(e.response.restStatus())
                )
            }
        }
    }

    /*
    fun `test creating an AD monitor with detector has monitor backend role`() {
        createAnomalyDetectorIndex()
        val backendRole = "test-role"
        val user = randomADUser(backendRole)
        indexDoc(ANOMALY_DETECTOR_INDEX, "1", randomAnomalyDetector())
        indexDoc(ANOMALY_DETECTOR_INDEX, "2", randomAnomalyDetectorWithUser(randomAlphaOfLength(5)))
        indexDoc(ANOMALY_DETECTOR_INDEX, "3", randomAnomalyDetectorWithUser(backendRole = backendRole), refresh = true)

        val monitor = randomADMonitor(user = user)
        val createResponse = client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
        assertEquals("Create monitor failed", RestStatus.CREATED, createResponse.restStatus())
        val responseBody = createResponse.asMap()
        val createdId = responseBody["_id"] as String
        val createdVersion = responseBody["_version"] as Int
        assertNotEquals("response is missing Id", Monitor.NO_ID, createdId)
        assertTrue("incorrect version", createdVersion > 0)
        assertEquals("Incorrect Location header", "$ALERTING_BASE_URI/$createdId", createResponse.getHeader("Location"))
    }*/

    private fun createAnomalyDetectorIndex() {
        try {
            createTestIndex(ANOMALY_DETECTOR_INDEX, anomalyDetectorIndexMapping())
        } catch (e: Exception) {
            // WarningFailureException is expected as we are creating system index start with dot
            assertTrue(e is WarningFailureException)
        }
    }

    /* Enable this test case after checking for disallowed destination during Monitor creation is added in
    fun `test creating a monitor with a disallowed destination type fails`() {
        try {
            // Create a Chime Destination
            val chime = Chime("http://abc.com")
            val destination = Destination(
                type = DestinationType.CHIME,
                name = "test",
                user = randomUser(),
                lastUpdateTime = Instant.now(),
                chime = chime,
                slack = null,
                customWebhook = null,
                email = null
            )
            val chimeDestination = createDestination(destination = destination)

            // Remove Chime from the allow_list
            val allowedDestinations = DestinationType.values().toList()
                .filter { destinationType -> destinationType != DestinationType.CHIME }
                .joinToString(prefix = "[", postfix = "]") { string -> "\"$string\"" }
            client().updateSettings(DestinationSettings.ALLOW_LIST.key, allowedDestinations)

            createMonitor(randomQueryLevelMonitor(triggers = listOf(randomQueryLevelTrigger(destinationId = chimeDestination.id))))
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        }
    }
     */

    @Throws(Exception::class)
    fun `test updating search for a monitor`() {
        val monitor = createRandomMonitor()

        val updatedSearch = SearchInput(
            emptyList(),
            SearchSourceBuilder().query(QueryBuilders.termQuery("foo", "bar"))
        )
        val updateResponse = client().makeRequest(
            "PUT", monitor.relativeUrl(),
            emptyMap(), monitor.copy(inputs = listOf(updatedSearch)).toHttpEntity()
        )

        assertEquals("Update monitor failed", RestStatus.OK, updateResponse.restStatus())
        val responseBody = updateResponse.asMap()
        assertEquals("Updated monitor id doesn't match", monitor.id, responseBody["_id"] as String)
        assertEquals("Version not incremented", (monitor.version + 1).toInt(), responseBody["_version"] as Int)

        val updatedMonitor = getMonitor(monitor.id)
        assertEquals("Monitor search not updated", listOf(updatedSearch), updatedMonitor.inputs)
    }

    @Throws(Exception::class)
    fun `test updating conditions for a monitor`() {
        val monitor = createRandomMonitor()

        val updatedTriggers = listOf(
            QueryLevelTrigger(
                name = "foo",
                severity = "1",
                condition = Script("return true"),
                actions = emptyList()
            )
        )
        val updateResponse = client().makeRequest(
            "PUT", monitor.relativeUrl(),
            emptyMap(), monitor.copy(triggers = updatedTriggers).toHttpEntity()
        )

        assertEquals("Update monitor failed", RestStatus.OK, updateResponse.restStatus())
        val responseBody = updateResponse.asMap()
        assertEquals("Updated monitor id doesn't match", monitor.id, responseBody["_id"] as String)
        assertEquals("Version not incremented", (monitor.version + 1).toInt(), responseBody["_version"] as Int)

        val updatedMonitor = getMonitor(monitor.id)
        assertEquals("Monitor trigger not updated", updatedTriggers, updatedMonitor.triggers)
    }

    @Throws(Exception::class)
    fun `test updating schedule for a monitor`() {
        val monitor = createRandomMonitor()

        val updatedSchedule = CronSchedule(expression = "0 9 * * *", timezone = ZoneId.of("UTC"))
        val updateResponse = client().makeRequest(
            "PUT", monitor.relativeUrl(),
            emptyMap(), monitor.copy(schedule = updatedSchedule).toHttpEntity()
        )

        assertEquals("Update monitor failed", RestStatus.OK, updateResponse.restStatus())
        val responseBody = updateResponse.asMap()
        assertEquals("Updated monitor id doesn't match", monitor.id, responseBody["_id"] as String)
        assertEquals("Version not incremented", (monitor.version + 1).toInt(), responseBody["_version"] as Int)

        val updatedMonitor = getMonitor(monitor.id)
        assertEquals("Monitor trigger not updated", updatedSchedule, updatedMonitor.schedule)
    }

    @Throws(Exception::class)
    fun `test getting a monitor`() {
        val monitor = createRandomMonitor()

        val storedMonitor = getMonitor(monitor.id)

        assertEquals("Indexed and retrieved monitor differ", monitor, storedMonitor)
    }

    @Throws(Exception::class)
    fun `test getting a monitor that doesn't exist`() {
        try {
            getMonitor(randomAlphaOfLength(20))
            fail("expected response exception")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test checking if a monitor exists`() {
        val monitor = createRandomMonitor()

        val headResponse = client().makeRequest("HEAD", monitor.relativeUrl())
        assertEquals("Unable to HEAD monitor", RestStatus.OK, headResponse.restStatus())
        assertNull("Response contains unexpected body", headResponse.entity)
    }

    fun `test checking if a non-existent monitor exists`() {
        val headResponse = client().makeRequest("HEAD", "$ALERTING_BASE_URI/foobarbaz")
        assertEquals("Unexpected status", RestStatus.NOT_FOUND, headResponse.restStatus())
    }

    @Throws(Exception::class)
    fun `test deleting a monitor`() {
        val monitor = createRandomMonitor()

        val deleteResponse = client().makeRequest("DELETE", monitor.relativeUrl())
        assertEquals("Delete failed", RestStatus.OK, deleteResponse.restStatus())

        val getResponse = client().makeRequest("HEAD", monitor.relativeUrl())
        assertEquals("Deleted monitor still exists", RestStatus.NOT_FOUND, getResponse.restStatus())
    }

    @Throws(Exception::class)
    fun `test deleting a monitor that doesn't exist`() {
        try {
            client().makeRequest("DELETE", "$ALERTING_BASE_URI/foobarbaz")
            fail("expected 404 ResponseException")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    fun `test getting UI metadata monitor not from OpenSearch Dashboards`() {
        val monitor = createRandomMonitor(withMetadata = true)
        val getMonitor = getMonitor(monitorId = monitor.id)
        assertEquals(
            "UI Metadata returned but request did not come from OpenSearch Dashboards.",
            getMonitor.uiMetadata, mapOf<String, Any>()
        )
    }

    fun `test getting UI metadata monitor from OpenSearch Dashboards`() {
        val monitor = createRandomMonitor(refresh = true, withMetadata = true)
        val header = BasicHeader(HttpHeaders.USER_AGENT, "OpenSearch-Dashboards")
        val getMonitor = getMonitor(monitorId = monitor.id, header = header)
        assertEquals("", monitor.uiMetadata, getMonitor.uiMetadata)
    }

    fun `test query a monitor that exists`() {
        val monitor = createRandomMonitor(true)

        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", monitor.id)).toString()
        val searchResponse = client().makeRequest(
            "GET", "$ALERTING_BASE_URI/_search",
            emptyMap(),
            NStringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search monitor failed", RestStatus.OK, searchResponse.restStatus())
        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("Monitor not found during search", 1, numberDocsFound)
    }

    fun `test query a monitor that exists POST`() {
        val monitor = createRandomMonitor(true)

        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", monitor.id)).toString()
        val searchResponse = client().makeRequest(
            "POST", "$ALERTING_BASE_URI/_search",
            emptyMap(),
            NStringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search monitor failed", RestStatus.OK, searchResponse.restStatus())
        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("Monitor not found during search", 1, numberDocsFound)
    }

    fun `test query a monitor that doesn't exist`() {
        // Create a random monitor to create the ScheduledJob index. Otherwise we test will fail with 404 index not found.
        createRandomMonitor(refresh = true)
        val search = SearchSourceBuilder().query(
            QueryBuilders.termQuery(
                OpenSearchTestCase.randomAlphaOfLength(5),
                OpenSearchTestCase.randomAlphaOfLength(5)
            )
        ).toString()

        val searchResponse = client().makeRequest(
            "GET",
            "$ALERTING_BASE_URI/_search",
            emptyMap(),
            NStringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search monitor failed", RestStatus.OK, searchResponse.restStatus())
        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("Monitor found during search when no document present.", 0, numberDocsFound)
    }

    fun `test query a monitor with UI metadata from OpenSearch Dashboards`() {
        val monitor = createRandomMonitor(refresh = true, withMetadata = true)
        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", monitor.id)).toString()
        val header = BasicHeader(HttpHeaders.USER_AGENT, "OpenSearch-Dashboards")
        val searchResponse = client().makeRequest(
            "GET",
            "$ALERTING_BASE_URI/_search",
            emptyMap(),
            NStringEntity(search, ContentType.APPLICATION_JSON),
            header
        )
        assertEquals("Search monitor failed", RestStatus.OK, searchResponse.restStatus())

        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"] as Map<String, Map<String, Any>>
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("Monitor not found during search", 1, numberDocsFound)

        val searchHits = hits["hits"] as List<Any>
        val hit = searchHits[0] as Map<String, Any>
        val monitorHit = hit["_source"] as Map<String, Any>
        assertNotNull(
            "UI Metadata returned from search but request did not come from OpenSearchDashboards",
            monitorHit[Monitor.UI_METADATA_FIELD]
        )
    }

    fun `test query a monitor with UI metadata as user`() {
        val monitor = createRandomMonitor(refresh = true, withMetadata = true)
        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", monitor.id)).toString()
        val searchResponse = client().makeRequest(
            "GET",
            "$ALERTING_BASE_URI/_search",
            emptyMap(),
            NStringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search monitor failed", RestStatus.OK, searchResponse.restStatus())

        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"] as Map<String, Map<String, Any>>
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("Monitor not found during search", 1, numberDocsFound)

        val searchHits = hits["hits"] as List<Any>
        val hit = searchHits[0] as Map<String, Any>
        val monitorHit = hit["_source"] as Map<String, Any>
        assertNull(
            "UI Metadata returned from search but request did not come from OpenSearchDashboards",
            monitorHit[Monitor.UI_METADATA_FIELD]
        )
    }

    fun `test acknowledge all alert states`() {
        putAlertMappings() // Required as we do not have a create alert API.
        val monitor = createRandomMonitor(refresh = true)
        val acknowledgedAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACKNOWLEDGED))
        val completedAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.COMPLETED))
        val errorAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.ERROR))
        val activeAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val invalidAlert = randomAlert(monitor).copy(id = "foobar")

        val response = acknowledgeAlerts(monitor, acknowledgedAlert, completedAlert, errorAlert, activeAlert, invalidAlert)
        val responseMap = response.asMap()

        val activeAlertAcknowledged = searchAlerts(monitor).single { it.id == activeAlert.id }
        assertNotNull("Unsuccessful acknowledgement", responseMap["success"] as List<String>)
        assertTrue("Alert not in acknowledged response", responseMap["success"].toString().contains(activeAlert.id))
        assertEquals("Alert not acknowledged.", Alert.State.ACKNOWLEDGED, activeAlertAcknowledged.state)
        assertNotNull("Alert acknowledged time is NULL", activeAlertAcknowledged.acknowledgedTime)

        val failedResponseList = responseMap["failed"].toString()
        assertTrue("Alert in state ${acknowledgedAlert.state} not found in failed list", failedResponseList.contains(acknowledgedAlert.id))
        assertTrue("Alert in state ${completedAlert.state} not found in failed list", failedResponseList.contains(errorAlert.id))
        assertTrue("Alert in state ${errorAlert.state} not found in failed list", failedResponseList.contains(completedAlert.id))
        assertTrue("Invalid alert not found in failed list", failedResponseList.contains(invalidAlert.id))
        assertFalse("Alert in state ${activeAlert.state} found in failed list", failedResponseList.contains(activeAlert.id))
    }

    fun `test acknowledging more than 10 alerts at once`() {
        // GIVEN
        putAlertMappings() // Required as we do not have a create alert API.
        val monitor = createRandomMonitor(refresh = true)
        val alertsToAcknowledge = (1..15).map { createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE)) }.toTypedArray()

        // WHEN
        val response = acknowledgeAlerts(monitor, *alertsToAcknowledge)

        // THEN
        val responseMap = response.asMap()
        val expectedAcknowledgedCount = alertsToAcknowledge.size

        val acknowledgedAlerts = responseMap["success"] as List<String>
        assertTrue(
            "Expected $expectedAcknowledgedCount alerts to be acknowledged successfully.",
            acknowledgedAlerts.size == expectedAcknowledgedCount
        )

        val acknowledgedAlertsList = acknowledgedAlerts.toString()
        alertsToAcknowledge.forEach {
            alert ->
            assertTrue("Alert with ID ${alert.id} not found in failed list.", acknowledgedAlertsList.contains(alert.id))
        }

        val failedResponse = responseMap["failed"] as List<String>
        assertTrue("Expected 0 alerts to fail acknowledgment.", failedResponse.isEmpty())
    }

    fun `test acknowledging more than 10 alerts at once, including acknowledged alerts`() {
        // GIVEN
        putAlertMappings() // Required as we do not have a create alert API.
        val monitor = createRandomMonitor(refresh = true)
        val alertsGroup1 = (1..15).map { createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE)) }.toTypedArray()
        acknowledgeAlerts(monitor, *alertsGroup1) // Acknowledging the first array of alerts.

        val alertsGroup2 = (1..15).map { createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE)) }.toTypedArray()

        // Creating an array of alerts that includes alerts that have been already acknowledged, and new alerts.
        val alertsToAcknowledge = arrayOf(*alertsGroup1, *alertsGroup2)

        // WHEN
        val response = acknowledgeAlerts(monitor, *alertsToAcknowledge)

        // THEN
        val responseMap = response.asMap()
        val expectedAcknowledgedCount = alertsToAcknowledge.size - alertsGroup1.size

        val acknowledgedAlerts = responseMap["success"] as List<String>
        assertTrue(
            "Expected $expectedAcknowledgedCount alerts to be acknowledged successfully.",
            acknowledgedAlerts.size == expectedAcknowledgedCount
        )

        val acknowledgedAlertsList = acknowledgedAlerts.toString()
        alertsGroup2.forEach {
            alert ->
            assertTrue("Alert with ID ${alert.id} not found in failed list.", acknowledgedAlertsList.contains(alert.id))
        }
        alertsGroup1.forEach {
            alert ->
            assertFalse("Alert with ID ${alert.id} found in failed list.", acknowledgedAlertsList.contains(alert.id))
        }

        val failedResponse = responseMap["failed"] as List<String>
        assertTrue("Expected ${alertsGroup1.size} alerts to fail acknowledgment.", failedResponse.size == alertsGroup1.size)

        val failedResponseList = failedResponse.toString()
        alertsGroup1.forEach {
            alert ->
            assertTrue("Alert with ID ${alert.id} not found in failed list.", failedResponseList.contains(alert.id))
        }
        alertsGroup2.forEach {
            alert ->
            assertFalse("Alert with ID ${alert.id} found in failed list.", failedResponseList.contains(alert.id))
        }
    }

    @Throws(Exception::class)
    fun `test acknowledging 0 alerts`() {
        // GIVEN
        putAlertMappings() // Required as we do not have a create alert API.
        val monitor = createRandomMonitor(refresh = true)
        val alertsToAcknowledge = arrayOf<Alert>()

        // WHEN & THEN
        try {
            acknowledgeAlerts(monitor, *alertsToAcknowledge)
            fail("Expected acknowledgeAlerts to throw an exception.")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test get all alerts in all states`() {
        putAlertMappings() // Required as we do not have a create alert API.
        val monitor = createRandomMonitor(refresh = true)
        val acknowledgedAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACKNOWLEDGED))
        val completedAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.COMPLETED))
        val errorAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.ERROR))
        val activeAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val invalidAlert = randomAlert(monitor).copy(id = "foobar")

        val inputMap = HashMap<String, Any>()
        inputMap["missing"] = "_last"

        val responseMap = getAlerts(inputMap).asMap()
        val alerts = responseMap["alerts"].toString()

        assertEquals(4, responseMap["totalAlerts"])
        assertTrue("Acknowledged alert with id, ${acknowledgedAlert.id}, not found in alert list", alerts.contains(acknowledgedAlert.id))
        assertTrue("Completed alert with id, ${completedAlert.id}, not found in alert list", alerts.contains(completedAlert.id))
        assertTrue("Error alert with id, ${errorAlert.id}, not found in alert list", alerts.contains(errorAlert.id))
        assertTrue("Active alert with id, ${activeAlert.id}, not found in alert list", alerts.contains(activeAlert.id))
        assertFalse("Invalid alert with id, ${invalidAlert.id}, found in alert list", alerts.contains(invalidAlert.id))
    }

    fun `test get all alerts with active states`() {
        putAlertMappings() // Required as we do not have a create alert API.
        val monitor = createRandomMonitor(refresh = true)
        val acknowledgedAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACKNOWLEDGED))
        val completedAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.COMPLETED))
        val errorAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.ERROR))
        val activeAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val invalidAlert = randomAlert(monitor).copy(id = "foobar")

        val inputMap = HashMap<String, Any>()
        inputMap["alertState"] = Alert.State.ACTIVE.name

        val responseMap = getAlerts(inputMap).asMap()
        val alerts = responseMap["alerts"].toString()

        assertEquals(1, responseMap["totalAlerts"])
        assertFalse("Acknowledged alert with id, ${acknowledgedAlert.id}, found in alert list", alerts.contains(acknowledgedAlert.id))
        assertFalse("Completed alert with id, ${completedAlert.id}, found in alert list", alerts.contains(completedAlert.id))
        assertFalse("Error alert with id, ${errorAlert.id}, found in alert list", alerts.contains(errorAlert.id))
        assertTrue("Active alert with id, ${activeAlert.id}, not found in alert list", alerts.contains(activeAlert.id))
        assertFalse("Invalid alert with id, ${invalidAlert.id}, found in alert list", alerts.contains(invalidAlert.id))
    }

    fun `test get all alerts with severity 1`() {
        putAlertMappings() // Required as we do not have a create alert API.
        val monitor = createRandomMonitor(refresh = true)
        val acknowledgedAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACKNOWLEDGED, severity = "1"))
        val completedAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.COMPLETED, severity = "3"))
        val errorAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.ERROR, severity = "1"))
        val activeAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE, severity = "2"))

        val inputMap = HashMap<String, Any>()
        inputMap["severityLevel"] = "1"

        val responseMap = getAlerts(inputMap).asMap()
        val alerts = responseMap["alerts"].toString()

        assertEquals(2, responseMap["totalAlerts"])
        assertTrue(
            "Acknowledged sev 1 alert with id, ${acknowledgedAlert.id}, not found in alert list",
            alerts.contains(acknowledgedAlert.id)
        )
        assertFalse("Completed sev 3 alert with id, ${completedAlert.id}, found in alert list", alerts.contains(completedAlert.id))
        assertTrue("Error sev 1 alert with id, ${errorAlert.id}, not found in alert list", alerts.contains(errorAlert.id))
        assertFalse("Active sev 2 alert with id, ${activeAlert.id}, found in alert list", alerts.contains(activeAlert.id))
    }

    fun `test get all alerts for a specific monitor by id`() {
        putAlertMappings() // Required as we do not have a create alert API.
        val monitor = createRandomMonitor(refresh = true)
        val monitor2 = createRandomMonitor(refresh = true)
        val acknowledgedAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACKNOWLEDGED))
        val completedAlert = createAlert(randomAlert(monitor2).copy(state = Alert.State.COMPLETED))
        val errorAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.ERROR))
        val activeAlert = createAlert(randomAlert(monitor2).copy(state = Alert.State.ACTIVE))

        val inputMap = HashMap<String, Any>()
        inputMap["monitorId"] = monitor.id

        val responseMap = getAlerts(inputMap).asMap()
        val alerts = responseMap["alerts"].toString()

        assertEquals(2, responseMap["totalAlerts"])
        assertTrue(
            "Acknowledged alert for chosen monitor with id, ${acknowledgedAlert.id}, not found in alert list",
            alerts.contains(acknowledgedAlert.id)
        )
        assertFalse("Completed sev 3 alert with id, ${completedAlert.id}, found in alert list", alerts.contains(completedAlert.id))
        assertTrue("Error alert for chosen monitor with id, ${errorAlert.id}, not found in alert list", alerts.contains(errorAlert.id))
        assertFalse("Active alert sev 2 with id, ${activeAlert.id}, found in alert list", alerts.contains(activeAlert.id))
    }

    fun `test get alerts by searching monitor name`() {
        putAlertMappings() // Required as we do not have a create alert API.

        val monitor = createRandomMonitor(refresh = true)
        val monitor2 = createRandomMonitor(refresh = true)
        val acknowledgedAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACKNOWLEDGED))
        val completedAlert = createAlert(randomAlert(monitor2).copy(state = Alert.State.COMPLETED))
        val errorAlert = createAlert(randomAlert(monitor).copy(state = Alert.State.ERROR))
        val activeAlert = createAlert(randomAlert(monitor2).copy(state = Alert.State.ACTIVE))

        val inputMap = HashMap<String, Any>()
        inputMap["searchString"] = monitor.name

        val responseMap = getAlerts(inputMap).asMap()
        val alerts = responseMap["alerts"].toString()

        assertEquals(2, responseMap["totalAlerts"])
        assertTrue(
            "Acknowledged alert for matching monitor with id, ${acknowledgedAlert.id}, not found in alert list",
            alerts.contains(acknowledgedAlert.id)
        )
        assertFalse("Completed sev 3 alert with id, ${completedAlert.id}, found in alert list", alerts.contains(completedAlert.id))
        assertTrue("Error alert for matching monitor with id, ${errorAlert.id}, not found in alert list", alerts.contains(errorAlert.id))
        assertFalse("Active alert sev 2 with id, ${activeAlert.id}, found in alert list", alerts.contains(activeAlert.id))
    }

    fun `test mappings after monitor creation`() {
        createRandomMonitor(refresh = true)

        val response = client().makeRequest("GET", "/${ScheduledJob.SCHEDULED_JOBS_INDEX}/_mapping")
        val parserMap = createParser(XContentType.JSON.xContent(), response.entity.content).map() as Map<String, Map<String, Any>>
        val mappingsMap = parserMap[ScheduledJob.SCHEDULED_JOBS_INDEX]!!["mappings"] as Map<String, Any>
        val expected = createParser(
            XContentType.JSON.xContent(),
            javaClass.classLoader.getResource("mappings/scheduled-jobs.json").readText()
        )
        val expectedMap = expected.map()

        assertEquals("Mappings are different", expectedMap, mappingsMap)
    }

    fun `test delete monitor moves alerts`() {
        client().updateSettings(ScheduledJobSettings.SWEEPER_ENABLED.key, true)
        putAlertMappings()
        val monitor = createRandomMonitor(true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        refreshIndex("*")
        val deleteResponse = client().makeRequest("DELETE", "$ALERTING_BASE_URI/${monitor.id}")
        assertEquals("Delete request not successful", RestStatus.OK, deleteResponse.restStatus())

        // Wait 5 seconds for event to be processed and alerts moved
        Thread.sleep(5000)

        val alerts = searchAlerts(monitor)
        assertEquals("Active alert was not deleted", 0, alerts.size)

        val historyAlerts = searchAlerts(monitor, AlertIndices.ALERT_HISTORY_WRITE_INDEX)
        assertEquals("Alert was not moved to history", 1, historyAlerts.size)
        assertEquals(
            "Alert data incorrect",
            alert.copy(state = Alert.State.DELETED).toJsonString(),
            historyAlerts.single().toJsonString()
        )
    }

    fun `test delete trigger moves alerts`() {
        client().updateSettings(ScheduledJobSettings.SWEEPER_ENABLED.key, true)
        putAlertMappings()
        val trigger = randomQueryLevelTrigger()
        val monitor = createMonitor(randomQueryLevelMonitor(triggers = listOf(trigger)))
        val alert = createAlert(randomAlert(monitor).copy(triggerId = trigger.id, state = Alert.State.ACTIVE))
        refreshIndex("*")
        val updatedMonitor = monitor.copy(triggers = emptyList())
        val updateResponse = client().makeRequest(
            "PUT", "$ALERTING_BASE_URI/${monitor.id}", emptyMap(),
            updatedMonitor.toHttpEntity()
        )
        assertEquals("Update request not successful", RestStatus.OK, updateResponse.restStatus())

        // Wait 5 seconds for event to be processed and alerts moved
        Thread.sleep(5000)

        val alerts = searchAlerts(monitor)
        assertEquals("Active alert was not deleted", 0, alerts.size)

        val historyAlerts = searchAlerts(monitor, AlertIndices.ALERT_HISTORY_WRITE_INDEX)
        assertEquals("Alert was not moved to history", 1, historyAlerts.size)
        assertEquals(
            "Alert data incorrect",
            alert.copy(state = Alert.State.DELETED).toJsonString(),
            historyAlerts.single().toJsonString()
        )
    }

    fun `test delete trigger moves alerts only for deleted trigger`() {
        client().updateSettings(ScheduledJobSettings.SWEEPER_ENABLED.key, true)
        putAlertMappings()
        val triggerToDelete = randomQueryLevelTrigger()
        val triggerToKeep = randomQueryLevelTrigger()
        val monitor = createMonitor(randomQueryLevelMonitor(triggers = listOf(triggerToDelete, triggerToKeep)))
        val alertKeep = createAlert(randomAlert(monitor).copy(triggerId = triggerToKeep.id, state = Alert.State.ACTIVE))
        val alertDelete = createAlert(randomAlert(monitor).copy(triggerId = triggerToDelete.id, state = Alert.State.ACTIVE))
        refreshIndex("*")
        val updatedMonitor = monitor.copy(triggers = listOf(triggerToKeep))
        val updateResponse = client().makeRequest(
            "PUT", "$ALERTING_BASE_URI/${monitor.id}", emptyMap(),
            updatedMonitor.toHttpEntity()
        )
        assertEquals("Update request not successful", RestStatus.OK, updateResponse.restStatus())

        // Wait 5 seconds for event to be processed and alerts moved
        Thread.sleep(5000)

        val alerts = searchAlerts(monitor)
        // We have two alerts from above, 1 for each trigger, there should be only 1 left in active index
        assertEquals("One alert should be in active index", 1, alerts.size)
        assertEquals("Wrong alert in active index", alertKeep.toJsonString(), alerts.single().toJsonString())

        val historyAlerts = searchAlerts(monitor, AlertIndices.ALERT_HISTORY_WRITE_INDEX)
        // Only alertDelete should of been moved to history index
        assertEquals("One alert should be in history index", 1, historyAlerts.size)
        assertEquals(
            "Alert data incorrect",
            alertDelete.copy(state = Alert.State.DELETED).toJsonString(),
            historyAlerts.single().toJsonString()
        )
    }

    fun `test update monitor with wrong version`() {
        val monitor = createRandomMonitor(refresh = true)
        try {
            client().makeRequest(
                "PUT", "${monitor.relativeUrl()}?refresh=true&if_seq_no=1234&if_primary_term=1234",
                emptyMap(), monitor.toHttpEntity()
            )
            fail("expected 409 ResponseException")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.CONFLICT, e.response.restStatus())
        }
    }

    fun `test monitor stats disable plugin`() {
        // Disable the Monitor plugin.
        disableScheduledJob()

        val responseMap = getAlertingStats()
        // assertEquals("Cluster name is incorrect", responseMap["cluster_name"], "alerting_integTestCluster")
        assertEquals("Scheduled job is not enabled", false, responseMap[ScheduledJobSettings.SWEEPER_ENABLED.key])
        assertEquals("Scheduled job index exists but there are no scheduled jobs.", false, responseMap["scheduled_job_index_exists"])
        val _nodes = responseMap["_nodes"] as Map<String, Int>
        validateAlertingStatsNodeResponse(_nodes)
    }

    fun `test monitor stats when disabling and re-enabling scheduled jobs with existing monitor`() {
        // Enable Monitor jobs
        enableScheduledJob()
        val monitorId = createMonitor(randomQueryLevelMonitor(enabled = true), refresh = true).id

        var alertingStats = getAlertingStats()
        assertEquals("Scheduled job is not enabled", true, alertingStats[ScheduledJobSettings.SWEEPER_ENABLED.key])
        assertEquals("Scheduled job index does not exist", true, alertingStats["scheduled_job_index_exists"])
        assertEquals("Scheduled job index is not yellow", "yellow", alertingStats["scheduled_job_index_status"])
        assertEquals("Nodes are not on schedule", numberOfNodes, alertingStats["nodes_on_schedule"])

        val _nodes = alertingStats["_nodes"] as Map<String, Int>
        validateAlertingStatsNodeResponse(_nodes)

        assertTrue(
            "Monitor [$monitorId] was not found scheduled based on the alerting stats response: $alertingStats",
            isMonitorScheduled(monitorId, alertingStats)
        )

        // Disable Monitor jobs
        disableScheduledJob()

        alertingStats = getAlertingStats()
        assertEquals("Scheduled job is still enabled", false, alertingStats[ScheduledJobSettings.SWEEPER_ENABLED.key])
        assertFalse(
            "Monitor [$monitorId] was still scheduled based on the alerting stats response: $alertingStats",
            isMonitorScheduled(monitorId, alertingStats)
        )

        // Re-enable Monitor jobs
        enableScheduledJob()

        // Sleep briefly so sweep can reschedule the Monitor
        Thread.sleep(2000)

        alertingStats = getAlertingStats()
        assertEquals("Scheduled job is not enabled", true, alertingStats[ScheduledJobSettings.SWEEPER_ENABLED.key])
        assertTrue(
            "Monitor [$monitorId] was not re-scheduled based on the alerting stats response: $alertingStats",
            isMonitorScheduled(monitorId, alertingStats)
        )
    }

    fun `test monitor stats no jobs`() {
        // Enable the Monitor plugin.
        enableScheduledJob()

        val responseMap = getAlertingStats()
        // assertEquals("Cluster name is incorrect", responseMap["cluster_name"], "alerting_integTestCluster")
        assertEquals("Scheduled job is not enabled", true, responseMap[ScheduledJobSettings.SWEEPER_ENABLED.key])
        assertEquals("Scheduled job index exists but there are no scheduled jobs.", false, responseMap["scheduled_job_index_exists"])
        val _nodes = responseMap["_nodes"] as Map<String, Int>
        validateAlertingStatsNodeResponse(_nodes)
    }

    fun `test monitor stats jobs`() {
        // Enable the Monitor plugin.
        enableScheduledJob()
        createRandomMonitor(refresh = true)

        val responseMap = getAlertingStats()
        // assertEquals("Cluster name is incorrect", responseMap["cluster_name"], "alerting_integTestCluster")
        assertEquals("Scheduled job is not enabled", true, responseMap[ScheduledJobSettings.SWEEPER_ENABLED.key])
        assertEquals("Scheduled job index does not exist", true, responseMap["scheduled_job_index_exists"])
        assertEquals("Scheduled job index is not yellow", "yellow", responseMap["scheduled_job_index_status"])
        assertEquals("Nodes are not on schedule", numberOfNodes, responseMap["nodes_on_schedule"])

        val _nodes = responseMap["_nodes"] as Map<String, Int>
        validateAlertingStatsNodeResponse(_nodes)
    }

    @Throws(Exception::class)
    fun `test max number of monitors`() {
        client().updateSettings(AlertingSettings.ALERTING_MAX_MONITORS.key, "1")

        createRandomMonitor(refresh = true)
        try {
            createRandomMonitor(refresh = true)
            fail("Request should be rejected as there are too many monitors.")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test monitor specific metric`() {
        // Enable the Monitor plugin.
        enableScheduledJob()
        createRandomMonitor(refresh = true)

        val responseMap = getAlertingStats("/jobs_info")
        // assertEquals("Cluster name is incorrect", responseMap["cluster_name"], "alerting_integTestCluster")
        assertEquals("Scheduled job is not enabled", true, responseMap[ScheduledJobSettings.SWEEPER_ENABLED.key])
        assertEquals("Scheduled job index does not exist", true, responseMap["scheduled_job_index_exists"])
        assertEquals("Scheduled job index is not yellow", "yellow", responseMap["scheduled_job_index_status"])
        assertEquals("Nodes not on schedule", numberOfNodes, responseMap["nodes_on_schedule"])

        val _nodes = responseMap["_nodes"] as Map<String, Int>
        validateAlertingStatsNodeResponse(_nodes)
    }

    fun `test monitor stats incorrect metric`() {
        try {
            getAlertingStats("/foobarzzz")
            fail("Incorrect stats metric should have failed")
        } catch (e: ResponseException) {
            assertEquals("Failed", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test monitor stats _all and other metric`() {
        try {
            getAlertingStats("/_all,jobs_info")
            fail("Incorrect stats metric should have failed")
        } catch (e: ResponseException) {
            assertEquals("Failed", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    private fun randomMonitorWithThrottle(value: Int, unit: ChronoUnit = ChronoUnit.MINUTES): Monitor {
        val throttle = randomThrottle(value, unit)
        val action = randomAction().copy(throttle = throttle)
        val trigger = randomQueryLevelTrigger(actions = listOf(action))
        return randomQueryLevelMonitor(triggers = listOf(trigger))
    }

    @Throws(Exception::class)
    fun `test search monitors only`() {

        // 1. create monitor
        val monitor = randomQueryLevelMonitor()
        val createResponse = client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
        assertEquals("Create monitor failed", RestStatus.CREATED, createResponse.restStatus())

        // 2. create destination
        val chime = Chime("http://abc.com")
        val destination = Destination(
            type = DestinationType.CHIME,
            name = "test",
            user = randomUser(),
            lastUpdateTime = Instant.now(),
            chime = chime,
            slack = null,
            customWebhook = null,
            email = null
        )
        val response = client().makeRequest(
            "POST",
            DESTINATION_BASE_URI,
            emptyMap(),
            destination.toHttpEntity()
        )
        assertEquals("Unable to create a new destination", RestStatus.CREATED, response.restStatus())

        // 3. search - must return only monitors.
        val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()
        val searchResponse = client().makeRequest(
            "GET",
            "$ALERTING_BASE_URI/_search",
            emptyMap(),
            NStringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search monitor failed", RestStatus.OK, searchResponse.restStatus())
        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("Destination objects are also returned by /_search.", 1, numberDocsFound)

        val searchHits = hits["hits"] as List<Any>
        val hit = searchHits[0] as Map<String, Any>
        val monitorHit = hit["_source"] as Map<String, Any>
        assertEquals("Type is not monitor", monitorHit[Monitor.TYPE_FIELD], "monitor")
    }

    @Throws(Exception::class)
    fun `test search monitor with alerting indices only`() {
        // 1. search - must return error as invalid index is passed
        val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()
        val params: MutableMap<String, String> = HashMap()
        params["index"] = "data-logs"
        try {
            client().makeRequest(
                "GET",
                "$ALERTING_BASE_URI/_search",
                params,
                NStringEntity(search, ContentType.APPLICATION_JSON)
            )
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    private fun validateAlertingStatsNodeResponse(nodesResponse: Map<String, Int>) {
        assertEquals("Incorrect number of nodes", numberOfNodes, nodesResponse["total"])
        assertEquals("Failed nodes found during monitor stats call", 0, nodesResponse["failed"])
        assertEquals("More than $numberOfNodes successful node", numberOfNodes, nodesResponse["successful"])
    }

    private fun isMonitorScheduled(monitorId: String, alertingStatsResponse: Map<String, Any>): Boolean {
        val nodesInfo = alertingStatsResponse["nodes"] as Map<String, Any>
        for (nodeId in nodesInfo.keys) {
            val nodeInfo = nodesInfo[nodeId] as Map<String, Any>
            val jobsInfo = nodeInfo["jobs_info"] as Map<String, Any>
            if (jobsInfo.keys.contains(monitorId)) {
                return true
            }
        }

        return false
    }

    @Throws(Exception::class)
    fun `test creating a document monitor`() {
        val testIndex = createTestIndex()
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docReturningInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docReturningInput), triggers = listOf(trigger)))

        val createResponse = client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())

        assertEquals("Create monitor failed", RestStatus.CREATED, createResponse.restStatus())
        val responseBody = createResponse.asMap()
        val createdId = responseBody["_id"] as String
        val createdVersion = responseBody["_version"] as Int
        assertNotEquals("response is missing Id", Monitor.NO_ID, createdId)
        assertTrue("incorrect version", createdVersion > 0)
        val actualLocation = createResponse.getHeader("Location")
        assertEquals("Incorrect Location header", "$ALERTING_BASE_URI/$createdId", actualLocation)
    }

    @Throws(Exception::class)
    fun `test getting a document level monitor`() {
        val testIndex = createTestIndex()
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docReturningInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(
            randomDocumentLevelMonitor(inputs = listOf(docReturningInput), triggers = listOf(trigger), user = null)
        )

        val storedMonitor = getMonitor(monitor.id)

        assertEquals("Indexed and retrieved monitor differ", monitor, storedMonitor)
    }

    @Throws(Exception::class)
    fun `test updating conditions for a doc-level monitor`() {
        val testIndex = createTestIndex()
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docReturningInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docReturningInput), triggers = listOf(trigger)))

        val updatedTriggers = listOf(
            DocumentLevelTrigger(
                name = "foo",
                severity = "1",
                condition = Script("return true"),
                actions = emptyList()
            )
        )
        val updateResponse = client().makeRequest(
            "PUT", monitor.relativeUrl(),
            emptyMap(), monitor.copy(triggers = updatedTriggers).toHttpEntity()
        )

        assertEquals("Update monitor failed", RestStatus.OK, updateResponse.restStatus())
        val responseBody = updateResponse.asMap()
        assertEquals("Updated monitor id doesn't match", monitor.id, responseBody["_id"] as String)
        assertEquals("Version not incremented", (monitor.version + 1).toInt(), responseBody["_version"] as Int)

        val updatedMonitor = getMonitor(monitor.id)
        assertEquals("Monitor trigger not updated", updatedTriggers, updatedMonitor.triggers)
    }

    @Throws(Exception::class)
    fun `test deleting a document level monitor`() {
        val testIndex = createTestIndex()
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docReturningInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docReturningInput), triggers = listOf(trigger)))

        val deleteResponse = client().makeRequest("DELETE", monitor.relativeUrl())
        assertEquals("Delete failed", RestStatus.OK, deleteResponse.restStatus())

        val getResponse = client().makeRequest("HEAD", monitor.relativeUrl())
        assertEquals("Deleted monitor still exists", RestStatus.NOT_FOUND, getResponse.restStatus())
    }

    fun `test creating a document monitor with error trigger`() {
        val trigger = randomQueryLevelTrigger()
        try {
            val monitor = randomDocumentLevelMonitor(triggers = listOf(trigger))
            client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
            fail("Monitor with illegal trigger should be rejected.")
        } catch (e: IllegalArgumentException) {
            assertEquals(
                "a document monitor with error trigger",
                "Incompatible trigger [${trigger.id}] for monitor type [${Monitor.MonitorType.DOC_LEVEL_MONITOR}]",
                e.message
            )
        }
    }

    fun `test creating a query monitor with error trigger`() {
        val trigger = randomBucketLevelTrigger()
        try {
            val monitor = randomQueryLevelMonitor(triggers = listOf(trigger))
            client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
            fail("Monitor with illegal trigger should be rejected.")
        } catch (e: IllegalArgumentException) {
            assertEquals(
                "a query monitor with error trigger",
                "Incompatible trigger [${trigger.id}] for monitor type [${Monitor.MonitorType.QUERY_LEVEL_MONITOR}]",
                e.message
            )
        }
    }
}
