/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.alertsv2

import org.junit.Before
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.TEST_INDEX_MAPPINGS
import org.opensearch.alerting.TEST_INDEX_NAME
import org.opensearch.alerting.core.settings.AlertingV2Settings
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.modelv2.PPLSQLMonitor
import org.opensearch.alerting.modelv2.PPLSQLTrigger
import org.opensearch.alerting.modelv2.PPLSQLTrigger.ConditionType
import org.opensearch.alerting.modelv2.PPLSQLTrigger.NumResultsCondition
import org.opensearch.alerting.modelv2.PPLSQLTrigger.TriggerMode
import org.opensearch.alerting.randomPPLMonitor
import org.opensearch.alerting.randomPPLTrigger
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.IntervalSchedule
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.core.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase
import java.time.temporal.ChronoUnit.MINUTES
import java.util.concurrent.TimeUnit

/**
 * Tests AlertV2 history migration, AlertV2 deletion, and AlertV2 expiration functionality
 *
 * Gradle command to run this suite:
 * ./gradlew :alerting:integTest -Dhttps=true -Dsecurity=true -Duser=admin -Dpassword=admin \
 * --tests "org.opensearch.alerting.alertsv2.AlertV2IndicesIT"
 */
class AlertV2IndicesIT : AlertingRestTestCase() {
    @Before
    fun enableAlertingV2() {
        client().updateSettings(AlertingV2Settings.ALERTING_V2_ENABLED.key, "true")
    }

    fun `test create alert v2 index`() {
        generateAlertV2s()

        assertIndexExists(AlertV2Indices.ALERT_V2_INDEX)
        assertIndexExists(AlertV2Indices.ALERT_V2_HISTORY_WRITE_INDEX)
    }

    fun `test update alert v2 index mapping with new schema version`() {
        wipeAllODFEIndices()
        assertIndexDoesNotExist(AlertV2Indices.ALERT_V2_INDEX)
        assertIndexDoesNotExist(AlertV2Indices.ALERT_V2_HISTORY_WRITE_INDEX)

        putAlertV2Mappings(
            AlertV2Indices.alertV2Mapping().trimStart('{').trimEnd('}')
                .replace("\"schema_version\": 1", "\"schema_version\": 0")
        )
        assertIndexExists(AlertV2Indices.ALERT_V2_INDEX)
        assertIndexExists(AlertV2Indices.ALERT_V2_HISTORY_WRITE_INDEX)
        verifyIndexSchemaVersion(AlertV2Indices.ALERT_V2_INDEX, 0)
        verifyIndexSchemaVersion(AlertV2Indices.ALERT_V2_HISTORY_WRITE_INDEX, 0)

        wipeAllODFEIndices()

        generateAlertV2s()
        assertIndexExists(AlertV2Indices.ALERT_V2_INDEX)
        assertIndexExists(AlertV2Indices.ALERT_V2_HISTORY_WRITE_INDEX)
        verifyIndexSchemaVersion(ScheduledJob.SCHEDULED_JOBS_INDEX, 9)
        verifyIndexSchemaVersion(AlertV2Indices.ALERT_V2_INDEX, 1)
        verifyIndexSchemaVersion(AlertV2Indices.ALERT_V2_HISTORY_WRITE_INDEX, 1)
    }

    fun `test alert v2 index gets recreated automatically if deleted`() {
        wipeAllODFEIndices()
        assertIndexDoesNotExist(AlertV2Indices.ALERT_V2_INDEX)

        generateAlertV2s()

        assertIndexExists(AlertV2Indices.ALERT_V2_INDEX)
        assertIndexExists(AlertV2Indices.ALERT_V2_HISTORY_WRITE_INDEX)
        wipeAllODFEIndices()
        assertIndexDoesNotExist(AlertV2Indices.ALERT_V2_INDEX)
        assertIndexDoesNotExist(AlertV2Indices.ALERT_V2_HISTORY_WRITE_INDEX)

        // ensure execute monitor succeeds even after alert indices are deleted
        generateAlertV2s()
    }

    fun `test rollover alert v2 history index`() {
        // Update the rollover check to be every 1 second and the index max age to be 1 second
        client().updateSettings(AlertingSettings.ALERT_V2_HISTORY_ROLLOVER_PERIOD.key, "1s")
        client().updateSettings(AlertingSettings.ALERT_V2_HISTORY_INDEX_MAX_AGE.key, "1s")

        generateAlertV2s()

        // Allow for a rollover index.
        OpenSearchTestCase.waitUntil({
            return@waitUntil (getAlertV2Indices().size >= 3)
        }, 2, TimeUnit.SECONDS)

        assertTrue("Did not find 3 alert v2 indices", getAlertV2Indices().size >= 3)
    }

    fun `test alert v2 history disabled`() {
        resetHistorySettings()

        // Disable alert history
        client().updateSettings(AlertingSettings.ALERT_V2_HISTORY_ENABLED.key, "false")

        val pplMonitorId = generateAlertV2s(
            randomPPLMonitor(
                schedule = IntervalSchedule(interval = 30, unit = MINUTES),
                query = "source = $TEST_INDEX_NAME | head 3",
                triggers = listOf(
                    randomPPLTrigger(
                        mode = PPLSQLTrigger.TriggerMode.RESULT_SET,
                        conditionType = ConditionType.NUMBER_OF_RESULTS,
                        numResultsCondition = NumResultsCondition.GREATER_THAN,
                        numResultsValue = 0L,
                        expireDuration = 1L
                    )
                )
            )
        )

        val alerts1 = searchAlertV2s(pplMonitorId)
        assertEquals("1 alert should be present", 1, alerts1.size)

        // wait for alert to expire.
        // since alert history is disabled, this should result
        // in hard deletion
        OpenSearchTestCase.waitUntil({
            return@waitUntil false
        }, 2, TimeUnit.MINUTES)

        // Since history is disabled, the alert should be hard deleted by now
        val alerts2 = searchAlertV2s(pplMonitorId, AlertV2Indices.ALL_ALERT_V2_INDEX_PATTERN)
        assertTrue("There should be no alerts, but alerts were found", alerts2.isEmpty())
    }

    fun `test short retention period`() {
        resetHistorySettings()

        val pplMonitorId = generateAlertV2s(
            randomPPLMonitor(
                schedule = IntervalSchedule(interval = 30, unit = MINUTES),
                query = "source = $TEST_INDEX_NAME | head 3",
                triggers = listOf(
                    randomPPLTrigger(
                        mode = PPLSQLTrigger.TriggerMode.RESULT_SET,
                        conditionType = ConditionType.NUMBER_OF_RESULTS,
                        numResultsCondition = NumResultsCondition.GREATER_THAN,
                        numResultsValue = 0L,
                        expireDuration = 1L
                    )
                )
            )
        )

        val alerts1 = searchAlertV2s(pplMonitorId)
        assertEquals("1 alert should be present", 1, alerts1.size)

        // history index should be created but empty
        assertEquals(0, getAlertV2HistoryDocCount())

        // wait for alert to expire.
        // since alert history is enabled, this should result
        // in the alert being archived in history index
        OpenSearchTestCase.waitUntil({
            return@waitUntil false
        }, 2, TimeUnit.MINUTES)

        assertTrue(searchAlertV2s(pplMonitorId).isEmpty())
        assertEquals(1, getAlertV2HistoryDocCount())

        // update rollover check and max docs as well as decreasing the retention period
        client().updateSettings(AlertingSettings.ALERT_V2_HISTORY_ROLLOVER_PERIOD.key, "3s")
        client().updateSettings(AlertingSettings.ALERT_V2_HISTORY_MAX_DOCS.key, 1)
        client().updateSettings(AlertingSettings.ALERT_V2_HISTORY_RETENTION_PERIOD.key, "1s")

        // give some time for newly updated settings to take effect
        OpenSearchTestCase.waitUntil({
            return@waitUntil getAlertV2HistoryDocCount() == 0L
        }, 40, TimeUnit.SECONDS)

        // Given the max_docs and retention settings above, the history index will rollover and the non-write index will be deleted.
        // This leaves two indices: active alerts index and an empty history write index
        assertEquals("Did not find 2 alert v2 indices", 2, getAlertV2Indices().size)
        assertEquals(0, getAlertV2HistoryDocCount())
    }

    fun `test generated alert gets expired because monitor was deleted with alert history enabled`() {
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(1, MINUTES, "abc", 5)

        val pplMonitor = createRandomPPLMonitor(
            randomPPLMonitor(
                enabled = true,
                schedule = IntervalSchedule(interval = 20, unit = MINUTES),
                triggers = listOf(
                    randomPPLTrigger(
                        throttleDuration = null,
                        // for this test, configured expire can't be the reason for alert expiration
                        expireDuration = 1000L,
                        mode = TriggerMode.RESULT_SET,
                        conditionType = ConditionType.NUMBER_OF_RESULTS,
                        numResultsCondition = NumResultsCondition.GREATER_THAN,
                        numResultsValue = 0L,
                        customCondition = null
                    )
                ),
                query = "source = $TEST_INDEX_NAME | head 10"
            )
        )

        val executeResponse = executeMonitorV2(pplMonitor.id)
        val triggered = isTriggered(pplMonitor, executeResponse)

        val getAlertsResponsePreExpire = getAlertV2s()
        val alertsGeneratedPreExpire = numAlerts(getAlertsResponsePreExpire) > 0

        assert(triggered) { "Monitor should have triggered but it didn't" }
        assert(alertsGeneratedPreExpire) { "Alerts should have been generated but they weren't" }

        // delete the monitor
        deleteMonitorV2(pplMonitor.id)

        // sleep so postDelete can expire the generated alert
        OpenSearchTestCase.waitUntil({
            return@waitUntil false
        }, 5, TimeUnit.SECONDS)

        val getAlertsResponsePostExpire = getAlertV2s()
        val alertsGeneratedPostExpire = numAlerts(getAlertsResponsePostExpire) > 0
        assert(!alertsGeneratedPostExpire)

        assertEquals(1, getAlertV2HistoryDocCount())
    }

    fun `test generated alert gets expired because monitor was edited with alert history enabled`() {
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(1, MINUTES, "abc", 5)

        // first create a ppl monitor that's guaranteed to generate an alert
        val initialPplTrigger = randomPPLTrigger(
            id = "initialID",
            throttleDuration = null,
            // for this test, configured expire can't be the reason for alert expiration
            expireDuration = 1000L,
            mode = TriggerMode.RESULT_SET,
            conditionType = ConditionType.NUMBER_OF_RESULTS,
            numResultsCondition = NumResultsCondition.GREATER_THAN,
            numResultsValue = 0L,
            customCondition = null
        )

        val initialPplMonitorConfig = randomPPLMonitor(
            enabled = true,
            schedule = IntervalSchedule(interval = 20, unit = MINUTES),
            triggers = listOf(initialPplTrigger),
            query = "source = $TEST_INDEX_NAME | head 10"
        )

        val pplMonitor = createRandomPPLMonitor(initialPplMonitorConfig)

        val executeResponse = executeMonitorV2(pplMonitor.id)
        val triggered = isTriggered(pplMonitor, executeResponse)

        val getAlertsResponsePreExpire = getAlertV2s()
        val alertsGeneratedPreExpire = numAlerts(getAlertsResponsePreExpire) > 0

        assert(triggered) { "Monitor should have triggered but it didn't" }
        assert(alertsGeneratedPreExpire) { "Alerts should have been generated but they weren't" }

        // update the monitor to any new config,
        // and more importantly, updated triggers
        updateMonitorV2(randomPPLMonitor().makeCopy(pplMonitor.id, pplMonitor.version))

        // sleep so postIndex can expire the generated alert
        OpenSearchTestCase.waitUntil({
            return@waitUntil false
        }, 5, TimeUnit.SECONDS)

        val getAlertsResponsePostExpire = getAlertV2s()
        val alertsGeneratedPostExpire = numAlerts(getAlertsResponsePostExpire) > 0
        assert(!alertsGeneratedPostExpire)

        assertEquals(1, getAlertV2HistoryDocCount())
    }

    fun `test generated alert gets expired because monitor was deleted with alert history disabled`() {
        client().updateSettings(AlertingSettings.ALERT_V2_HISTORY_ENABLED.key, "false")

        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(1, MINUTES, "abc", 5)

        val pplMonitor = createRandomPPLMonitor(
            randomPPLMonitor(
                enabled = true,
                schedule = IntervalSchedule(interval = 20, unit = MINUTES),
                triggers = listOf(
                    randomPPLTrigger(
                        throttleDuration = null,
                        // for this test, configured expire can't be the reason for alert expiration
                        expireDuration = 1000L,
                        mode = TriggerMode.RESULT_SET,
                        conditionType = ConditionType.NUMBER_OF_RESULTS,
                        numResultsCondition = NumResultsCondition.GREATER_THAN,
                        numResultsValue = 0L,
                        customCondition = null
                    )
                ),
                query = "source = $TEST_INDEX_NAME | head 10"
            )
        )

        val executeResponse = executeMonitorV2(pplMonitor.id)
        val triggered = isTriggered(pplMonitor, executeResponse)

        val getAlertsResponsePreExpire = getAlertV2s()
        val alertsGeneratedPreExpire = numAlerts(getAlertsResponsePreExpire) > 0

        assert(triggered) { "Monitor should have triggered but it didn't" }
        assert(alertsGeneratedPreExpire) { "Alerts should have been generated but they weren't" }

        // delete the monitor
        deleteMonitorV2(pplMonitor.id)

        // sleep so postDelete can expire the generated alert
        OpenSearchTestCase.waitUntil({
            return@waitUntil false
        }, 5, TimeUnit.SECONDS)

        val getAlertsResponsePostExpire = getAlertV2s()
        val alertsGeneratedPostExpire = numAlerts(getAlertsResponsePostExpire) > 0
        assert(!alertsGeneratedPostExpire)

        assertEquals(0, getAlertV2HistoryDocCount())
    }

    fun `test generated alert gets expired because monitor was edited with alert history disabled`() {
        client().updateSettings(AlertingSettings.ALERT_V2_HISTORY_ENABLED.key, "false")

        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(1, MINUTES, "abc", 5)

        // first create a ppl monitor that's guaranteed to generate an alert
        val initialPplTrigger = randomPPLTrigger(
            id = "initialID",
            throttleDuration = null,
            // for this test, configured expire can't be the reason for alert expiration
            expireDuration = 1000L,
            mode = TriggerMode.RESULT_SET,
            conditionType = ConditionType.NUMBER_OF_RESULTS,
            numResultsCondition = NumResultsCondition.GREATER_THAN,
            numResultsValue = 0L,
            customCondition = null
        )

        val initialPplMonitorConfig = randomPPLMonitor(
            enabled = true,
            schedule = IntervalSchedule(interval = 20, unit = MINUTES),
            triggers = listOf(initialPplTrigger),
            query = "source = $TEST_INDEX_NAME | head 10"
        )

        val pplMonitor = createRandomPPLMonitor(initialPplMonitorConfig)

        val executeResponse = executeMonitorV2(pplMonitor.id)
        val triggered = isTriggered(pplMonitor, executeResponse)

        val getAlertsResponsePreExpire = getAlertV2s()
        val alertsGeneratedPreExpire = numAlerts(getAlertsResponsePreExpire) > 0

        assert(triggered) { "Monitor should have triggered but it didn't" }
        assert(alertsGeneratedPreExpire) { "Alerts should have been generated but they weren't" }

        // update the monitor to any new config
        updateMonitorV2(randomPPLMonitor().makeCopy(pplMonitor.id, pplMonitor.version))

        // sleep so postIndex can expire the generated alert
        OpenSearchTestCase.waitUntil({
            return@waitUntil false
        }, 5, TimeUnit.SECONDS)

        val getAlertsResponsePostExpire = getAlertV2s()
        val alertsGeneratedPostExpire = numAlerts(getAlertsResponsePostExpire) > 0
        assert(!alertsGeneratedPostExpire)

        assertEquals(0, getAlertV2HistoryDocCount())
    }

    private fun assertIndexExists(index: String) {
        val response = client().makeRequest("HEAD", index)
        assertEquals("Index $index does not exist.", RestStatus.OK, response.restStatus())
    }

    private fun assertIndexDoesNotExist(index: String) {
        val response = client().makeRequest("HEAD", index)
        assertEquals("Index $index exists when it shouldn't.", RestStatus.NOT_FOUND, response.restStatus())
    }

    private fun resetHistorySettings() {
        client().updateSettings(AlertingSettings.ALERT_V2_HISTORY_ENABLED.key, "true")
        client().updateSettings(AlertingSettings.ALERT_V2_HISTORY_ROLLOVER_PERIOD.key, "60s")
        client().updateSettings(AlertingSettings.ALERT_V2_HISTORY_RETENTION_PERIOD.key, "60s")
    }

    private fun getAlertV2Indices(): List<String> {
        val response = client().makeRequest("GET", "/_cat/indices/${AlertV2Indices.ALL_ALERT_V2_INDEX_PATTERN}?format=json")
        val xcp = createParser(XContentType.JSON.xContent(), response.entity.content)
        val responseList = xcp.list()
        val indices = mutableListOf<String>()
        responseList.filterIsInstance<Map<String, Any>>().forEach { indices.add(it["index"] as String) }

        return indices
    }

    // generates alerts by creating then executing a monitor
    private fun generateAlertV2s(
        pplMonitorConfig: PPLSQLMonitor = randomPPLMonitor(
            query = "source = $TEST_INDEX_NAME | head 3",
            triggers = listOf(
                randomPPLTrigger(
                    conditionType = ConditionType.NUMBER_OF_RESULTS,
                    numResultsCondition = NumResultsCondition.GREATER_THAN,
                    numResultsValue = 0L
                )
            )
        )
    ): String {
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(1, MINUTES, "abc", 5)
        indexDocFromSomeTimeAgo(2, MINUTES, "def", 10)
        indexDocFromSomeTimeAgo(3, MINUTES, "ghi", 7)

        val pplMonitor = createRandomPPLMonitor(pplMonitorConfig)

        val executeResponse = executeMonitorV2(pplMonitor.id)

        // ensure execute call succeeded
        val xcp = createParser(XContentType.JSON.xContent(), executeResponse.entity.content)
        val output = xcp.map()
        assertNull("Error running monitor v2", output["error"])

        return pplMonitor.id
    }
}
