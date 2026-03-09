/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.IntervalSchedule
import org.opensearch.commons.alerting.model.PPLSQLTrigger.ConditionType
import org.opensearch.commons.alerting.model.PPLSQLTrigger.NumResultsCondition
import org.opensearch.commons.alerting.model.PPLSQLTrigger.TriggerMode
import org.opensearch.test.OpenSearchTestCase
import java.time.temporal.ChronoUnit.MINUTES
import java.util.concurrent.TimeUnit

/***
 * Create various kinds of monitors and ensures they all generate alerts
 * under the expected circumstances
 *
 * Gradle command to run this suite:
 * ./gradlew :alerting:integTest -Dhttps=true -Dsecurity=true -Duser=admin -Dpassword=admin \
 * --tests "org.opensearch.alerting.PPLMonitorRunnerIT"
 */
class PPLSQLMonitorRunnerIT : AlertingRestTestCase() {
    fun `test monitor execution timeout generates error alert`() {
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(2, MINUTES, "abc", 5)

        val pplMonitor = createRandomPPLMonitor(
            randomPPLMonitor(
                enabled = true,
                schedule = IntervalSchedule(interval = 1, unit = MINUTES),
                triggers = listOf(
                    randomPPLTrigger(
                        throttleDuration = null,
                        expireDuration = 5,
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

        // set the monitor execution timebox to 1 nanosecond to guarantee a timeout
        client().updateSettings(AlertingSettings.ALERT_V2_MONITOR_EXECUTION_MAX_DURATION.key, TimeValue.timeValueNanos(1L))

        val executeMonitorResponse = executeMonitor(pplMonitor.id)

        val getAlertsResponse = getAlerts()
        val alertsGenerated = numAlerts(getAlertsResponse) > 0
        val containsErrorAlert = containsErrorAlert(getAlertsResponse)
        val executeResponseContainsError =
            (entityAsMap(executeMonitorResponse).stringMap("input_results")?.get("error") as String).isNotEmpty()

        assert(alertsGenerated) { "Alerts should have been generated but they weren't" }
        assert(containsErrorAlert) { "Error alert should have been generated for timeout but wasn't" }
        assert(executeResponseContainsError) { "Execute monitor response should've included an error message but didn't" }
    }

    fun `test running number of results condition and result set mode ppl monitor`() {
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(2, MINUTES, "abc", 5)

        val pplMonitor = createRandomPPLMonitor(
            randomPPLMonitor(
                enabled = true,
                schedule = IntervalSchedule(interval = 1, unit = MINUTES),
                triggers = listOf(
                    randomPPLTrigger(
                        throttleDuration = null,
                        expireDuration = 5,
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

        val versionBefore = pplMonitor.version

        val executeResponse = executeMonitor(pplMonitor.id)
        val triggered = isTriggered(pplMonitor, executeResponse)

        val getAlertsResponse = getAlerts()
        val alertsGenerated = numAlerts(getAlertsResponse) > 0

        assert(triggered) { "Monitor should have triggered but it didn't" }
        assert(alertsGenerated) { "Alerts should have been generated but they weren't" }

        val pplMonitorAfter = getMonitor(pplMonitor.id)
        val versionAfter = pplMonitorAfter.version

        assert(versionBefore == versionAfter) { "Monitor version changed after monitor execution" }
    }

    fun `test running number of results condition and per result mode ppl monitor`() {
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(1, MINUTES, "abc", 5)
        indexDocFromSomeTimeAgo(2, MINUTES, "def", 10)
        indexDocFromSomeTimeAgo(3, MINUTES, "ghi", 7)

        val pplMonitor = createRandomPPLMonitor(
            randomPPLMonitor(
                enabled = true,
                schedule = IntervalSchedule(interval = 1, unit = MINUTES),
                triggers = listOf(
                    randomPPLTrigger(
                        throttleDuration = null,
                        expireDuration = 5,
                        mode = TriggerMode.PER_RESULT,
                        conditionType = ConditionType.NUMBER_OF_RESULTS,
                        numResultsCondition = NumResultsCondition.GREATER_THAN,
                        numResultsValue = 0L,
                        customCondition = null
                    )
                ),
                query = "source = $TEST_INDEX_NAME | head 10"
            )
        )

        val executeResponse = executeMonitor(pplMonitor.id)
        val triggered = isTriggered(pplMonitor, executeResponse)

        val getAlertsResponse = getAlerts()
        val alertsGenerated = numAlerts(getAlertsResponse)

        assert(triggered) { "Monitor should have triggered but it didn't" }
        assertEquals(
            "A number of alerts matching the number of docs ingested (3) should have been generated",
            3, alertsGenerated
        )
    }

    fun `test running custom condition and result set mode ppl monitor`() {
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(1, MINUTES, "abc", 1)
        indexDocFromSomeTimeAgo(2, MINUTES, "abc", 2)
        indexDocFromSomeTimeAgo(3, MINUTES, "abc", 3)
        indexDocFromSomeTimeAgo(4, MINUTES, "def", 4)
        indexDocFromSomeTimeAgo(5, MINUTES, "def", 5)
        indexDocFromSomeTimeAgo(6, MINUTES, "def", 6)
        indexDocFromSomeTimeAgo(7, MINUTES, "ghi", 7)
        indexDocFromSomeTimeAgo(8, MINUTES, "ghi", 8)
        indexDocFromSomeTimeAgo(9, MINUTES, "ghi", 9)

        val pplMonitor = createRandomPPLMonitor(
            randomPPLMonitor(
                enabled = true,
                schedule = IntervalSchedule(interval = 1, unit = MINUTES),
                triggers = listOf(
                    randomPPLTrigger(
                        throttleDuration = null,
                        expireDuration = 5,
                        mode = TriggerMode.RESULT_SET,
                        conditionType = ConditionType.CUSTOM,
                        customCondition = "eval result = max_num > 5",
                        numResultsCondition = null,
                        numResultsValue = null
                    )
                ),
                query = "source = $TEST_INDEX_NAME | stats max(number) as max_num by abc"
            )
        )

        val executeResponse = executeMonitor(pplMonitor.id)
        val triggered = isTriggered(pplMonitor, executeResponse)

        val getAlertsResponse = getAlerts()
        val alertsGenerated = numAlerts(getAlertsResponse) > 0

        assert(triggered) { "Monitor should have triggered but it didn't" }
        assert(alertsGenerated) { "Alerts should have been generated but they weren't" }
    }

    fun `test running custom condition and per result mode ppl monitor`() {
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(1, MINUTES, "abc", 1)
        indexDocFromSomeTimeAgo(2, MINUTES, "abc", 2)
        indexDocFromSomeTimeAgo(3, MINUTES, "abc", 3)
        indexDocFromSomeTimeAgo(4, MINUTES, "def", 4)
        indexDocFromSomeTimeAgo(5, MINUTES, "def", 5)
        indexDocFromSomeTimeAgo(6, MINUTES, "def", 6)
        indexDocFromSomeTimeAgo(7, MINUTES, "ghi", 7)
        indexDocFromSomeTimeAgo(8, MINUTES, "ghi", 8)
        indexDocFromSomeTimeAgo(9, MINUTES, "ghi", 9)

        val pplMonitor = createRandomPPLMonitor(
            randomPPLMonitor(
                enabled = true,
                schedule = IntervalSchedule(interval = 1, unit = MINUTES),
                triggers = listOf(
                    randomPPLTrigger(
                        throttleDuration = null,
                        expireDuration = 5,
                        mode = TriggerMode.PER_RESULT,
                        conditionType = ConditionType.CUSTOM,
                        customCondition = "eval evaluation = max_num > 5",
                        numResultsCondition = null,
                        numResultsValue = null
                    )
                ),
                query = "source = $TEST_INDEX_NAME | stats max(number) as max_num by abc"
            )
        )

        val executeResponse = executeMonitor(pplMonitor.id)
        val triggered = isTriggered(pplMonitor, executeResponse)

        val getAlertsResponse = getAlerts()
        val alertsGenerated = numAlerts(getAlertsResponse)

        // when the indexed docs above are aggregated by field abc, we have:
        // max("abc") = 3
        // max("def") = 6
        // max("ghi") = 9
        // only 2 of these buckets satisfy the custom condition max_num > 5, so
        // only 2 alerts should be generated

        assert(triggered) { "Monitor should have triggered but it didn't" }
        assertEquals(
            "A number of alerts matching the number of docs ingested (2) should have been generated",
            2, alertsGenerated
        )
    }

    fun `test execute api generated alert gets expired`() {
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(1, MINUTES, "abc", 5)

        val pplMonitor = createRandomPPLMonitor(
            randomPPLMonitor(
                enabled = true,
                schedule = IntervalSchedule(interval = 20, unit = MINUTES),
                triggers = listOf(
                    randomPPLTrigger(
                        throttleDuration = null,
                        expireDuration = 1L,
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

        val executeResponse = executeMonitor(pplMonitor.id)
        val triggered = isTriggered(pplMonitor, executeResponse)

        val getAlertsResponsePreExpire = getAlerts()
        val alertsGeneratedPreExpire = numAlerts(getAlertsResponsePreExpire) > 0

        assert(triggered) { "Monitor should have triggered but it didn't" }
        assert(alertsGeneratedPreExpire) { "Alerts should have been generated but they weren't" }

        // sleep briefly so alert mover can expire the alert
        OpenSearchTestCase.waitUntil({
            return@waitUntil false
        }, 2, TimeUnit.MINUTES)

        val getAlertsResponsePostExpire = getAlerts()
        val alertsGeneratedPostExpire = numAlerts(getAlertsResponsePostExpire) > 0
        assert(!alertsGeneratedPostExpire)
    }

    fun `test scheduled job generated alert gets expired`() {
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(1, MINUTES, "abc", 5)

        // the monitor should generate 1 alert, then not generate
        // any alerts for the rest of the test
        createRandomPPLMonitor(
            randomPPLMonitor(
                enabled = true,
                schedule = IntervalSchedule(interval = 1, unit = MINUTES),
                triggers = listOf(
                    randomPPLTrigger(
                        throttleDuration = 100L,
                        expireDuration = 1L,
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

        // sleep briefly so scheduled job can generate the alert
        OpenSearchTestCase.waitUntil({
            return@waitUntil false
        }, 2, TimeUnit.MINUTES)

        val getAlertsResponsePreExpire = getAlerts()
        val alertsGeneratedPreExpire = numAlerts(getAlertsResponsePreExpire) > 0

        assert(alertsGeneratedPreExpire) { "Alerts should have been generated but they weren't" }

        // sleep briefly so alert mover can expire the alert
        OpenSearchTestCase.waitUntil({
            return@waitUntil false
        }, 2, TimeUnit.MINUTES)

        val getAlertsResponsePostExpire = getAlerts()
        logger.info("num alerts: ${numAlerts(getAlertsResponsePostExpire)}")
        val alertsGeneratedPostExpire = numAlerts(getAlertsResponsePostExpire) > 0
        assert(!alertsGeneratedPostExpire)
    }

    fun `test scheduled job monitor execution gets throttled`() {
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(1, MINUTES, "abc", 5)

        val pplMonitor = createRandomPPLMonitor(
            randomPPLMonitor(
                enabled = true,
                schedule = IntervalSchedule(interval = 1, unit = MINUTES),
                triggers = listOf(
                    randomPPLTrigger(
                        throttleDuration = 10,
                        expireDuration = 5,
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

        val executeResponse = executeMonitor(pplMonitor.id)
        val triggered = isTriggered(pplMonitor, executeResponse)

        val getAlertsResponsePreThrottle = getAlerts()
        val numAlertsPreThrottle = numAlerts(getAlertsResponsePreThrottle)

        assert(triggered) { "Monitor should have triggered but it didn't" }
        assertEquals("Alerts should have been generated but they weren't", 1, numAlertsPreThrottle)

        // sleep briefly to give the monitor to execute again
        // automatically and get throttled
        OpenSearchTestCase.waitUntil({
            return@waitUntil false
        }, 2, TimeUnit.MINUTES)

        val getAlertsResponsePostThrottled = getAlerts()
        val numAlertsPostThrottled = numAlerts(getAlertsResponsePostThrottled)
        assertEquals("A new alert was generated when it should have been throttled", 1, numAlertsPostThrottled)
    }

    fun `test manual monitor execution bypasses throttle`() {
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(1, MINUTES, "abc", 5)

        val pplMonitor = createRandomPPLMonitor(
            randomPPLMonitor(
                enabled = true,
                schedule = IntervalSchedule(interval = 30, unit = MINUTES),
                triggers = listOf(
                    randomPPLTrigger(
                        throttleDuration = 20,
                        expireDuration = 5,
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

        val executeResponse = executeMonitor(pplMonitor.id)
        val triggered = isTriggered(pplMonitor, executeResponse)

        val getAlertsResponse = getAlerts()
        val numAlerts = numAlerts(getAlertsResponse)

        assert(triggered) { "Monitor should have triggered but it didn't" }
        assertEquals("Alerts should have been generated but they weren't", 1, numAlerts)

        // sleep briefly to get comfortable inside
        // the throttle window
        OpenSearchTestCase.waitUntil({
            return@waitUntil false
        }, 10, TimeUnit.SECONDS)

        val executeAgainResponse = executeMonitor(pplMonitor.id)
        val triggeredAgain = isTriggered(pplMonitor, executeAgainResponse)

        val getAlertsAgainResponse = getAlerts()
        val numAlertsAgain = numAlerts(getAlertsAgainResponse)

        assert(triggeredAgain) { "Monitor should have triggered again but it didn't" }
        assertEquals("A new alert should have been generated but was instead throttled", 2, numAlertsAgain)
    }
}
