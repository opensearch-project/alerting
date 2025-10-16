package org.opensearch.alerting

import org.opensearch.alerting.core.modelv2.PPLMonitor
import org.opensearch.alerting.core.modelv2.PPLTrigger.ConditionType
import org.opensearch.alerting.core.modelv2.PPLTrigger.NumResultsCondition
import org.opensearch.alerting.resthandler.MonitorV2RestApiIT.Companion.TEST_INDEX_MAPPINGS
import org.opensearch.alerting.resthandler.MonitorV2RestApiIT.Companion.TEST_INDEX_NAME
import org.opensearch.alerting.resthandler.MonitorV2RestApiIT.Companion.TIMESTAMP_FIELD
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.model.IntervalSchedule
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.temporal.ChronoUnit.MILLIS
import java.time.temporal.ChronoUnit.MINUTES

/***
 * These tests create various kinds of monitors and ensure they all generate alerts
 * under the expected circumstances
 */
class PPLMonitorRunnerIT : AlertingRestTestCase() {
    /* Test Cases */
    fun `test running basic ppl monitor`() {
        indexDocFromSomeTimeAgo(2, MINUTES, "abc", 5)

        val pplMonitorConfig = randomPPLMonitor(
            enabled = true,
            schedule = IntervalSchedule(interval = 1, unit = MINUTES),
            lookbackWindow = null,
            triggers = listOf(
                randomPPLTrigger(
                    suppressDuration = null,
                    expireDuration = 5,
                    conditionType = ConditionType.NUMBER_OF_RESULTS,
                    numResultsCondition = NumResultsCondition.GREATER_THAN,
                    numResultsValue = 0L
                )
            ),
            query = "source = $TEST_INDEX_NAME | head 10"
        )
        val pplMonitor = createMonitorV2(pplMonitorConfig) as PPLMonitor

        val executeResponse = entityAsMap(executeMonitorV2(pplMonitor.id))

        val triggered = isTriggered(pplMonitor, executeResponse)

        // TODO: implement getAlertsV2 in AlertingRestTestCase

        assert(triggered)
    }

    fun `test running ppl monitor with lookback window and doc within lookback window`() {
        indexDocFromSomeTimeAgo(2, MINUTES, "abc", 5)

        val pplMonitorConfig = randomPPLMonitor(
            enabled = true,
            schedule = IntervalSchedule(interval = 1, unit = MINUTES),
            lookbackWindow = 5,
            timestampField = TIMESTAMP_FIELD,
            triggers = listOf(
                randomPPLTrigger(
                    suppressDuration = null,
                    expireDuration = 5,
                    conditionType = ConditionType.NUMBER_OF_RESULTS,
                    numResultsCondition = NumResultsCondition.GREATER_THAN,
                    numResultsValue = 0L
                )
            ),
            query = "source = $TEST_INDEX_NAME | head 10"
        )
        val pplMonitor = createMonitorV2(pplMonitorConfig) as PPLMonitor

        val executeResponse = entityAsMap(executeMonitorV2(pplMonitor.id))

        val triggered = isTriggered(pplMonitor, executeResponse)

        assert(triggered)
    }

    // lookback window doc beyond lookback window not triggered
    // manual monitor execute should bypass suppress
    // auto monitor execute should honor suppress

    /* Utils */

    // creates index TEST_INDEX_NAME with TEST_INDEX_MAPPINGS, then
    // indexes into it a doc from some time ago.
    // this function only works on the TEST_INDEX_NAME index which has fields
    // "timestamp" (date), "abc" (string), "number" (integer)
    private fun indexDocFromSomeTimeAgo(timeValue: Long, timeUnit: ChronoUnit, abc: String, number: Int) {
        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        val someTimeAgo = ZonedDateTime.now().minus(timeValue, timeUnit).truncatedTo(MILLIS)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(someTimeAgo) // the timestamp string is given a random timezone offset
        val testDoc = """{ "timestamp" : "$testTime", "abc": "$abc", "number" : "$number" }"""
        logger.info("test time: $testTime")
        indexDoc(TEST_INDEX_NAME, "1", testDoc)
    }

    // takes in an execute monitor API response and returns true if the
    // trigger condition was met. assumes the monitor executed only had 1 trigger
    private fun isTriggered(pplMonitor: PPLMonitor, executeResponse: Map<String, Any>): Boolean {
        val triggerResultsObj = (executeResponse["trigger_results"] as Map<String, Any>)[pplMonitor.triggers[0].id] as Map<String, Any>
        return triggerResultsObj["triggered"] as Boolean
    }
}
