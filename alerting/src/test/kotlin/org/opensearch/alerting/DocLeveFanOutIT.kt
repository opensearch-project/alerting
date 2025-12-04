/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.action.ActionExecutionPolicy
import org.opensearch.commons.alerting.model.action.PerExecutionActionScope
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.MILLIS
import kotlin.test.Test

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class DocLeveFanOutIT : AlertingRestTestCase() {
    @Test
    fun `test execution reaches endtime before completing execution`() {
        val updateSettings1 = adminClient().updateSettings(AlertingSettings.FINDING_HISTORY_ENABLED.key, false)
        logger.info(updateSettings1)
        val testIndex = createTestIndex()
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))

        val actionExecutionScope = PerExecutionActionScope()
        val actionExecutionPolicy = ActionExecutionPolicy(actionExecutionScope)
        val actions =
            (0..randomInt(10)).map {
                randomActionWithPolicy(
                    template = randomTemplateScript("Hello {{ctx.monitor.name}}"),
                    destinationId = createDestination().id,
                    actionExecutionPolicy = actionExecutionPolicy,
                )
            }

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = actions)

        val monitor =
            createMonitor(
                randomDocumentLevelMonitor(
                    inputs = listOf(docLevelInput),
                    triggers = listOf(trigger),
                ),
            )
        assertNotNull(monitor.id)
        executeMonitor(monitor.id)
        indexDoc(testIndex, "1", testDoc)
        indexDoc(testIndex, "2", testDoc)

        var response = executeMonitor(monitor.id)

        var output = entityAsMap(response)
        val findings1 = searchFindings(monitor)
        val findingsSize1 = findings1.size
        assertEquals(findingsSize1, 2)
        adminClient().updateSettings(AlertingSettings.DOC_LEVEL_MONITOR_EXECUTION_MAX_DURATION.key, TimeValue.timeValueNanos(1))
        executeMonitor(monitor.id)
        Thread.sleep(1000)
        adminClient().updateSettings(AlertingSettings.DOC_LEVEL_MONITOR_EXECUTION_MAX_DURATION.key, TimeValue.timeValueMinutes(4))
        indexDoc(testIndex, "3", testDoc)
        indexDoc(testIndex, "4", testDoc)
        executeMonitor(monitor.id)
        val findings = searchFindings(monitor)
        val findingsSize = findings.size
        assertEquals(findingsSize, 4)
    }
}
