package org.opensearch.alerting

import org.junit.Assert
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.action.ActionExecutionPolicy
import org.opensearch.commons.alerting.model.action.PerExecutionActionScope
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.MILLIS

class DocLeveFanOutIT : AlertingRestTestCase() {

    fun `test execution reaches endtime before completing execution`() {
        var updateSettings =
            adminClient().updateSettings(AlertingSettings.DOC_LEVEL_MONITOR_EXECUTION_MAX_DURATION.key, TimeValue.timeValueNanos(1))
        val updateSettings1 = adminClient().updateSettings(AlertingSettings.FINDING_HISTORY_ENABLED.key, false)
        logger.info(updateSettings1)
        logger.info(updateSettings)
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
        val actions = (0..randomInt(10)).map {
            randomActionWithPolicy(
                template = randomTemplateScript("Hello {{ctx.monitor.name}}"),
                destinationId = createDestination().id,
                actionExecutionPolicy = actionExecutionPolicy
            )
        }

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = actions)
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = actions)
        val trigger2 = randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = actions)
        val trigger3 = randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = actions)
        val trigger4 = randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = actions)
        val trigger5 = randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = actions)
        val trigger6 = randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = actions)

        val monitor = createMonitor(
            randomDocumentLevelMonitor(
                inputs = listOf(docLevelInput),
                triggers = listOf(trigger, trigger1, trigger2, trigger3, trigger4, trigger5, trigger6)
            )
        )
        assertNotNull(monitor.id)

        indexDoc(testIndex, "1", testDoc)
        indexDoc(testIndex, "5", testDoc)

        var response = executeMonitor(monitor.id)

        var output = entityAsMap(response)

        assertEquals(monitor.name, output["monitor_name"])
        @Suppress("UNCHECKED_CAST")
        var inputResults = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        Assert.assertTrue(inputResults.isEmpty())

        updateSettings =
            adminClient().updateSettings(AlertingSettings.DOC_LEVEL_MONITOR_EXECUTION_MAX_DURATION.key, TimeValue.timeValueMinutes(4))
        logger.info(updateSettings)

        response = executeMonitor(monitor.id)
        output = entityAsMap(response)
        assertEquals(monitor.name, output["monitor_name"])
        inputResults = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        val matchingDocsToQuery = inputResults[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 2, matchingDocsToQuery.size)
        assertTrue("Incorrect search result", matchingDocsToQuery.contains("1|$testIndex"))
        assertTrue("Incorrect search result", matchingDocsToQuery.contains("5|$testIndex"))
    }
}
