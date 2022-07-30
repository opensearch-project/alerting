/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALL_ALERT_INDEX_PATTERN
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALL_FINDING_INDEX_PATTERN
import org.opensearch.alerting.core.model.CronSchedule
import org.opensearch.alerting.core.model.DocLevelMonitorInput
import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.alerting.core.model.IntervalSchedule
import org.opensearch.alerting.model.DocumentLevelTrigger
import org.opensearch.alerting.model.action.ActionExecutionPolicy
import org.opensearch.alerting.model.action.AlertCategory
import org.opensearch.alerting.model.action.PerAlertActionScope
import org.opensearch.alerting.model.action.PerExecutionActionScope
import org.opensearch.client.Response
import org.opensearch.client.ResponseException
import org.opensearch.script.Script
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.MILLIS

class DocumentMonitorRunnerIT : AlertingRestTestCase() {

    private val log = LogManager.getLogger(javaClass)

    fun `test execute monitor with dryrun`() {

        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val index = createTestIndex()

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))

        val action = randomAction(template = randomTemplateScript("Hello {{ctx.monitor.name}}"), destinationId = createDestination().id)
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action)))
        )

        indexDoc(index, "1", testDoc)

        val response = executeMonitor(monitor, params = DRYRUN_MONITOR)

        val output = entityAsMap(response)
        assertEquals(monitor.name, output["monitor_name"])

        assertEquals(1, output.objectMap("trigger_results").values.size)

        for (triggerResult in output.objectMap("trigger_results").values) {
            assertEquals(1, triggerResult.objectMap("action_results").values.size)
            for (alertActionResult in triggerResult.objectMap("action_results").values) {
                for (actionResult in alertActionResult.values) {
                    @Suppress("UNCHECKED_CAST") val actionOutput = (actionResult as Map<String, Map<String, String>>)["output"]
                        as Map<String, String>
                    assertEquals("Hello ${monitor.name}", actionOutput["subject"])
                    assertEquals("Hello ${monitor.name}", actionOutput["message"])
                }
            }
        }

        val alerts = searchAlerts(monitor)
        assertEquals("Alert saved for test monitor", 0, alerts.size)
    }

    fun `test execute monitor returns search result with dryrun`() {
        val testIndex = createTestIndex()
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger))

        indexDoc(testIndex, "1", testDoc)
        indexDoc(testIndex, "5", testDoc)

        val response = executeMonitor(monitor, params = DRYRUN_MONITOR)

        val output = entityAsMap(response)

        assertEquals(monitor.name, output["monitor_name"])
        @Suppress("UNCHECKED_CAST")
        val searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        val matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 2, matchingDocsToQuery.size)
        assertTrue("Incorrect search result", matchingDocsToQuery.contains("1|$testIndex"))
        assertTrue("Incorrect search result", matchingDocsToQuery.contains("5|$testIndex"))
    }

    fun `test execute monitor generates alerts and findings`() {
        val testIndex = createTestIndex()
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))
        assertNotNull(monitor.id)

        indexDoc(testIndex, "1", testDoc)
        indexDoc(testIndex, "5", testDoc)

        val response = executeMonitor(monitor.id)

        val output = entityAsMap(response)

        assertEquals(monitor.name, output["monitor_name"])
        @Suppress("UNCHECKED_CAST")
        val searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        val matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 2, matchingDocsToQuery.size)
        assertTrue("Incorrect search result", matchingDocsToQuery.containsAll(listOf("1|$testIndex", "5|$testIndex")))

        val alerts = searchAlertsWithFilter(monitor)
        assertEquals("Alert saved for test monitor", 2, alerts.size)

        val findings = searchFindings(monitor)
        assertEquals("Findings saved for test monitor", 2, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
        assertTrue("Findings saved for test monitor", findings[1].relatedDocIds.contains("5"))
    }

    fun `test execute monitor generates alerts and findings with per alert execution for actions`() {
        val testIndex = createTestIndex()
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))

        val alertCategories = AlertCategory.values()
        val actionExecutionScope = PerAlertActionScope(
            actionableAlerts = (1..randomInt(alertCategories.size)).map { alertCategories[it - 1] }.toSet()
        )
        val actionExecutionPolicy = ActionExecutionPolicy(actionExecutionScope)
        val actions = (0..randomInt(10)).map {
            randomActionWithPolicy(
                template = randomTemplateScript("Hello {{ctx.monitor.name}}"),
                destinationId = createDestination().id,
                actionExecutionPolicy = actionExecutionPolicy
            )
        }

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = actions)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))
        assertNotNull(monitor.id)

        indexDoc(testIndex, "1", testDoc)
        indexDoc(testIndex, "5", testDoc)

        val response = executeMonitor(monitor.id)

        val output = entityAsMap(response)

        assertEquals(monitor.name, output["monitor_name"])
        @Suppress("UNCHECKED_CAST")
        val searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        val matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 2, matchingDocsToQuery.size)
        assertTrue("Incorrect search result", matchingDocsToQuery.containsAll(listOf("1|$testIndex", "5|$testIndex")))

        for (triggerResult in output.objectMap("trigger_results").values) {
            assertEquals(2, triggerResult.objectMap("action_results").values.size)
            for (alertActionResult in triggerResult.objectMap("action_results").values) {
                assertEquals(actions.size, alertActionResult.values.size)
                for (actionResult in alertActionResult.values) {
                    @Suppress("UNCHECKED_CAST") val actionOutput = (actionResult as Map<String, Map<String, String>>)["output"]
                        as Map<String, String>
                    assertEquals("Hello ${monitor.name}", actionOutput["subject"])
                    assertEquals("Hello ${monitor.name}", actionOutput["message"])
                }
            }
        }

        val alerts = searchAlertsWithFilter(monitor)
        assertEquals("Alert saved for test monitor", 2, alerts.size)

        val findings = searchFindings(monitor)
        assertEquals("Findings saved for test monitor", 2, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
        assertTrue("Findings saved for test monitor", findings[1].relatedDocIds.contains("5"))
    }

    fun `test execute monitor generates alerts and findings with per trigger execution for actions`() {
        val testIndex = createTestIndex()
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
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
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))
        assertNotNull(monitor.id)

        indexDoc(testIndex, "1", testDoc)
        indexDoc(testIndex, "5", testDoc)

        val response = executeMonitor(monitor.id)

        val output = entityAsMap(response)

        assertEquals(monitor.name, output["monitor_name"])
        @Suppress("UNCHECKED_CAST")
        val searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        val matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 2, matchingDocsToQuery.size)
        assertTrue("Incorrect search result", matchingDocsToQuery.containsAll(listOf("1|$testIndex", "5|$testIndex")))

        for (triggerResult in output.objectMap("trigger_results").values) {
            assertEquals(2, triggerResult.objectMap("action_results").values.size)
            for (alertActionResult in triggerResult.objectMap("action_results").values) {
                assertEquals(actions.size, alertActionResult.values.size)
                for (actionResult in alertActionResult.values) {
                    @Suppress("UNCHECKED_CAST") val actionOutput = (actionResult as Map<String, Map<String, String>>)["output"]
                        as Map<String, String>
                    assertEquals("Hello ${monitor.name}", actionOutput["subject"])
                    assertEquals("Hello ${monitor.name}", actionOutput["message"])
                }
            }
        }

        val alerts = searchAlertsWithFilter(monitor)
        assertEquals("Alert saved for test monitor", 2, alerts.size)

        val findings = searchFindings(monitor)
        assertEquals("Findings saved for test monitor", 2, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
        assertTrue("Findings saved for test monitor", findings[1].relatedDocIds.contains("5"))
    }

    fun `test execute monitor with wildcard index that generates alerts and findings`() {
        val testIndex = createTestIndex("test1")
        val testIndex2 = createTestIndex("test2")
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf("test*"), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))
        assertNotNull(monitor.id)

        indexDoc(testIndex, "1", testDoc)
        indexDoc(testIndex2, "5", testDoc)

        val response = executeMonitor(monitor.id)

        val output = entityAsMap(response)

        assertEquals(monitor.name, output["monitor_name"])
        @Suppress("UNCHECKED_CAST")
        val searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        val matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 2, matchingDocsToQuery.size)
        assertTrue("Incorrect search result", matchingDocsToQuery.containsAll(listOf("1|$testIndex", "5|$testIndex2")))

        val alerts = searchAlertsWithFilter(monitor)
        assertEquals("Alert saved for test monitor", 2, alerts.size)

        val findings = searchFindings(monitor)
        assertEquals("Findings saved for test monitor", 2, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
        assertTrue("Findings saved for test monitor", findings[1].relatedDocIds.contains("5"))
    }

    fun `test execute monitor with new index added after first execution that generates alerts and findings`() {
        val testIndex = createTestIndex("test1")
        val testIndex2 = createTestIndex("test2")
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf("test*"), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))
        assertNotNull(monitor.id)

        indexDoc(testIndex, "1", testDoc)
        indexDoc(testIndex2, "5", testDoc)
        executeMonitor(monitor.id)

        var alerts = searchAlertsWithFilter(monitor)
        assertEquals("Alert saved for test monitor", 2, alerts.size)

        var findings = searchFindings(monitor)
        assertEquals("Findings saved for test monitor", 2, findings.size)
        assertTrue(
            "Findings saved for test monitor expected 1 instead of ${findings[0].relatedDocIds}",
            findings[0].relatedDocIds.contains("1")
        )
        assertTrue(
            "Findings saved for test monitor expected 51 instead of ${findings[1].relatedDocIds}",
            findings[1].relatedDocIds.contains("5")
        )

        // clear previous findings and alerts
        deleteIndex(ALL_FINDING_INDEX_PATTERN)
        deleteIndex(ALL_ALERT_INDEX_PATTERN)

        val testIndex3 = createTestIndex("test3")
        indexDoc(testIndex3, "10", testDoc)
        indexDoc(testIndex, "14", testDoc)
        indexDoc(testIndex2, "51", testDoc)

        val response = executeMonitor(monitor.id)

        val output = entityAsMap(response)

        assertEquals(monitor.name, output["monitor_name"])
        @Suppress("UNCHECKED_CAST")
        val searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        val matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 3, matchingDocsToQuery.size)
        assertTrue("Incorrect search result", matchingDocsToQuery.containsAll(listOf("14|$testIndex", "51|$testIndex2", "10|$testIndex3")))

        alerts = searchAlertsWithFilter(monitor)
        assertEquals("Alert saved for test monitor", 3, alerts.size)

        findings = searchFindings(monitor)
        assertEquals("Findings saved for test monitor", 3, findings.size)
        assertTrue(
            "Findings saved for test monitor expected 14 instead of ${findings[0].relatedDocIds}",
            findings[0].relatedDocIds.contains("14")
        )
        assertTrue(
            "Findings saved for test monitor expected 51 instead of ${findings[1].relatedDocIds}",
            findings[1].relatedDocIds.contains("51")
        )
        assertTrue(
            "Findings saved for test monitor expected 10 instead of ${findings[2].relatedDocIds}",
            findings[2].relatedDocIds.contains("10")
        )
    }

    fun `test document-level monitor when alias only has write index with 0 docs`() {
        // Monitor should execute, but create 0 findings.
        val alias = createTestAlias(includeWriteIndex = true)
        val aliasIndex = alias.keys.first()
        val query = randomDocLevelQuery(tags = listOf())
        val input = randomDocLevelMonitorInput(indices = listOf(aliasIndex), queries = listOf(query))
        val trigger = randomDocumentLevelTrigger(condition = Script("query[id=\"${query.id}\"]"))
        val monitor = createMonitor(randomDocumentLevelMonitor(enabled = false, inputs = listOf(input), triggers = listOf(trigger)))

        val response: Response
        try {
            response = executeMonitor(monitor.id)
        } catch (e: ResponseException) {
            assertNotNull("Expected an error message: $e", e.message)
            e.message?.let {
                assertTrue("Unexpected exception: $e", it.contains("""reason":"no such index [.opensearch-alerting-findings]"""))
            }
            assertEquals(404, e.response.statusLine.statusCode)
            return
        }

        val output = entityAsMap(response)
        val inputResults = output.stringMap("input_results")
        val errorMessage = inputResults?.get("error")
        @Suppress("UNCHECKED_CAST")
        val searchResult = (inputResults?.get("results") as List<Map<String, Any>>).firstOrNull()
        @Suppress("UNCHECKED_CAST")
        val findings = searchFindings()

        assertEquals(monitor.name, output["monitor_name"])
        assertNull("Unexpected monitor execution failure: $errorMessage", errorMessage)
        findings.findings.forEach {
            val queryIds = it.finding.docLevelQueries.map { query -> query.id }
            assertFalse("No findings should exist with queryId ${query.id}, but found: $it", queryIds.contains(query.id))
        }
    }

    fun `test document-level monitor when docs exist prior to monitor creation`() {
        // FIXME: Consider renaming this test case
        // Only new docs should create findings.
        val alias = createTestAlias(includeWriteIndex = true)
        val aliasIndex = alias.keys.first()
        val indices = alias[aliasIndex]?.keys?.toList() as List<String>
        val query = randomDocLevelQuery(tags = listOf())
        val input = randomDocLevelMonitorInput(indices = listOf(aliasIndex), queries = listOf(query))
        val trigger = randomDocumentLevelTrigger(condition = Script("query[id=\"${query.id}\"]"))

        val preExistingDocIds = mutableSetOf<String>()
        indices.forEach { index ->
            val docId = index.hashCode().toString()
            val doc = """{ "message" : "${query.query}" }"""
            preExistingDocIds.add(docId)
            indexDoc(index = index, id = docId, doc = doc)
        }
        assertEquals(indices.size, preExistingDocIds.size)

        val monitor = createMonitor(randomDocumentLevelMonitor(enabled = false, inputs = listOf(input), triggers = listOf(trigger)))

        val response = executeMonitor(monitor.id)

        val output = entityAsMap(response)
        val inputResults = output.stringMap("input_results")
        val errorMessage = inputResults?.get("error")
        @Suppress("UNCHECKED_CAST")
        val searchResult = (inputResults?.get("results") as List<Map<String, Any>>).firstOrNull()
        @Suppress("UNCHECKED_CAST")
        val findings = searchFindings()

        assertEquals(monitor.name, output["monitor_name"])
        assertNull("Unexpected monitor execution failure: $errorMessage", errorMessage)
        findings.findings.forEach {
            val docIds = it.finding.relatedDocIds
            assertTrue(
                "Findings index should not contain a pre-existing doc, but found $it",
                preExistingDocIds.intersect(docIds).isEmpty()
            )
        }
    }

    fun `test document-level monitor when alias indices only contain docs that match query`() {
        // Only new docs should create findings.
        val alias = createTestAlias(includeWriteIndex = true)
        val aliasIndex = alias.keys.first()
        val indices = alias[aliasIndex]?.keys?.toList() as List<String>
        val query = randomDocLevelQuery(tags = listOf())
        val input = randomDocLevelMonitorInput(indices = listOf(aliasIndex), queries = listOf(query))
        val trigger = randomDocumentLevelTrigger(condition = Script("query[id=\"${query.id}\"]"))

        val preExistingDocIds = mutableSetOf<String>()
        indices.forEach { index ->
            val docId = index.hashCode().toString()
            val doc = """{ "message" : "${query.query}" }"""
            preExistingDocIds.add(docId)
            indexDoc(index = index, id = docId, doc = doc)
        }
        assertEquals(indices.size, preExistingDocIds.size)

        val monitor = createMonitor(randomDocumentLevelMonitor(enabled = false, inputs = listOf(input), triggers = listOf(trigger)))
        executeMonitor(monitor.id)

        val newDocIds = mutableSetOf<String>()
        indices.forEach { index ->
            (1..5).map {
                val docId = "${index.hashCode()}$it"
                val doc = """{ "message" : "${query.query}" }"""
                newDocIds.add(docId)
                indexDoc(index = index, id = docId, doc = doc)
            }
        }
        assertEquals(indices.size * 5, newDocIds.size)

        val response = executeMonitor(monitor.id)

        val output = entityAsMap(response)
        val inputResults = output.stringMap("input_results")
        val errorMessage = inputResults?.get("error")
        @Suppress("UNCHECKED_CAST")
        val searchResult = (inputResults?.get("results") as List<Map<String, Any>>).firstOrNull()
        @Suppress("UNCHECKED_CAST")
        val findings = searchFindings()

        assertEquals(monitor.name, output["monitor_name"])
        assertNull("Unexpected monitor execution failure: $errorMessage", errorMessage)
        findings.findings.forEach {
            val docIds = it.finding.relatedDocIds
            assertTrue(
                "Findings index should not contain a pre-existing doc, but found $it",
                preExistingDocIds.intersect(docIds).isEmpty()
            )
            assertTrue("Found an unexpected finding $it", newDocIds.intersect(docIds).isNotEmpty())
        }
    }

    fun `test document-level monitor when alias indices contain docs that do and do not match query`() {
        // Only matching docs should create findings.
        val alias = createTestAlias(includeWriteIndex = true)
        val aliasIndex = alias.keys.first()
        val indices = alias[aliasIndex]?.keys?.toList() as List<String>
        val query = randomDocLevelQuery(tags = listOf())
        val input = randomDocLevelMonitorInput(indices = listOf(aliasIndex), queries = listOf(query))
        val trigger = randomDocumentLevelTrigger(condition = Script("query[id=\"${query.id}\"]"))

        val preExistingDocIds = mutableSetOf<String>()
        indices.forEach { index ->
            val docId = index.hashCode().toString()
            val doc = """{ "message" : "${query.query}" }"""
            preExistingDocIds.add(docId)
            indexDoc(index = index, id = docId, doc = doc)
        }
        assertEquals(indices.size, preExistingDocIds.size)

        val monitor = createMonitor(randomDocumentLevelMonitor(enabled = false, inputs = listOf(input), triggers = listOf(trigger)))
        executeMonitor(monitor.id)

        val matchingDocIds = mutableSetOf<String>()
        val nonMatchingDocIds = mutableSetOf<String>()
        indices.forEach { index ->
            (1..5).map {
                val matchingDocId = "${index.hashCode()}$it"
                val matchingDoc = """{ "message" : "${query.query}" }"""
                indexDoc(index = index, id = matchingDocId, doc = matchingDoc)
                matchingDocIds.add(matchingDocId)

                val nonMatchingDocId = "${index.hashCode()}${it}2"
                var nonMatchingDoc = StringBuilder(query.query).insert(2, "difference").toString()
                nonMatchingDoc = """{ "message" : "$nonMatchingDoc" }"""
                indexDoc(index = index, id = nonMatchingDocId, doc = nonMatchingDoc)
                nonMatchingDocIds.add(nonMatchingDocId)
            }
        }
        assertEquals(indices.size * 5, matchingDocIds.size)

        val response = executeMonitor(monitor.id)

        val output = entityAsMap(response)
        val inputResults = output.stringMap("input_results")
        val errorMessage = inputResults?.get("error")
        @Suppress("UNCHECKED_CAST")
        val searchResult = (inputResults?.get("results") as List<Map<String, Any>>).firstOrNull()
        @Suppress("UNCHECKED_CAST")
        val findings = searchFindings()

        assertEquals(monitor.name, output["monitor_name"])
        assertNull("Unexpected monitor execution failure: $errorMessage", errorMessage)
        findings.findings.forEach {
            val docIds = it.finding.relatedDocIds
            assertTrue(
                "Findings index should not contain a pre-existing doc, but found $it",
                preExistingDocIds.intersect(docIds).isEmpty()
            )
            assertTrue("Found doc that doesn't match query: $it", nonMatchingDocIds.intersect(docIds).isEmpty())
            assertFalse("Found an unexpected finding $it", matchingDocIds.intersect(docIds).isNotEmpty())
        }
    }

    fun `test DocLevelMonitor with IntervalSchedule returns expected template args`() {
        // create a template that invokes all template arg fields we support
        val template = randomTemplateScript(
            "{{ctx.monitor._id}};" + // 0
                "{{ctx.monitor._version}};" + // 1
                "{{ctx.monitor.name}};" + // 2
                "{{ctx.monitor.monitor_type}};" + // 3
                "{{ctx.monitor.enabled}};" + // 4
                "{{ctx.monitor.enabled_time}};" + // 5
                "{{ctx.monitor.last_update_time}};" + // 6
                "{{ctx.monitor.schedule.period.interval}};" + // 7
                "{{ctx.monitor.schedule.period.unit}};" + // 8
                "{{ctx.monitor.inputs.size}};" + // 9
                "{{ctx.monitor.inputs.0.search.indices.size}};" + // 10
                "{{ctx.monitor.inputs.0.search.indices.0}};" + // 11
                "{{ctx.monitor.inputs.0.search.queries.size}};" + // 12
                "{{ctx.monitor.inputs.0.search.queries.0.query}};" + // 13
                "{{ctx.trigger.id}};" + // 14
                "{{ctx.trigger.name}};" + // 15
                "{{ctx.trigger.severity}};" + // 16
                "{{ctx.trigger.condition.script.source}};" + // 17
                "{{ctx.trigger.condition.script.lang}};" + // 18
                "{{ctx.trigger.actions.size}};" + // 19
                "{{ctx.trigger.actions.0.id}};" + // 20
                "{{ctx.trigger.actions.0.name}};" + // 21
                "{{ctx.trigger.actions.0.destination_id}};" + // 22
                "{{ctx.trigger.actions.0.throttle_enabled}}" // 23
        )

        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val index = createTestIndex()

        val docQuery = DocLevelQuery(query = "test_field:us-west-2", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))

        val action = randomAction(template = template, destinationId = createDestination().id)

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action))

        val monitor = randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger))

        indexDoc(index, "1", testDoc)

        val output = entityAsMap(executeMonitor(monitor))

        log.info("output: $output")

        assertEquals(1, output.objectMap("trigger_results").values.size) // ensure that we got actual trigger results
        for (triggerResult in output.objectMap("trigger_results").values) {
            assertEquals(1, triggerResult.objectMap("action_results").values.size)
            for (alertActionResult in triggerResult.objectMap("action_results").values) {
                for (actionResult in alertActionResult.values) {
                    @Suppress("UNCHECKED_CAST") val actionOutput = (actionResult as Map<String, Map<String, String>>)["output"]
                        as Map<String, String>

                    val rawOutput: String = actionOutput["message"] as String // the raw mustache template that just lists all the fields
                    val results: List<String> = rawOutput.split(";")

                    assertEquals(24, results.size) // we asked for 23 fields, make sure we got them

                    // start checking asTemplateArg values
                    assertEquals(results[0], monitor.id) // ctx.monitor.id
                    assertEquals(results[1], monitor.version.toString()) // ctx.monitor.version
                    assertEquals(results[2], monitor.name) // ctx.monitor.name
                    assertEquals(results[3], monitor.monitorType.toString()) // ctx.monitor.monitor_type
                    assertEquals(results[4], monitor.enabled.toString()) // ctx.monitor.enabled
                    assertEquals(results[5], monitor.enabledTime.toString()) // ctx.monitor.enabled_time
                    assertEquals(results[6], monitor.lastUpdateTime.toString()) // ctx.monitor.last_update_time
                    assertEquals(results[7], (monitor.schedule as IntervalSchedule).interval.toString()) // ctx.monitor.schedule.period.interval
                    assertEquals(results[8], (monitor.schedule as IntervalSchedule).unit.toString()) // ctx.monitor.schedule.period.unit
                    assertEquals(results[9], monitor.inputs.size.toString()) // ctx.monitor.inputs.size
                    assertEquals(results[10], (monitor.inputs[0] as DocLevelMonitorInput).indices.size.toString()) // ctx.monitor.inputs.search.indices.size
                    assertEquals(results[11], (monitor.inputs[0] as DocLevelMonitorInput).indices[0]) // ctx.monitor.inputs.search.indices
                    assertEquals(results[12], (monitor.inputs[0] as DocLevelMonitorInput).queries.size.toString()) // ctx.monitor.inputs.search.queries.size
                    assertEquals(results[13], (monitor.inputs[0] as DocLevelMonitorInput).queries[0].query) // ctx.monitor.inputs.search.queries.query
                    assertEquals(results[14], monitor.triggers[0].id) // ctx.trigger.id
                    assertEquals(results[15], monitor.triggers[0].name) // ctx.trigger.name
                    assertEquals(results[16], monitor.triggers[0].severity) // ctx.trigger.severity
                    assertEquals(results[17], (monitor.triggers[0] as DocumentLevelTrigger).condition.idOrCode) // ctx.trigger.condition.script.source
                    assertEquals(results[18], (monitor.triggers[0] as DocumentLevelTrigger).condition.lang) // ctx.trigger.condition.script.lang
                    assertEquals(results[19], (monitor.triggers[0] as DocumentLevelTrigger).actions.size.toString()) // ctx.trigger.actions.size
                    assertEquals(results[20], (monitor.triggers[0] as DocumentLevelTrigger).actions[0].id) // ctx.trigger.actions.id
                    assertEquals(results[21], (monitor.triggers[0] as DocumentLevelTrigger).actions[0].name) // ctx.trigger.actions.name
                    assertEquals(results[22], (monitor.triggers[0] as DocumentLevelTrigger).actions[0].destinationId) // ctx.trigger.actions.destination_id
                    assertEquals(results[23], (monitor.triggers[0] as DocumentLevelTrigger).actions[0].throttleEnabled.toString()) // ctx.trigger.actions.throttle_enabled
                }
            }
        }
    }

    fun `test DocLevelMonitor with CronSchedule returns expected template args`() {
        // create a template that invokes all template arg fields we support
        val template = randomTemplateScript(
            "{{ctx.monitor._id}};" + // 0
                "{{ctx.monitor._version}};" + // 1
                "{{ctx.monitor.name}};" + // 2
                "{{ctx.monitor.monitor_type}};" + // 3
                "{{ctx.monitor.enabled}};" + // 4
                "{{ctx.monitor.enabled_time}};" + // 5
                "{{ctx.monitor.last_update_time}};" + // 6
                "{{ctx.monitor.schedule.cron.expression}};" + // 7
                "{{ctx.monitor.schedule.cron.timezone}};" + // 8
                "{{ctx.monitor.inputs.size}};" + // 9
                "{{ctx.monitor.inputs.0.search.indices.size}};" + // 10
                "{{ctx.monitor.inputs.0.search.indices.0}};" + // 11
                "{{ctx.monitor.inputs.0.search.queries.size}};" + // 12
                "{{ctx.monitor.inputs.0.search.queries.0.query}};" + // 13
                "{{ctx.trigger.id}};" + // 14
                "{{ctx.trigger.name}};" + // 15
                "{{ctx.trigger.severity}};" + // 16
                "{{ctx.trigger.condition.script.source}};" + // 17
                "{{ctx.trigger.condition.script.lang}};" + // 18
                "{{ctx.trigger.actions.size}};" + // 19
                "{{ctx.trigger.actions.0.id}};" + // 20
                "{{ctx.trigger.actions.0.name}};" + // 21
                "{{ctx.trigger.actions.0.destination_id}};" + // 22
                "{{ctx.trigger.actions.0.throttle_enabled}}" // 23
        )

        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val index = createTestIndex()

        val docQuery = DocLevelQuery(query = "test_field:us-west-2", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))

        val action = randomAction(template = template, destinationId = createDestination().id)

        val schedule = CronSchedule(expression = "* * * * *", timezone = randomZone())

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action))

        val monitor = randomDocumentLevelMonitor(inputs = listOf(docLevelInput), schedule = schedule, triggers = listOf(trigger))

        indexDoc(index, "1", testDoc)

        val output = entityAsMap(executeMonitor(monitor))

        log.info("output: $output")

        assertEquals(1, output.objectMap("trigger_results").values.size) // ensure that we got actual trigger results
        for (triggerResult in output.objectMap("trigger_results").values) {
            assertEquals(1, triggerResult.objectMap("action_results").values.size)
            for (alertActionResult in triggerResult.objectMap("action_results").values) {
                for (actionResult in alertActionResult.values) {
                    @Suppress("UNCHECKED_CAST") val actionOutput = (actionResult as Map<String, Map<String, String>>)["output"]
                        as Map<String, String>

                    val rawOutput: String = actionOutput["message"] as String // the raw mustache template that just lists all the fields
                    val results: List<String> = rawOutput.split(";")

                    assertEquals(24, results.size) // we asked for 23 fields, make sure we got them

                    // start checking asTemplateArg values
                    assertEquals(results[0], monitor.id) // ctx.monitor.id
                    assertEquals(results[1], monitor.version.toString()) // ctx.monitor.version
                    assertEquals(results[2], monitor.name) // ctx.monitor.name
                    assertEquals(results[3], monitor.monitorType.toString()) // ctx.monitor.monitor_type
                    assertEquals(results[4], monitor.enabled.toString()) // ctx.monitor.enabled
                    assertEquals(results[5], monitor.enabledTime.toString()) // ctx.monitor.enabled_time
                    assertEquals(results[6], monitor.lastUpdateTime.toString()) // ctx.monitor.last_update_time
                    assertEquals(results[7], (monitor.schedule as CronSchedule).expression) // ctx.monitor.schedule.period.interval
                    assertEquals(results[8], (monitor.schedule as CronSchedule).timezone.toString()) // ctx.monitor.schedule.period.unit
                    assertEquals(results[9], monitor.inputs.size.toString()) // ctx.monitor.inputs.size
                    assertEquals(results[10], (monitor.inputs[0] as DocLevelMonitorInput).indices.size.toString()) // ctx.monitor.inputs.search.indices.size
                    assertEquals(results[11], (monitor.inputs[0] as DocLevelMonitorInput).indices[0]) // ctx.monitor.inputs.search.indices
                    assertEquals(results[12], (monitor.inputs[0] as DocLevelMonitorInput).queries.size.toString()) // ctx.monitor.inputs.search.queries.size
                    assertEquals(results[13], (monitor.inputs[0] as DocLevelMonitorInput).queries[0].query) // ctx.monitor.inputs.search.queries.query
                    assertEquals(results[14], monitor.triggers[0].id) // ctx.trigger.id
                    assertEquals(results[15], monitor.triggers[0].name) // ctx.trigger.name
                    assertEquals(results[16], monitor.triggers[0].severity) // ctx.trigger.severity
                    assertEquals(results[17], (monitor.triggers[0] as DocumentLevelTrigger).condition.idOrCode) // ctx.trigger.condition.script.source
                    assertEquals(results[18], (monitor.triggers[0] as DocumentLevelTrigger).condition.lang) // ctx.trigger.condition.script.lang
                    assertEquals(results[19], (monitor.triggers[0] as DocumentLevelTrigger).actions.size.toString()) // ctx.trigger.actions.size
                    assertEquals(results[20], (monitor.triggers[0] as DocumentLevelTrigger).actions[0].id) // ctx.trigger.actions.id
                    assertEquals(results[21], (monitor.triggers[0] as DocumentLevelTrigger).actions[0].name) // ctx.trigger.actions.name
                    assertEquals(results[22], (monitor.triggers[0] as DocumentLevelTrigger).actions[0].destinationId) // ctx.trigger.actions.destination_id
                    assertEquals(results[23], (monitor.triggers[0] as DocumentLevelTrigger).actions[0].throttleEnabled.toString()) // ctx.trigger.actions.throttle_enabled
                }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    /** helper that returns a field in a json map whose values are all json objects */
    private fun Map<String, Any>.objectMap(key: String): Map<String, Map<String, Any>> {
        return this[key] as Map<String, Map<String, Any>>
    }
}
