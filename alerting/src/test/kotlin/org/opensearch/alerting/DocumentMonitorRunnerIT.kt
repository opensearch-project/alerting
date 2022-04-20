/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.core.model.DocLevelMonitorInput
import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.client.Response
import org.opensearch.client.ResponseException
import org.opensearch.script.Script
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.MILLIS

class DocumentMonitorRunnerIT : AlertingRestTestCase() {

    fun `test execute monitor with dryrun`() {

        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val index = createTestIndex()

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docReturningInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))

        val action = randomAction(template = randomTemplateScript("Hello {{ctx.monitor.name}}"), destinationId = createDestination().id)
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docReturningInput),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action)))
        )

        indexDoc(index, "1", testDoc)

        val response = executeMonitor(monitor, params = DRYRUN_MONITOR)

        val output = entityAsMap(response)
        assertEquals(monitor.name, output["monitor_name"])

        assertEquals(1, output.objectMap("trigger_results").values.size)

        for (triggerResult in output.objectMap("trigger_results").values) {
            assertEquals(1, triggerResult.objectMap("action_results").values.size)
            for (actionResult in triggerResult.objectMap("action_results").values) {
                @Suppress("UNCHECKED_CAST") val actionOutput = actionResult["output"] as Map<String, String>
                assertEquals("Hello ${monitor.name}", actionOutput["subject"])
                assertEquals("Hello ${monitor.name}", actionOutput["message"])
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
        val docReturningInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = randomDocumentLevelMonitor(inputs = listOf(docReturningInput), triggers = listOf(trigger))

        indexDoc(testIndex, "1", testDoc)
        indexDoc(testIndex, "5", testDoc)

        val response = executeMonitor(monitor, params = DRYRUN_MONITOR)

        val output = entityAsMap(response)

        assertEquals(monitor.name, output["monitor_name"])
        @Suppress("UNCHECKED_CAST")
        val searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        val matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 1, matchingDocsToQuery.size)
        assertTrue("Incorrect search result", matchingDocsToQuery.contains("5|$testIndex"))
    }

    fun `test execute monitor generates alerts and findings`() {
        putFindingMappings()
        val testIndex = createTestIndex()
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docReturningInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docReturningInput), triggers = listOf(trigger)))
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

        // TODO: modify findings such that there is a finding per document, so this test will need to be modified
        val findings = searchFindings(monitor)
        assertEquals("Findings saved for test monitor", 2, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
        assertTrue("Findings saved for test monitor", findings[1].relatedDocIds.contains("5"))
    }

    fun `test document-level monitor when alias only has write index with 0 docs`() {
        // Monitor should execute, but create 0 findings.
        val alias = createTestAlias(includeWriteIndex = false)
        val indices = alias[alias.keys.first()]?.keys?.toList() as List<String>
        val query = randomDocLevelQuery(tags = listOf())
        val input = randomDocLevelMonitorInput(indices = indices, queries = listOf(query))
        val trigger = randomDocLevelTrigger(condition = Script("params${query.id}"))
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
        val findings = if (searchResult == null) listOf()
        else searchResult?.stringMap("hits")?.getOrDefault("hits", listOf<List<Map<String, Any>>>()) as List<Map<String, Any>>

        assertEquals(monitor.name, output["monitor_name"])
        assertNull("Unexpected monitor execution failure: $errorMessage", errorMessage)
        findings.forEach {
            val findingQueryId = it["queryId"]
            assertNotEquals("No findings should exist with queryId ${query.id}, but found: $it", query.id, findingQueryId)
        }
    }

    fun `test document-level monitor when docs exist prior to monitor creation`() {
        // FIXME: Consider renaming this test case
        // Only new docs should create findings.
        val alias = createTestAlias(includeWriteIndex = false)
        val indices = alias[alias.keys.first()]?.keys?.toList() as List<String>
        val query = randomDocLevelQuery(tags = listOf())
        val input = randomDocLevelMonitorInput(indices = indices, queries = listOf(query))
        val trigger = randomDocLevelTrigger(condition = Script("params${query.id}"))

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
        val findings = if (searchResult == null) listOf()
        else searchResult?.stringMap("hits")?.getOrDefault("hits", listOf<List<Map<String, Any>>>()) as List<Map<String, Any>>

        assertEquals(monitor.name, output["monitor_name"])
        assertNull("Unexpected monitor execution failure: $errorMessage", errorMessage)
        findings.forEach {
            val findingDocId = it["id"] as String
            assertFalse("Findings index should not contain a pre-existing doc, but found $it", preExistingDocIds.contains(findingDocId))
        }
    }

    fun `test document-level monitor when alias indices only contain docs that match query`() {
        // Only new docs should create findings.
        val alias = createTestAlias(includeWriteIndex = false)
        val indices = alias[alias.keys.first()]?.keys?.toList() as List<String>
        val query = randomDocLevelQuery(tags = listOf())
        val input = randomDocLevelMonitorInput(indices = indices, queries = listOf(query))
        val trigger = randomDocLevelTrigger(condition = Script("params${query.id}"))

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
        val findings = if (searchResult == null) listOf()
        else searchResult?.stringMap("hits")?.getOrDefault("hits", listOf<List<Map<String, Any>>>()) as List<Map<String, Any>>

        assertEquals(monitor.name, output["monitor_name"])
        assertNull("Unexpected monitor execution failure: $errorMessage", errorMessage)
        findings.forEach {
            val findingDocId = it["id"] as String
            assertFalse("Findings index should not contain a pre-existing doc, but found $it", preExistingDocIds.contains(findingDocId))
            assertTrue("Found an unexpected finding $it", newDocIds.contains(findingDocId))
        }
    }

    fun `test document-level monitor when alias indices contain docs that do and do not match query`() {
        // Only matching docs should create findings.
        val alias = createTestAlias(includeWriteIndex = false)
        val indices = alias[alias.keys.first()]?.keys?.toList() as List<String>
        val query = randomDocLevelQuery(tags = listOf())
        val input = randomDocLevelMonitorInput(indices = indices, queries = listOf(query))
        val trigger = randomDocLevelTrigger(condition = Script("params${query.id}"))

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
        val findings = if (searchResult == null) listOf()
        else searchResult?.stringMap("hits")?.getOrDefault("hits", listOf<List<Map<String, Any>>>()) as List<Map<String, Any>>

        assertEquals(monitor.name, output["monitor_name"])
        assertNull("Unexpected monitor execution failure: $errorMessage", errorMessage)
        findings.forEach {
            val findingDocId = it["id"] as String
            assertFalse("Findings index should not contain a pre-existing doc, but found $it", preExistingDocIds.contains(findingDocId))
            assertFalse("Found doc that doesn't match query: $it", nonMatchingDocIds.contains(findingDocId))
            assertTrue("Found an unexpected finding $it", matchingDocIds.contains(findingDocId))
        }
    }

    @Suppress("UNCHECKED_CAST")
    /** helper that returns a field in a json map whose values are all json objects */
    private fun Map<String, Any>.objectMap(key: String): Map<String, Map<String, Any>> {
        return this[key] as Map<String, Map<String, Any>>
    }
}
