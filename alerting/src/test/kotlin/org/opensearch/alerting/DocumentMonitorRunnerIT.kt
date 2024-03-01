/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALL_ALERT_INDEX_PATTERN
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALL_FINDING_INDEX_PATTERN
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.client.Response
import org.opensearch.client.ResponseException
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.action.ActionExecutionPolicy
import org.opensearch.commons.alerting.model.action.AlertCategory
import org.opensearch.commons.alerting.model.action.PerAlertActionScope
import org.opensearch.commons.alerting.model.action.PerExecutionActionScope
import org.opensearch.core.rest.RestStatus
import org.opensearch.script.Script
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.MILLIS
import java.util.Locale

class DocumentMonitorRunnerIT : AlertingRestTestCase() {

    fun `test execute monitor with dryrun`() {

        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val index = createTestIndex()

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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

        // ensure doc level query is deleted on dry run
        val request = """{
            "size": 10,
            "query": {
                "match_all": {}
            }
        }"""
        var httpResponse = adminClient().makeRequest(
            "GET", "/${monitor.dataSources.queryIndex}/_search",
            StringEntity(request, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search failed", RestStatus.OK, httpResponse.restStatus())
        var searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, httpResponse.entity.content))
        searchResponse.hits.totalHits?.let { assertEquals("Query saved in query index", 0L, it.value) }
    }

    fun `test dryrun execute monitor with queryFieldNames set up with correct field`() {

        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val index = createTestIndex()

        val docQuery =
            DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf(), queryFieldNames = listOf("test_field"))
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

    fun `test seq_no calculation correctness when docs are deleted`() {
        adminClient().updateSettings(AlertingSettings.DOC_LEVEL_MONITOR_SHARD_FETCH_SIZE.key, 2)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val index = createTestIndex()

        val docQuery =
            DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))

        val action = randomAction(template = randomTemplateScript("Hello {{ctx.monitor.name}}"), destinationId = createDestination().id)
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action)))
        )

        indexDoc(index, "1", testDoc)
        indexDoc(index, "2", testDoc)
        indexDoc(index, "3", testDoc)
        indexDoc(index, "4", testDoc)
        indexDoc(index, "5", testDoc)
        indexDoc(index, "11", testDoc)
        indexDoc(index, "21", testDoc)
        indexDoc(index, "31", testDoc)
        indexDoc(index, "41", testDoc)
        indexDoc(index, "51", testDoc)

        deleteDoc(index, "51")
        val response = executeMonitor(monitor, params = mapOf("dryrun" to "false"))

        val output = entityAsMap(response)
        assertEquals(monitor.name, output["monitor_name"])

        for (triggerResult in output.objectMap("trigger_results").values) {
            assertEquals(9, triggerResult.objectMap("action_results").values.size)
        }
    }

    fun `test dryrun execute monitor with queryFieldNames set up with wrong field`() {

        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val index = createTestIndex()
        // using wrong field name
        val docQuery = DocLevelQuery(
            query = "test_field:\"us-west-2\"",
            name = "3",
            fields = listOf(),
            queryFieldNames = listOf("wrong_field")
        )
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))

        val action = randomAction(template = randomTemplateScript("Hello {{ctx.monitor.name}}"), destinationId = createDestination().id)
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action)))
        )

        indexDoc(index, "1", testDoc)
        indexDoc(index, "2", testDoc)
        indexDoc(index, "3", testDoc)
        indexDoc(index, "4", testDoc)
        indexDoc(index, "5", testDoc)

        val response = executeMonitor(monitor, params = DRYRUN_MONITOR)

        val output = entityAsMap(response)
        assertEquals(monitor.name, output["monitor_name"])

        assertEquals(1, output.objectMap("trigger_results").values.size)

        for (triggerResult in output.objectMap("trigger_results").values) {
            assertEquals(0, triggerResult.objectMap("action_results").values.size)
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

    fun `test fetch_query_field_names setting is disabled by configuring queryFieldNames set up with wrong field still works`() {
        adminClient().updateSettings(AlertingSettings.DOC_LEVEL_MONITOR_FETCH_ONLY_QUERY_FIELDS_ENABLED.key, "false")
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val index = createTestIndex()
        // using wrong field name
        val docQuery = DocLevelQuery(
            query = "test_field:\"us-west-2\"",
            name = "3",
            fields = listOf(),
            queryFieldNames = listOf("wrong_field")
        )
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

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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

        // ensure doc level query is deleted on dry run
        val request = """{
            "size": 10,
            "query": {
                "match_all": {}
            }
        }"""
        var httpResponse = adminClient().makeRequest(
            "GET", "/${monitor.dataSources.queryIndex}/_search",
            StringEntity(request, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search failed", RestStatus.OK, httpResponse.restStatus())
        var searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, httpResponse.entity.content))
        searchResponse.hits.totalHits?.let { assertEquals("Query saved in query index", 0L, it.value) }
    }

    fun `test execute monitor returns search result with dryrun then without dryrun ensure dry run query not saved`() {
        val testIndex = createTestIndex()
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger))

        indexDoc(testIndex, "1", testDoc)
        indexDoc(testIndex, "2", testDoc)

        val response = executeMonitor(monitor, params = DRYRUN_MONITOR)

        val output = entityAsMap(response)

        assertEquals(monitor.name, output["monitor_name"])
        @Suppress("UNCHECKED_CAST")
        val searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        val matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 2, matchingDocsToQuery.size)
        assertTrue("Incorrect search result", matchingDocsToQuery.contains("1|$testIndex"))
        assertTrue("Incorrect search result", matchingDocsToQuery.contains("2|$testIndex"))

        // ensure doc level query is deleted on dry run
        val request = """{
            "size": 10,
            "query": {
                "match_all": {}
            }
        }"""
        var httpResponse = adminClient().makeRequest(
            "GET", "/${monitor.dataSources.queryIndex}/_search",
            StringEntity(request, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search failed", RestStatus.OK, httpResponse.restStatus())
        var searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, httpResponse.entity.content))
        searchResponse.hits.totalHits?.let { assertEquals(0L, it.value) }

        // create and execute second monitor not as dryrun
        val testIndex2 = createTestIndex("test1")
        val testTime2 = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc2 = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime2",
            "test_field" : "us-east-1"
        }"""

        val docQuery2 = DocLevelQuery(query = "test_field:\"us-east-1\"", name = "3", fields = listOf())
        val docLevelInput2 = DocLevelMonitorInput("description", listOf(testIndex2), listOf(docQuery2))

        val trigger2 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor2 = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput2), triggers = listOf(trigger2)))
        assertNotNull(monitor2.id)

        indexDoc(testIndex2, "1", testDoc2)
        indexDoc(testIndex2, "5", testDoc2)

        val response2 = executeMonitor(monitor2.id)
        val output2 = entityAsMap(response2)

        assertEquals(monitor2.name, output2["monitor_name"])
        @Suppress("UNCHECKED_CAST")
        val searchResult2 = (output2.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        val matchingDocsToQuery2 = searchResult2[docQuery2.id] as List<String>
        assertEquals("Incorrect search result", 2, matchingDocsToQuery2.size)
        assertTrue("Incorrect search result", matchingDocsToQuery2.containsAll(listOf("1|$testIndex2", "5|$testIndex2")))

        val alerts = searchAlertsWithFilter(monitor2)
        assertEquals("Alert saved for test monitor", 2, alerts.size)

        val findings = searchFindings(monitor2)
        assertEquals("Findings saved for test monitor", 2, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("5"))
        assertTrue("Findings saved for test monitor", findings[1].relatedDocIds.contains("1"))

        // ensure query from second monitor was saved
        val expectedQueries = listOf("test_field_test1_${monitor2.id}:\"us-east-1\"")
        httpResponse = adminClient().makeRequest(
            "GET", "/${monitor.dataSources.queryIndex}/_search",
            StringEntity(request, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search failed", RestStatus.OK, httpResponse.restStatus())
        searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, httpResponse.entity.content))
        searchResponse.hits.forEach { hit ->
            val query = ((hit.sourceAsMap["query"] as Map<String, Any>)["query_string"] as Map<String, Any>)["query"]
            assertTrue(expectedQueries.contains(query))
        }
        searchResponse.hits.totalHits?.let { assertEquals("Query saved in query index", 1L, it.value) }
    }

    fun `test execute monitor generates alerts and findings`() {
        val testIndex = createTestIndex()
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("5"))
        assertTrue("Findings saved for test monitor", findings[1].relatedDocIds.contains("1"))
    }

    fun `test execute monitor with tag as trigger condition generates alerts and findings`() {
        val testIndex = createTestIndex()
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", tags = listOf("test_tag"), fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = Script("query[tag=test_tag]"))
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
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("5"))
        assertTrue("Findings saved for test monitor", findings[1].relatedDocIds.contains("1"))
    }

    fun `test execute monitor input error`() {
        val testIndex = createTestIndex()
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", tags = listOf("test_tag"), fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))
        assertNotNull(monitor.id)

        deleteIndex(testIndex)

        val response = executeMonitor(monitor.id)

        val output = entityAsMap(response)
        assertEquals(monitor.name, output["monitor_name"])
        @Suppress("UNCHECKED_CAST")
        val inputResults = output.stringMap("input_results")
        assertTrue("Missing monitor error message", (inputResults?.get("error") as String).isNotEmpty())

        val alerts = searchAlerts(monitor)
        assertEquals("Alert not saved", 1, alerts.size)
        assertEquals("Alert status is incorrect", Alert.State.ERROR, alerts[0].state)
    }

    fun `test execute monitor generates alerts and findings with per alert execution for actions`() {
        val testIndex = createTestIndex()
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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

        refreshAllIndices()

        val alerts = searchAlertsWithFilter(monitor)
        assertEquals("Alert saved for test monitor", 2, alerts.size)

        val findings = searchFindings(monitor)
        assertEquals("Findings saved for test monitor", 2, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("5"))
        assertTrue("Findings saved for test monitor", findings[1].relatedDocIds.contains("1"))
    }

    fun `test execute monitor generates alerts and findings with per trigger execution for actions`() {
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
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("5"))
        assertTrue("Findings saved for test monitor", findings[1].relatedDocIds.contains("1"))
    }

    fun `test execute monitor with wildcard index that generates alerts and findings for EQUALS query operator`() {
        val testIndexPrefix = "test-index-${randomAlphaOfLength(10).lowercase(Locale.ROOT)}"
        val testQueryName = "wildcard-test-query"
        val testIndex = createTestIndex("${testIndexPrefix}1")
        val testIndex2 = createTestIndex("${testIndexPrefix}2")

        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = testQueryName, fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf("$testIndexPrefix*"), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = Script("query[name=$testQueryName]"))
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
        val foundFindings = findings.filter { it.relatedDocIds.contains("1") || it.relatedDocIds.contains("5") }
        assertEquals("Didn't find findings for docs 1 and 5", 2, foundFindings.size)
    }

    fun `test execute monitor for bulk index findings`() {
        val testIndexPrefix = "test-index-${randomAlphaOfLength(10).lowercase(Locale.ROOT)}"
        val testQueryName = "wildcard-test-query"
        val testIndex = createTestIndex("${testIndexPrefix}1")
        val testIndex2 = createTestIndex("${testIndexPrefix}2")

        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = testQueryName, fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf("$testIndexPrefix*"), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = Script("query[name=$testQueryName]"))
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))
        assertNotNull(monitor.id)

        for (i in 0 until 9) {
            indexDoc(testIndex, i.toString(), testDoc)
        }
        indexDoc(testIndex2, "3", testDoc)
        adminClient().updateSettings("plugins.alerting.alert_findings_indexing_batch_size", 2)

        val response = executeMonitor(monitor.id)

        val output = entityAsMap(response)

        assertEquals(monitor.name, output["monitor_name"])
        @Suppress("UNCHECKED_CAST")
        val searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        val matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Correct search result", 10, matchingDocsToQuery.size)
        assertTrue("Correct search result", matchingDocsToQuery.containsAll(listOf("1|$testIndex", "2|$testIndex", "3|$testIndex2")))

        val alerts = searchAlertsWithFilter(monitor)
        assertEquals("Alert saved for test monitor", 10, alerts.size)

        val findings = searchFindings(monitor)
        assertEquals("Findings saved for test monitor", 10, findings.size)
        val foundFindings =
            findings.filter { it.relatedDocIds.contains("1") || it.relatedDocIds.contains("2") || it.relatedDocIds.contains("3") }
        assertEquals("Found findings for all docs", 4, foundFindings.size)
    }

    fun `test execute monitor with wildcard index that generates alerts and findings for NOT EQUALS query operator`() {
        val testIndexPrefix = "test-index-${randomAlphaOfLength(10).lowercase(Locale.ROOT)}"
        val testQueryName = "wildcard-test-query"
        val testIndex = createTestIndex("${testIndexPrefix}1")
        val testIndex2 = createTestIndex("${testIndexPrefix}2")

        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "NOT (test_field:\"us-west-1\")", name = testQueryName, fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf("$testIndexPrefix*"), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = Script("query[name=$testQueryName]"))
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
        val foundFindings = findings.filter { it.relatedDocIds.contains("1") || it.relatedDocIds.contains("5") }
        assertEquals("Didn't find findings for docs 1 and 5", 2, foundFindings.size)
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

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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

        var foundFindings = findings.filter { it.relatedDocIds.contains("1") || it.relatedDocIds.contains("5") }
        assertEquals("Findings saved for test monitor expected 1 and 5", 2, foundFindings.size)

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

        foundFindings = findings.filter {
            it.relatedDocIds.contains("14") || it.relatedDocIds.contains("51") || it.relatedDocIds.contains("10")
        }
        assertEquals("Findings saved for test monitor expected 14, 51 and 10", 3, foundFindings.size)
    }

    fun `test execute monitor with indices having fields with same name but different data types`() {
        val testIndex = createTestIndex(
            "test1",
            """"properties": {
                    "source.device.port": { "type": "long" },
                    "source.device.hwd.id": { "type": "long" },
                    "nested_field": {
                      "type": "nested",
                      "properties": {
                        "test1": {
                          "type": "keyword"
                        }
                      }
                    },
                    "my_join_field": { 
                      "type": "join",
                      "relations": {
                         "question": "answer" 
                      }
                   },
                   "test_field" : { "type" : "integer" }
                }
            """.trimIndent()
        )
        var testDoc = """{
            "source" : { "device": {"port" : 12345 } },
            "nested_field": { "test1": "some text" },
            "test_field": 12345
        }"""

        val docQuery1 = DocLevelQuery(
            query = "(source.device.port:12345 AND test_field:12345) OR source.device.hwd.id:12345",
            name = "4",
            fields = listOf()
        )
        val docQuery2 = DocLevelQuery(
            query = "(source.device.port:\"12345\" AND test_field:\"12345\") OR source.device.hwd.id:\"12345\"",
            name = "5",
            fields = listOf()
        )
        val docLevelInput = DocLevelMonitorInput("description", listOf("test*"), listOf(docQuery1, docQuery2))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))
        assertNotNull(monitor.id)

        indexDoc(testIndex, "1", testDoc)
        executeMonitor(monitor.id)

        var alerts = searchAlertsWithFilter(monitor)
        assertEquals("Alert saved for test monitor", 1, alerts.size)

        var findings = searchFindings(monitor)
        assertEquals("Findings saved for test monitor", 1, findings.size)

        // clear previous findings and alerts
        deleteIndex(ALL_FINDING_INDEX_PATTERN)
        deleteIndex(ALL_ALERT_INDEX_PATTERN)

        indexDoc(testIndex, "2", testDoc)

        // no fields expanded as only index test1 is present
        val oldExpectedQueries = listOf(
            "(source.device.port_test__${monitor.id}:12345 AND test_field_test__${monitor.id}:12345) OR " +
                "source.device.hwd.id_test__${monitor.id}:12345",
            "(source.device.port_test__${monitor.id}:\"12345\" AND test_field_test__${monitor.id}:\"12345\") " +
                "OR source.device.hwd.id_test__${monitor.id}:\"12345\""
        )

        val request = """{
            "size": 10,
            "query": {
                "match_all": {}
            }
        }"""
        var httpResponse = adminClient().makeRequest(
            "GET", "/${monitor.dataSources.queryIndex}/_search",
            StringEntity(request, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search failed", RestStatus.OK, httpResponse.restStatus())
        var searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, httpResponse.entity.content))
        searchResponse.hits.forEach { hit ->
            val query = ((hit.sourceAsMap["query"] as Map<String, Any>)["query_string"] as Map<String, Any>)["query"]
            assertTrue(oldExpectedQueries.contains(query))
        }

        val testIndex2 = createTestIndex(
            "test2",
            """
                "properties" : {
                  "test_strict_date_time" : { "type" : "date", "format" : "strict_date_time" },
                  "test_field" : { "type" : "keyword" },
                  "number" : { "type" : "keyword" }
                }
            """.trimIndent()
        )
        testDoc = """{
            "source" : { "device": {"port" : "12345" } },
            "nested_field": { "test1": "some text" },
            "test_field": "12345"
        }"""
        indexDoc(testIndex2, "1", testDoc)
        executeMonitor(monitor.id)

        // only fields source.device.port & test_field is expanded as they have same name but different data types
        // in indices test1 & test2
        val newExpectedQueries = listOf(
            "(source.device.port_test2_${monitor.id}:12345 AND test_field_test2_${monitor.id}:12345) " +
                "OR source.device.hwd.id_test__${monitor.id}:12345",
            "(source.device.port_test1_${monitor.id}:12345 AND test_field_test1_${monitor.id}:12345) " +
                "OR source.device.hwd.id_test__${monitor.id}:12345",
            "(source.device.port_test2_${monitor.id}:\"12345\" AND test_field_test2_${monitor.id}:\"12345\") " +
                "OR source.device.hwd.id_test__${monitor.id}:\"12345\"",
            "(source.device.port_test1_${monitor.id}:\"12345\" AND test_field_test1_${monitor.id}:\"12345\") " +
                "OR source.device.hwd.id_test__${monitor.id}:\"12345\""
        )

        alerts = searchAlertsWithFilter(monitor)
        assertEquals("Alert saved for test monitor", 2, alerts.size)

        findings = searchFindings(monitor)
        assertEquals("Findings saved for test monitor", 2, findings.size)

        httpResponse = adminClient().makeRequest(
            "GET", "/${monitor.dataSources.queryIndex}/_search",
            StringEntity(request, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search failed", RestStatus.OK, httpResponse.restStatus())
        searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, httpResponse.entity.content))
        searchResponse.hits.forEach { hit ->
            val query = ((hit.sourceAsMap["query"] as Map<String, Any>)["query_string"] as Map<String, Any>)["query"]
            assertTrue(oldExpectedQueries.contains(query) || newExpectedQueries.contains(query))
        }
    }

    fun `test execute monitor with indices having fields with same name but with different nesting`() {
        val testIndex = createTestIndex(
            "test1",
            """"properties": {
                    "nested_field": {
                      "type": "nested",
                      "properties": {
                        "test1": {
                          "type": "keyword"
                        }
                      }
                    }
                }
            """.trimIndent()
        )

        val testIndex2 = createTestIndex(
            "test2",
            """"properties": {
                      "nested_field": {
                          "properties": {
                            "test1": {
                              "type": "keyword"
                            }
                          }
                        }
                    }
            """.trimIndent()
        )
        val testDoc = """{
            "nested_field": { "test1": "12345" }
        }"""

        val docQuery = DocLevelQuery(
            query = "nested_field.test1:\"12345\"",
            name = "5",
            fields = listOf()
        )
        val docLevelInput = DocLevelMonitorInput("description", listOf("test*"), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))
        assertNotNull(monitor.id)

        indexDoc(testIndex, "1", testDoc)
        indexDoc(testIndex2, "1", testDoc)

        executeMonitor(monitor.id)

        val alerts = searchAlertsWithFilter(monitor)
        assertEquals("Alert saved for test monitor", 2, alerts.size)

        val findings = searchFindings(monitor)
        assertEquals("Findings saved for test monitor", 2, findings.size)

        // as mappings of source.id & test_field are different so, both of them expands
        val expectedQueries = listOf(
            "nested_field.test1_test__${monitor.id}:\"12345\""
        )

        val request = """{
            "size": 10,
            "query": {
                "match_all": {}
            }
        }"""
        var httpResponse = adminClient().makeRequest(
            "GET", "/${monitor.dataSources.queryIndex}/_search",
            StringEntity(request, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search failed", RestStatus.OK, httpResponse.restStatus())
        var searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, httpResponse.entity.content))
        searchResponse.hits.forEach { hit ->
            val query = ((hit.sourceAsMap["query"] as Map<String, Any>)["query_string"] as Map<String, Any>)["query"]
            assertTrue(expectedQueries.contains(query))
        }
    }

    fun `test execute monitor with indices having fields with same name but different field mappings`() {
        val testIndex = createTestIndex(
            "test1",
            """"properties": {
                    "source": {
                        "properties": {
                            "id": {
                                "type":"text",
                                "analyzer":"whitespace" 
                            }
                        }
                    },
                   "test_field" : {
                        "type":"text",
                        "analyzer":"whitespace"
                    }
                }
            """.trimIndent()
        )

        val testIndex2 = createTestIndex(
            "test2",
            """"properties": {
                    "source": {
                        "properties": {
                            "id": {
                                "type":"text"
                            }
                        }
                    },
                   "test_field" : {
                        "type":"text"
                    }
                }
            """.trimIndent()
        )
        val testDoc = """{
            "source" : {"id" : "12345" },
            "nested_field": { "test1": "some text" },
            "test_field": "12345"
        }"""

        val docQuery = DocLevelQuery(
            query = "test_field:\"12345\" AND source.id:\"12345\"",
            name = "5",
            fields = listOf()
        )
        val docLevelInput = DocLevelMonitorInput("description", listOf("test*"), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))
        assertNotNull(monitor.id)

        indexDoc(testIndex, "1", testDoc)
        indexDoc(testIndex2, "1", testDoc)

        executeMonitor(monitor.id)

        val alerts = searchAlertsWithFilter(monitor)
        assertEquals("Alert saved for test monitor", 2, alerts.size)

        val findings = searchFindings(monitor)
        assertEquals("Findings saved for test monitor", 2, findings.size)

        // as mappings of source.id & test_field are different so, both of them expands
        val expectedQueries = listOf(
            "test_field_test2_${monitor.id}:\"12345\" AND source.id_test2_${monitor.id}:\"12345\"",
            "test_field_test1_${monitor.id}:\"12345\" AND source.id_test1_${monitor.id}:\"12345\""
        )

        val request = """{
            "size": 10,
            "query": {
                "match_all": {}
            }
        }"""
        var httpResponse = adminClient().makeRequest(
            "GET", "/${monitor.dataSources.queryIndex}/_search",
            StringEntity(request, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search failed", RestStatus.OK, httpResponse.restStatus())
        var searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, httpResponse.entity.content))
        searchResponse.hits.forEach { hit ->
            val query = ((hit.sourceAsMap["query"] as Map<String, Any>)["query_string"] as Map<String, Any>)["query"]
            assertTrue(expectedQueries.contains(query))
        }
    }

    fun `test execute monitor with indices having fields with same name but different field mappings in multiple indices`() {
        val testIndex = createTestIndex(
            "test1",
            """"properties": {
                    "source": {
                        "properties": {
                            "device": {
                                "properties": {
                                    "hwd": {
                                        "properties": {
                                            "id": {
                                                "type":"text",
                                                "analyzer":"whitespace" 
                                            }
                                        }
                                    } 
                                }
                            }
                        }
                    },
                   "test_field" : {
                        "type":"text" 
                    }
                }
            """.trimIndent()
        )

        val testIndex2 = createTestIndex(
            "test2",
            """"properties": {
                    "test_field" : {
                        "type":"keyword"
                    }
                }
            """.trimIndent()
        )

        val testIndex4 = createTestIndex(
            "test4",
            """"properties": {
                   "source": {
                        "properties": {
                            "device": {
                                "properties": {
                                    "hwd": {
                                        "properties": {
                                            "id": {
                                                "type":"text"
                                            }
                                        }
                                    } 
                                }
                            }
                        }
                    },
                   "test_field" : {
                        "type":"text" 
                    }
                }
            """.trimIndent()
        )

        val testDoc1 = """{
            "source" : {"device" : {"hwd" : {"id" : "12345"}} },
            "nested_field": { "test1": "some text" }
        }"""
        val testDoc2 = """{
            "nested_field": { "test1": "some text" },
            "test_field": "12345"
        }"""

        val docQuery1 = DocLevelQuery(
            query = "test_field:\"12345\"",
            name = "4",
            fields = listOf()
        )
        val docQuery2 = DocLevelQuery(
            query = "source.device.hwd.id:\"12345\"",
            name = "5",
            fields = listOf()
        )

        val docLevelInput = DocLevelMonitorInput("description", listOf("test*"), listOf(docQuery1, docQuery2))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))
        assertNotNull(monitor.id)

        indexDoc(testIndex4, "1", testDoc1)
        indexDoc(testIndex2, "1", testDoc2)
        indexDoc(testIndex, "1", testDoc1)
        indexDoc(testIndex, "2", testDoc2)

        executeMonitor(monitor.id)

        val alerts = searchAlertsWithFilter(monitor)
        assertEquals("Alert saved for test monitor", 4, alerts.size)

        val findings = searchFindings(monitor)
        assertEquals("Findings saved for test monitor", 4, findings.size)

        val request = """{
            "size": 0,
            "query": {
                "match_all": {}
            }
        }"""
        val httpResponse = adminClient().makeRequest(
            "GET", "/${monitor.dataSources.queryIndex}/_search",
            StringEntity(request, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search failed", RestStatus.OK, httpResponse.restStatus())

        val searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, httpResponse.entity.content))
        searchResponse.hits.totalHits?.let { assertEquals(5L, it.value) }
    }

    fun `test no of queries generated for document-level monitor based on wildcard indexes`() {
        val testIndex = createTestIndex("test1")
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf("test*"), listOf(docQuery))

        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))
        assertNotNull(monitor.id)

        indexDoc(testIndex, "1", testDoc)
        executeMonitor(monitor.id)

        val request = """{
            "size": 0,
            "query": {
                "match_all": {}
            }
        }"""
        var httpResponse = adminClient().makeRequest(
            "GET", "/${monitor.dataSources.queryIndex}/_search",
            StringEntity(request, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search failed", RestStatus.OK, httpResponse.restStatus())

        var searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, httpResponse.entity.content))
        searchResponse.hits.totalHits?.let { assertEquals(1L, it.value) }

        val testIndex2 = createTestIndex("test2")
        indexDoc(testIndex2, "1", testDoc)
        executeMonitor(monitor.id)

        httpResponse = adminClient().makeRequest(
            "GET", "/${monitor.dataSources.queryIndex}/_search",
            StringEntity(request, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search failed", RestStatus.OK, httpResponse.restStatus())

        searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, httpResponse.entity.content))
        searchResponse.hits.totalHits?.let { assertEquals(1L, it.value) }
    }

    fun `test execute monitor with new index added after first execution that generates alerts and findings from new query`() {
        val testIndex = createTestIndex("test1")
        val testIndex2 = createTestIndex("test2")
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val docQuery1 = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docQuery2 = DocLevelQuery(query = "test_field_new:\"us-west-2\"", name = "4", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf("test*"), listOf(docQuery1, docQuery2))

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

        var foundFindings = findings.filter { it.relatedDocIds.contains("1") || it.relatedDocIds.contains("5") }
        assertEquals("Findings saved for test monitor expected 1 and 5", 2, foundFindings.size)

        // clear previous findings and alerts
        deleteIndex(ALL_FINDING_INDEX_PATTERN)
        deleteIndex(ALL_ALERT_INDEX_PATTERN)

        val testDocNew = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field_new" : "us-west-2"
        }"""

        val testIndex3 = createTestIndex("test3")
        indexDoc(testIndex3, "10", testDocNew)

        val response = executeMonitor(monitor.id)

        val output = entityAsMap(response)

        assertEquals(monitor.name, output["monitor_name"])
        @Suppress("UNCHECKED_CAST")
        val searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        val matchingDocsToQuery = searchResult[docQuery2.id] as List<String>
        assertEquals("Incorrect search result", 1, matchingDocsToQuery.size)
        assertTrue("Incorrect search result", matchingDocsToQuery.containsAll(listOf("10|$testIndex3")))

        alerts = searchAlertsWithFilter(monitor)
        assertEquals("Alert saved for test monitor", 1, alerts.size)

        findings = searchFindings(monitor)
        assertEquals("Findings saved for test monitor", 1, findings.size)

        foundFindings = findings.filter {
            it.relatedDocIds.contains("10")
        }
        assertEquals("Findings saved for test monitor expected 10", 1, foundFindings.size)
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

    fun `test document-level monitor when datastreams contain docs that do match query`() {
        val dataStreamName = "test-datastream"
        createDataStream(
            dataStreamName,
            """
                "properties" : {
                  "test_strict_date_time" : { "type" : "date", "format" : "strict_date_time" },
                  "test_field" : { "type" : "keyword" },
                  "number" : { "type" : "keyword" }
                }
            """.trimIndent(),
            false
        )

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(dataStreamName), listOf(docQuery))

        val action = randomAction(template = randomTemplateScript("Hello {{ctx.monitor.name}}"), destinationId = createDestination().id)
        val monitor = createMonitor(
            randomDocumentLevelMonitor(
                inputs = listOf(docLevelInput),
                triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action)))
            )
        )

        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "@timestamp": "$testTime",
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        indexDoc(dataStreamName, "1", testDoc)
        var response = executeMonitor(monitor.id)
        var output = entityAsMap(response)
        var searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        var matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 1, matchingDocsToQuery.size)

        rolloverDatastream(dataStreamName)
        indexDoc(dataStreamName, "2", testDoc)
        response = executeMonitor(monitor.id)
        output = entityAsMap(response)
        searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 1, matchingDocsToQuery.size)

        deleteDataStream(dataStreamName)
    }

    fun `test document-level monitor when datastreams contain docs across read-only indices that do match query`() {
        val dataStreamName = "test-datastream"
        createDataStream(
            dataStreamName,
            """
                "properties" : {
                  "test_strict_date_time" : { "type" : "date", "format" : "strict_date_time" },
                  "test_field" : { "type" : "keyword" },
                  "number" : { "type" : "keyword" }
                }
            """.trimIndent(),
            false
        )

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(dataStreamName), listOf(docQuery))

        val action = randomAction(template = randomTemplateScript("Hello {{ctx.monitor.name}}"), destinationId = createDestination().id)
        val monitor = createMonitor(
            randomDocumentLevelMonitor(
                inputs = listOf(docLevelInput),
                triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action)))
            )
        )

        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "@timestamp": "$testTime",
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        indexDoc(dataStreamName, "1", testDoc)
        var response = executeMonitor(monitor.id)
        var output = entityAsMap(response)
        var searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        var matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 1, matchingDocsToQuery.size)

        indexDoc(dataStreamName, "2", testDoc)
        rolloverDatastream(dataStreamName)
        rolloverDatastream(dataStreamName)
        indexDoc(dataStreamName, "4", testDoc)
        rolloverDatastream(dataStreamName)
        response = executeMonitor(monitor.id)
        output = entityAsMap(response)
        searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 2, matchingDocsToQuery.size)

        indexDoc(dataStreamName, "5", testDoc)
        indexDoc(dataStreamName, "6", testDoc)
        response = executeMonitor(monitor.id)
        output = entityAsMap(response)
        searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 2, matchingDocsToQuery.size)
        deleteDataStream(dataStreamName)
    }

    fun `test document-level monitor when index alias contain docs that do match query`() {
        val aliasName = "test-alias"
        createIndexAlias(
            aliasName,
            """
                "properties" : {
                  "test_strict_date_time" : { "type" : "date", "format" : "strict_date_time" },
                  "test_field" : { "type" : "keyword" },
                  "number" : { "type" : "keyword" }
                }
            """.trimIndent()
        )

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf("$aliasName"), listOf(docQuery))

        val action = randomAction(template = randomTemplateScript("Hello {{ctx.monitor.name}}"), destinationId = createDestination().id)
        val monitor = createMonitor(
            randomDocumentLevelMonitor(
                inputs = listOf(docLevelInput),
                triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action)))
            )
        )

        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "@timestamp": "$testTime",
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        indexDoc(aliasName, "1", testDoc)
        var response = executeMonitor(monitor.id)
        var output = entityAsMap(response)
        var searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        var matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 1, matchingDocsToQuery.size)

        rolloverDatastream(aliasName)
        indexDoc(aliasName, "2", testDoc)
        response = executeMonitor(monitor.id)
        output = entityAsMap(response)
        searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 1, matchingDocsToQuery.size)

        deleteIndexAlias(aliasName)
    }

    fun `test document-level monitor  when multiple datastreams contain docs across read-only indices that do match query`() {
        val dataStreamName1 = "test-datastream1"
        createDataStream(
            dataStreamName1,
            """
                "properties" : {
                  "test_strict_date_time" : { "type" : "date", "format" : "strict_date_time" },
                  "test_field" : { "type" : "keyword" },
                  "number" : { "type" : "keyword" }
                }
            """.trimIndent(),
            false
        )
        val dataStreamName2 = "test-datastream2"
        createDataStream(
            dataStreamName2,
            """
                "properties" : {
                  "test_strict_date_time" : { "type" : "date", "format" : "strict_date_time" },
                  "test_field" : { "type" : "keyword" },
                  "number" : { "type" : "keyword" }
                }
            """.trimIndent(),
            false
        )

        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "@timestamp": "$testTime",
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        indexDoc(dataStreamName2, "-1", testDoc)
        rolloverDatastream(dataStreamName2)
        indexDoc(dataStreamName2, "0", testDoc)

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf("test-datastream*"), listOf(docQuery))

        val action = randomAction(template = randomTemplateScript("Hello {{ctx.monitor.name}}"), destinationId = createDestination().id)
        val monitor = createMonitor(
            randomDocumentLevelMonitor(
                inputs = listOf(docLevelInput),
                triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action)))
            )
        )

        indexDoc(dataStreamName1, "1", testDoc)
        indexDoc(dataStreamName2, "1", testDoc)
        var response = executeMonitor(monitor.id)
        var output = entityAsMap(response)
        var searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        var matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 2, matchingDocsToQuery.size)

        indexDoc(dataStreamName1, "2", testDoc)
        indexDoc(dataStreamName2, "2", testDoc)
        rolloverDatastream(dataStreamName1)
        rolloverDatastream(dataStreamName1)
        rolloverDatastream(dataStreamName2)
        indexDoc(dataStreamName1, "4", testDoc)
        indexDoc(dataStreamName2, "4", testDoc)
        rolloverDatastream(dataStreamName1)
        response = executeMonitor(monitor.id)
        output = entityAsMap(response)
        searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 4, matchingDocsToQuery.size)

        indexDoc(dataStreamName1, "5", testDoc)
        indexDoc(dataStreamName1, "6", testDoc)
        indexDoc(dataStreamName2, "5", testDoc)
        indexDoc(dataStreamName2, "6", testDoc)
        response = executeMonitor(monitor.id)
        output = entityAsMap(response)
        searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 4, matchingDocsToQuery.size)
        deleteDataStream(dataStreamName1)
        deleteDataStream(dataStreamName2)
    }

    fun `test document-level monitor ignoring old read-only indices for datastreams`() {
        val dataStreamName = "test-datastream"
        createDataStream(
            dataStreamName,
            """
                "properties" : {
                  "test_strict_date_time" : { "type" : "date", "format" : "strict_date_time" },
                  "test_field" : { "type" : "keyword" },
                  "number" : { "type" : "keyword" }
                }
            """.trimIndent(),
            false
        )

        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "@timestamp": "$testTime",
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        indexDoc(dataStreamName, "-1", testDoc)
        rolloverDatastream(dataStreamName)
        indexDoc(dataStreamName, "0", testDoc)

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(dataStreamName), listOf(docQuery))

        val action = randomAction(template = randomTemplateScript("Hello {{ctx.monitor.name}}"), destinationId = createDestination().id)
        val monitor = createMonitor(
            randomDocumentLevelMonitor(
                inputs = listOf(docLevelInput),
                triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action)))
            )
        )

        indexDoc(dataStreamName, "1", testDoc)
        var response = executeMonitor(monitor.id)
        var output = entityAsMap(response)
        var searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        var matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 1, matchingDocsToQuery.size)

        rolloverDatastream(dataStreamName)
        indexDoc(dataStreamName, "2", testDoc)
        response = executeMonitor(monitor.id)
        output = entityAsMap(response)
        searchResult = (output.objectMap("input_results")["results"] as List<Map<String, Any>>).first()
        @Suppress("UNCHECKED_CAST")
        matchingDocsToQuery = searchResult[docQuery.id] as List<String>
        assertEquals("Incorrect search result", 1, matchingDocsToQuery.size)

        deleteDataStream(dataStreamName)
    }

    fun `test execute monitor with non-null data sources`() {

        val testIndex = createTestIndex()
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
                "message" : "This is an error from IAD region",
                "test_strict_date_time" : "$testTime",
                "test_field" : "us-west-2"
            }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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
        try {
            createMonitor(
                randomDocumentLevelMonitor(
                    inputs = listOf(docLevelInput),
                    triggers = listOf(trigger),
                    dataSources = DataSources(
                        findingsIndex = "custom_findings_index",
                        alertsIndex = "custom_alerts_index",
                    )
                )
            )
            fail("Expected create monitor to fail")
        } catch (e: ResponseException) {
            assertTrue(e.message!!.contains("illegal_argument_exception"))
        }
    }

    fun `test execute monitor with indices removed after first run`() {
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val index1 = createTestIndex()
        val index2 = createTestIndex()
        val index4 = createTestIndex()
        val index5 = createTestIndex()

        val docQuery = DocLevelQuery(query = "\"us-west-2\"", name = "3", fields = listOf())
        var docLevelInput = DocLevelMonitorInput("description", listOf(index1, index2, index4, index5), listOf(docQuery))

        val action = randomAction(template = randomTemplateScript("Hello {{ctx.monitor.name}}"), destinationId = createDestination().id)
        val monitor = createMonitor(
            randomDocumentLevelMonitor(
                inputs = listOf(docLevelInput),
                triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action)))
            )
        )

        indexDoc(index1, "1", testDoc)
        indexDoc(index2, "1", testDoc)
        indexDoc(index4, "1", testDoc)
        indexDoc(index5, "1", testDoc)

        var response = executeMonitor(monitor.id)

        var output = entityAsMap(response)
        assertEquals(monitor.name, output["monitor_name"])

        assertEquals(1, output.objectMap("trigger_results").values.size)
        deleteIndex(index1)
        deleteIndex(index2)

        indexDoc(index4, "2", testDoc)
        response = executeMonitor(monitor.id)

        output = entityAsMap(response)
        assertEquals(1, output.objectMap("trigger_results").values.size)
    }

    @Suppress("UNCHECKED_CAST")
    /** helper that returns a field in a json map whose values are all json objects */
    private fun Map<String, Any>.objectMap(key: String): Map<String, Map<String, Any>> {
        return this[key] as Map<String, Map<String, Any>>
    }

    fun `test execute monitor with non-null owner`() {

        val testIndex = createTestIndex()
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
                "message" : "This is an error from IAD region",
                "test_strict_date_time" : "$testTime",
                "test_field" : "us-west-2"
            }"""

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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
        try {
            createMonitor(
                randomDocumentLevelMonitor(
                    inputs = listOf(docLevelInput),
                    triggers = listOf(trigger),
                    owner = "owner"
                )
            )
            fail("Expected create monitor to fail")
        } catch (e: ResponseException) {
            assertTrue(e.message!!.contains("illegal_argument_exception"))
        }
    }
}
