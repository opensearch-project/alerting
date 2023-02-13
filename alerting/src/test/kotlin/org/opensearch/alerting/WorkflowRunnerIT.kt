/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.junit.Assert
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.transport.WorkflowSingleNodeTestCase
import org.opensearch.commons.alerting.action.AcknowledgeAlertRequest
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.GetAlertsRequest
import org.opensearch.commons.alerting.action.IndexMonitorResponse
import org.opensearch.commons.alerting.aggregation.bucketselectorext.BucketSelectorExtAggregationBuilder
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.alerting.model.Table
import org.opensearch.index.query.QueryBuilders
import org.opensearch.script.Script
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

class WorkflowRunnerIT : WorkflowSingleNodeTestCase() {

    fun `test execute workflow with custom alerts and finding index with doc level delegates`() {
        val docQuery1 = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput1 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customAlertsIndex1 = "custom_alerts_index"
        val customFindingsIndex1 = "custom_findings_index"
        val customFindingsIndexPattern1 = "custom_findings_index-1"
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1),
            dataSources = DataSources(
                alertsIndex = customAlertsIndex1,
                findingsIndex = customFindingsIndex1,
                findingsIndexPattern = customFindingsIndexPattern1
            )
        )
        val monitorResponse = createMonitor(monitor1)!!

        val docQuery2 = DocLevelQuery(query = "source.ip.v6.v2:16645", name = "4")
        val docLevelInput2 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery2))
        val trigger2 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customAlertsIndex2 = "custom_alerts_index_2"
        val customFindingsIndex2 = "custom_findings_index_2"
        val customFindingsIndexPattern2 = "custom_findings_index-2"
        var monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput2),
            triggers = listOf(trigger2),
            dataSources = DataSources(
                alertsIndex = customAlertsIndex2,
                findingsIndex = customFindingsIndex2,
                findingsIndexPattern = customFindingsIndexPattern2
            )
        )

        val monitorResponse2 = createMonitor(monitor2)!!

        var workflow = randomWorkflowMonitor(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)!!
        assertNotNull(workflowById)

        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1 and monitor2
        val testDoc2 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        indexDoc(index, "2", testDoc2)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Doesn't match
        val testDoc3 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 123456, 
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-east-1"
        }"""
        indexDoc(index, "3", testDoc3)

        val workflowId = workflowResponse.id
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        val monitorsRunResults = executeWorkflowResponse.workflowRunResult
        assertEquals(2, monitorsRunResults.size)

        assertEquals(monitor1.name, monitorsRunResults[0].monitorName)
        assertEquals(1, monitorsRunResults[0].triggerResults.size)

        Assert.assertEquals(monitor2.name, monitorsRunResults[1].monitorName)
        Assert.assertEquals(1, monitorsRunResults[1].triggerResults.size)

        assertAlerts(monitorResponse, customAlertsIndex1, 2)
        assertFindings(monitorResponse.id, customFindingsIndex1, 2, 2, listOf("1", "2"))

        assertAlerts(monitorResponse2, customAlertsIndex2, 1)
        assertFindings(monitorResponse2.id, customFindingsIndex2, 1, 1, listOf("2"))
    }

    fun `test execute workflow with custom alerts and finding index with doc level and bucket level delegates`() {
        insertSampleTimeSerializedData(
            index,
            listOf(
                "test_value_1",
                "test_value_1", // adding duplicate to verify aggregation
                "test_value_2"
            )
        )

        val query = QueryBuilders.rangeQuery("test_strict_date_time")
            .gt("{{period_end}}||-10d")
            .lte("{{period_end}}")
            .format("epoch_millis")
        val compositeSources = listOf(
            TermsValuesSourceBuilder("test_field").field("test_field")
        )
        val compositeAgg = CompositeAggregationBuilder("composite_agg", compositeSources)
        val input = SearchInput(indices = listOf(index), query = SearchSourceBuilder().size(0).query(query).aggregation(compositeAgg))
        val triggerScript = """
            params.docCount > 0
        """.trimIndent()

        var trigger = randomBucketLevelTrigger()
        trigger = trigger.copy(
            bucketSelector = BucketSelectorExtAggregationBuilder(
                name = trigger.id,
                bucketsPathsMap = mapOf("docCount" to "_count"),
                script = Script(triggerScript),
                parentBucketPath = "composite_agg",
                filter = null
            )
        )
        val bucketLevelMonitorResponse = createMonitor(randomBucketLevelMonitor(inputs = listOf(input), enabled = false, triggers = listOf(trigger)))!!

        val docQuery = DocLevelQuery(query = "test_field:\"test_value_2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val docLevelTrigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val customAlertsIndex = "custom_alerts_index"
        val customFindingsIndexPattern = "custom_findings_index"

        var docLevelMonitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(docLevelTrigger),
            dataSources = DataSources(
                alertsIndex = customAlertsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )
        val docLevelMonitorResponse = createMonitor(docLevelMonitor)!!

        var workflow = randomWorkflowMonitor(
            monitorIds = listOf(docLevelMonitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)!!
        assertNotNull(workflowById)

        val workflowId = workflowResponse.id
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        assertNotNull(executeWorkflowResponse)

        val bucketLevelResponse = executeWorkflowResponse.workflowRunResult[1]

        assertEquals(bucketLevelMonitorResponse.monitor.name, bucketLevelResponse.monitorName)
        val searchResult = bucketLevelResponse.inputResults.results.first()
        @Suppress("UNCHECKED_CAST")
        val buckets = searchResult.stringMap("aggregations")?.stringMap("composite_agg")?.get("buckets") as List<Map<String, Any>>
        assertEquals("Incorrect search result", 2, buckets.size)
    }

    fun `test bucket execution`() {
        insertSampleTimeSerializedData(
            index,
            listOf(
                "test_value_1",
                "test_value_1", // adding duplicate to verify aggregation
                "test_value_2"
            )
        )

        val compositeSources = listOf(
            TermsValuesSourceBuilder("test_field").field("test_field")
        )
        val compositeAgg = CompositeAggregationBuilder("composite_agg", compositeSources)
        val input = SearchInput(indices = listOf(index), query = SearchSourceBuilder().size(0).aggregation(compositeAgg))
        val triggerScript = """
            params.docCount > 0
        """.trimIndent()

        var trigger = randomBucketLevelTrigger()
        trigger = trigger.copy(
            bucketSelector = BucketSelectorExtAggregationBuilder(
                name = trigger.id,
                bucketsPathsMap = mapOf("docCount" to "_count"),
                script = Script(triggerScript),
                parentBucketPath = "composite_agg",
                filter = null
            )
        )
        val monitor = createMonitor(randomBucketLevelMonitor(inputs = listOf(input), enabled = false, triggers = listOf(trigger)))!!
        val response = executeMonitor(monitor.monitor, monitor.id, false)!!

        assertEquals(monitor.monitor.name, response.monitorRunResult.monitorName)
        @Suppress("UNCHECKED_CAST")
        val searchResult = (response.monitorRunResult.inputResults.results).first()
        @Suppress("UNCHECKED_CAST")
        val buckets = searchResult.stringMap("aggregations")?.stringMap("composite_agg")?.get("buckets") as List<Map<String, Any>>
        assertEquals("Incorrect search result", 2, buckets.size)
    }

    private fun assertFindings(
        monitorId: String,
        customFindingsIndex: String,
        findingSize: Int,
        matchedQueryNumber: Int,
        relatedDocIds: List<String>
    ) {
        val findings = searchFindings(monitorId, customFindingsIndex)
        assertEquals("Findings saved for test monitor", findingSize, findings.size)

        val findingDocIds = findings.flatMap { it.relatedDocIds }

        assertEquals("Didn't match $matchedQueryNumber query", matchedQueryNumber, findingDocIds.size)
        assertTrue("Findings saved for test monitor", relatedDocIds.containsAll(findingDocIds))
    }

    private fun assertAlerts(
        monitorResponse: IndexMonitorResponse,
        customAlertsIndex: String,
        alertSize: Int
    ) {
        val monitorId = monitorResponse.id
        val alerts = searchAlerts(monitorId, customAlertsIndex)
        assertEquals("Alert saved for test monitor", alertSize, alerts.size)
        val table = Table("asc", "id", null, alertSize, 0, "")
        var getAlertsResponse = client()
            .execute(
                AlertingActions.GET_ALERTS_ACTION_TYPE,
                GetAlertsRequest(table, "ALL", "ALL", null, customAlertsIndex)
            )
            .get()
        assertTrue(getAlertsResponse != null)
        assertTrue(getAlertsResponse.alerts.size == alertSize)
        getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", monitorId, null))
            .get()
        assertTrue(getAlertsResponse != null)
        assertTrue(getAlertsResponse.alerts.size == alertSize)

        val alertIds = getAlertsResponse.alerts.map { it.id }
        val acknowledgeAlertResponse = client().execute(
            AlertingActions.ACKNOWLEDGE_ALERTS_ACTION_TYPE,
            AcknowledgeAlertRequest(monitorId, alertIds, WriteRequest.RefreshPolicy.IMMEDIATE)
        ).get()

        assertEquals(alertSize, acknowledgeAlertResponse.acknowledged.size)
    }
}
