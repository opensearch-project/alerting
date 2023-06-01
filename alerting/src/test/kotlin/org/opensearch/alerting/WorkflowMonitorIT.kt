/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.junit.Assert
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.alerting.model.WorkflowMetadata
import org.opensearch.alerting.transport.WorkflowSingleNodeTestCase
import org.opensearch.alerting.util.AlertingException
import org.opensearch.commons.alerting.action.AcknowledgeAlertRequest
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.GetAlertsRequest
import org.opensearch.commons.alerting.action.GetAlertsResponse
import org.opensearch.commons.alerting.action.IndexMonitorResponse
import org.opensearch.commons.alerting.aggregation.bucketselectorext.BucketSelectorExtAggregationBuilder
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.ChainedMonitorFindings
import org.opensearch.commons.alerting.model.CompositeInput
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.Delegate
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.alerting.model.Table
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus
import org.opensearch.script.Script
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Collections
import java.util.UUID
import java.util.concurrent.ExecutionException

class WorkflowMonitorIT : WorkflowSingleNodeTestCase() {

    fun `test execute workflow with custom alerts and finding index with doc level delegates`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3")
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

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1 and monitor2
        val testDoc2 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "2", testDoc2)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Doesn't match
        val testDoc3 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-east-1"
        }"""
        indexDoc(index, "3", testDoc3)

        val workflowId = workflowResponse.id
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        val monitorsRunResults = executeWorkflowResponse.workflowRunResult.workflowRunResult
        assertEquals(2, monitorsRunResults.size)

        assertEquals(monitor1.name, monitorsRunResults[0].monitorName)
        assertEquals(1, monitorsRunResults[0].triggerResults.size)

        Assert.assertEquals(monitor2.name, monitorsRunResults[1].monitorName)
        Assert.assertEquals(1, monitorsRunResults[1].triggerResults.size)

        val getAlertsResponse = assertAlerts(monitorResponse.id, customAlertsIndex1, 2)
        assertAcknowledges(getAlertsResponse.alerts, monitorResponse.id, 2)
        assertFindings(monitorResponse.id, customFindingsIndex1, 2, 2, listOf("1", "2"))

        val getAlertsResponse2 = assertAlerts(monitorResponse2.id, customAlertsIndex2, 1)
        assertAcknowledges(getAlertsResponse2.alerts, monitorResponse2.id, 1)
        assertFindings(monitorResponse2.id, customFindingsIndex2, 1, 1, listOf("2"))
    }

    fun `test execute workflows with shared doc level monitor delegate`() {
        val docQuery = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customAlertsIndex = "custom_alerts_index"
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                alertsIndex = customAlertsIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )
        val monitorResponse = createMonitor(monitor)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        var workflow1 = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse1 = upsertWorkflow(workflow1)!!
        val workflowById1 = searchWorkflow(workflowResponse1.id)
        assertNotNull(workflowById1)

        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        val testDoc2 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "2", testDoc2)

        val workflowId = workflowResponse.id
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        val monitorsRunResults = executeWorkflowResponse.workflowRunResult.workflowRunResult
        assertEquals(1, monitorsRunResults.size)

        assertEquals(monitor.name, monitorsRunResults[0].monitorName)
        assertEquals(1, monitorsRunResults[0].triggerResults.size)

        // Assert and not ack the alerts (in order to verify later on that all the alerts are generated)
        assertAlerts(monitorResponse.id, customAlertsIndex, 2)
        assertFindings(monitorResponse.id, customFindingsIndex, 2, 2, listOf("1", "2"))
        // Verify workflow and monitor delegate metadata
        val workflowMetadata = searchWorkflowMetadata(id = workflowId)
        assertNotNull("Workflow metadata not initialized", workflowMetadata)
        assertEquals(
            "Workflow metadata execution id not correct",
            executeWorkflowResponse.workflowRunResult.executionId,
            workflowMetadata!!.latestExecutionId
        )
        val monitorMetadataId = getDelegateMonitorMetadataId(monitorResponse, workflowMetadata)
        val monitorMetadata = searchMonitorMetadata(monitorMetadataId)
        assertNotNull(monitorMetadata)

        // Execute second workflow
        val workflowId1 = workflowResponse1.id
        val executeWorkflowResponse1 = executeWorkflow(workflowById1, workflowId1, false)!!
        val monitorsRunResults1 = executeWorkflowResponse1.workflowRunResult.workflowRunResult
        assertEquals(1, monitorsRunResults1.size)

        assertEquals(monitor.name, monitorsRunResults1[0].monitorName)
        assertEquals(1, monitorsRunResults1[0].triggerResults.size)

        val getAlertsResponse = assertAlerts(monitorResponse.id, customAlertsIndex, 4)
        assertAcknowledges(getAlertsResponse.alerts, monitorResponse.id, 4)
        assertFindings(monitorResponse.id, customFindingsIndex, 4, 4, listOf("1", "2", "1", "2"))
        // Verify workflow and monitor delegate metadata
        val workflowMetadata1 = searchWorkflowMetadata(id = workflowId1)
        assertNotNull("Workflow metadata not initialized", workflowMetadata1)
        assertEquals(
            "Workflow metadata execution id not correct",
            executeWorkflowResponse1.workflowRunResult.executionId,
            workflowMetadata1!!.latestExecutionId
        )
        val monitorMetadataId1 = getDelegateMonitorMetadataId(monitorResponse, workflowMetadata1)
        val monitorMetadata1 = searchMonitorMetadata(monitorMetadataId1)
        assertNotNull(monitorMetadata1)
        // Verify that for two workflows two different doc level monitor metadata has been created
        assertTrue("Different monitor is used in workflows", monitorMetadata!!.monitorId == monitorMetadata1!!.monitorId)
        assertTrue(monitorMetadata.id != monitorMetadata1.id)
    }

    fun `test execute workflows with shared doc level monitor delegate updating delegate datasource`() {
        val docQuery = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)!!

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        val workflow1 = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse1 = upsertWorkflow(workflow1)!!
        val workflowById1 = searchWorkflow(workflowResponse1.id)
        assertNotNull(workflowById1)

        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        val testDoc2 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "2", testDoc2)

        val workflowId = workflowResponse.id
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        val monitorsRunResults = executeWorkflowResponse.workflowRunResult.workflowRunResult
        assertEquals(1, monitorsRunResults.size)

        assertEquals(monitor.name, monitorsRunResults[0].monitorName)
        assertEquals(1, monitorsRunResults[0].triggerResults.size)

        assertAlerts(monitorResponse.id, AlertIndices.ALERT_INDEX, 2)
        assertFindings(monitorResponse.id, AlertIndices.FINDING_HISTORY_WRITE_INDEX, 2, 2, listOf("1", "2"))
        // Verify workflow and monitor delegate metadata
        val workflowMetadata = searchWorkflowMetadata(id = workflowId)
        assertNotNull("Workflow metadata not initialized", workflowMetadata)
        assertEquals(
            "Workflow metadata execution id not correct",
            executeWorkflowResponse.workflowRunResult.executionId,
            workflowMetadata!!.latestExecutionId
        )
        val monitorMetadataId = getDelegateMonitorMetadataId(monitorResponse, workflowMetadata)
        val monitorMetadata = searchMonitorMetadata(monitorMetadataId)
        assertNotNull(monitorMetadata)

        val customAlertsIndex = "custom_alerts_index"
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val monitorId = monitorResponse.id
        updateMonitor(
            monitor = monitor.copy(
                dataSources = DataSources(
                    alertsIndex = customAlertsIndex,
                    findingsIndex = customFindingsIndex,
                    findingsIndexPattern = customFindingsIndexPattern
                )
            ),
            monitorId
        )

        // Execute second workflow
        val workflowId1 = workflowResponse1.id
        val executeWorkflowResponse1 = executeWorkflow(workflowById1, workflowId1, false)!!
        val monitorsRunResults1 = executeWorkflowResponse1.workflowRunResult.workflowRunResult
        assertEquals(1, monitorsRunResults1.size)

        assertEquals(monitor.name, monitorsRunResults1[0].monitorName)
        assertEquals(1, monitorsRunResults1[0].triggerResults.size)

        // Verify alerts for the custom index
        val getAlertsResponse = assertAlerts(monitorResponse.id, customAlertsIndex, 2)
        assertAcknowledges(getAlertsResponse.alerts, monitorResponse.id, 2)
        assertFindings(monitorResponse.id, customFindingsIndex, 2, 2, listOf("1", "2"))

        // Verify workflow and monitor delegate metadata
        val workflowMetadata1 = searchWorkflowMetadata(id = workflowId1)
        assertNotNull("Workflow metadata not initialized", workflowMetadata1)
        assertEquals(
            "Workflow metadata execution id not correct",
            executeWorkflowResponse1.workflowRunResult.executionId,
            workflowMetadata1!!.latestExecutionId
        )
        val monitorMetadataId1 = getDelegateMonitorMetadataId(monitorResponse, workflowMetadata1)
        val monitorMetadata1 = searchMonitorMetadata(monitorMetadataId1)
        assertNotNull(monitorMetadata1)
        // Verify that for two workflows two different doc level monitor metadata has been created
        assertTrue("Different monitor is used in workflows", monitorMetadata!!.monitorId == monitorMetadata1!!.monitorId)
        assertTrue(monitorMetadata.id != monitorMetadata1.id)
    }

    fun `test execute workflow verify workflow metadata`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3")
        val docLevelInput1 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1)
        )
        val monitorResponse = createMonitor(monitor1)!!

        val docQuery2 = DocLevelQuery(query = "source.ip.v6.v2:16645", name = "4")
        val docLevelInput2 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery2))
        val trigger2 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput2),
            triggers = listOf(trigger2),
        )

        val monitorResponse2 = createMonitor(monitor2)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)
        // First execution
        val workflowId = workflowResponse.id
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        val monitorsRunResults = executeWorkflowResponse.workflowRunResult.workflowRunResult
        assertEquals(2, monitorsRunResults.size)

        val workflowMetadata = searchWorkflowMetadata(id = workflowId)
        assertNotNull("Workflow metadata not initialized", workflowMetadata)
        assertEquals(
            "Workflow metadata execution id not correct",
            executeWorkflowResponse.workflowRunResult.executionId,
            workflowMetadata!!.latestExecutionId
        )
        val monitorMetadataId = getDelegateMonitorMetadataId(monitorResponse, workflowMetadata)
        val monitorMetadata = searchMonitorMetadata(monitorMetadataId)
        assertNotNull(monitorMetadata)

        // Second execution
        val executeWorkflowResponse1 = executeWorkflow(workflowById, workflowId, false)!!
        val monitorsRunResults1 = executeWorkflowResponse1.workflowRunResult.workflowRunResult
        assertEquals(2, monitorsRunResults1.size)

        val workflowMetadata1 = searchWorkflowMetadata(id = workflowId)
        assertNotNull("Workflow metadata not initialized", workflowMetadata)
        assertEquals(
            "Workflow metadata execution id not correct",
            executeWorkflowResponse1.workflowRunResult.executionId,
            workflowMetadata1!!.latestExecutionId
        )
        val monitorMetadataId1 = getDelegateMonitorMetadataId(monitorResponse, workflowMetadata1)
        assertTrue(monitorMetadataId == monitorMetadataId1)
        val monitorMetadata1 = searchMonitorMetadata(monitorMetadataId1)
        assertNotNull(monitorMetadata1)
    }

    private fun getDelegateMonitorMetadataId(
        monitorResponse: IndexMonitorResponse,
        workflowMetadata: WorkflowMetadata,
    ) = "${workflowMetadata.id}-${monitorResponse.id}-metadata"

    fun `test execute workflow dryrun verify workflow metadata not created`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3")
        val docLevelInput1 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1)
        )
        val monitorResponse = createMonitor(monitor1)!!

        val docQuery2 = DocLevelQuery(query = "source.ip.v6.v2:16645", name = "4")
        val docLevelInput2 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery2))
        val trigger2 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput2),
            triggers = listOf(trigger2),
        )

        val monitorResponse2 = createMonitor(monitor2)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)
        // First execution
        val workflowId = workflowResponse.id
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, true)

        assertNotNull("Workflow run result is null", executeWorkflowResponse)
        val monitorsRunResults = executeWorkflowResponse!!.workflowRunResult.workflowRunResult
        assertEquals(2, monitorsRunResults.size)

        var exception: java.lang.Exception? = null
        try {
            searchWorkflowMetadata(id = workflowId)
        } catch (ex: java.lang.Exception) {
            exception = ex
        }
        assertTrue(exception is NoSuchElementException)
    }

    fun `test execute workflow with custom alerts and finding index when bucket monitor is used in chained finding of doc monitor`() {
        val query = QueryBuilders.rangeQuery("test_strict_date_time")
            .gt("{{period_end}}||-10d")
            .lte("{{period_end}}")
            .format("epoch_millis")
        val compositeSources = listOf(
            TermsValuesSourceBuilder("test_field_1").field("test_field_1")
        )
        val compositeAgg = CompositeAggregationBuilder("composite_agg", compositeSources)
        val input = SearchInput(indices = listOf(index), query = SearchSourceBuilder().size(0).query(query).aggregation(compositeAgg))
        // Bucket level monitor will reduce the size of matched doc ids on those that belong
        // to a bucket that contains more than 1 document after term grouping
        val triggerScript = """
            params.docCount > 1
        """.trimIndent()

        var trigger = randomBucketLevelTrigger()
        trigger = trigger.copy(
            bucketSelector = BucketSelectorExtAggregationBuilder(
                name = trigger.id,
                bucketsPathsMap = mapOf("docCount" to "_count"),
                script = Script(triggerScript),
                parentBucketPath = "composite_agg",
                filter = null,
            )
        )
        val bucketCustomAlertsIndex = "custom_alerts_index"
        val bucketCustomFindingsIndex = "custom_findings_index"
        val bucketCustomFindingsIndexPattern = "custom_findings_index-1"

        val bucketLevelMonitorResponse = createMonitor(
            randomBucketLevelMonitor(
                inputs = listOf(input),
                enabled = false,
                triggers = listOf(trigger),
                dataSources = DataSources(
                    findingsEnabled = true,
                    alertsIndex = bucketCustomAlertsIndex,
                    findingsIndex = bucketCustomFindingsIndex,
                    findingsIndexPattern = bucketCustomFindingsIndexPattern
                )
            )
        )!!

        val docQuery1 = DocLevelQuery(query = "test_field_1:\"test_value_2\"", name = "1")
        val docQuery2 = DocLevelQuery(query = "test_field_1:\"test_value_1\"", name = "2")
        val docQuery3 = DocLevelQuery(query = "test_field_1:\"test_value_3\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1, docQuery2, docQuery3))
        val docTrigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val docCustomAlertsIndex = "custom_alerts_index"
        val docCustomFindingsIndex = "custom_findings_index"
        val docCustomFindingsIndexPattern = "custom_findings_index-1"
        var docLevelMonitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(docTrigger),
            dataSources = DataSources(
                alertsIndex = docCustomAlertsIndex,
                findingsIndex = docCustomFindingsIndex,
                findingsIndexPattern = docCustomFindingsIndexPattern
            )
        )

        val docLevelMonitorResponse = createMonitor(docLevelMonitor)!!
        // 1. bucketMonitor (chainedFinding = null) 2. docMonitor (chainedFinding = bucketMonitor)
        var workflow = randomWorkflow(
            monitorIds = listOf(bucketLevelMonitorResponse.id, docLevelMonitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        // Creates 5 documents
        insertSampleTimeSerializedData(
            index,
            listOf(
                "test_value_1",
                "test_value_1", // adding duplicate to verify aggregation
                "test_value_2",
                "test_value_2",
                "test_value_3"
            )
        )

        val workflowId = workflowResponse.id
        // 1. bucket level monitor should reduce the doc findings to 4 (1, 2, 3, 4)
        // 2. Doc level monitor will match those 4 documents although it contains rules for matching all 5 documents (docQuery3 matches the fifth)
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        assertNotNull(executeWorkflowResponse)

        for (monitorRunResults in executeWorkflowResponse.workflowRunResult.workflowRunResult) {
            if (bucketLevelMonitorResponse.monitor.name == monitorRunResults.monitorName) {
                val searchResult = monitorRunResults.inputResults.results.first()

                @Suppress("UNCHECKED_CAST")
                val buckets = searchResult.stringMap("aggregations")?.stringMap("composite_agg")?.get("buckets") as List<Map<String, Any>>
                assertEquals("Incorrect search result", 3, buckets.size)

                val getAlertsResponse = assertAlerts(bucketLevelMonitorResponse.id, bucketCustomAlertsIndex, 2)
                assertAcknowledges(getAlertsResponse.alerts, bucketLevelMonitorResponse.id, 2)
                assertFindings(bucketLevelMonitorResponse.id, bucketCustomFindingsIndex, 1, 4, listOf("1", "2", "3", "4"))
            } else {
                assertEquals(1, monitorRunResults.inputResults.results.size)
                val values = monitorRunResults.triggerResults.values
                assertEquals(1, values.size)
                @Suppress("UNCHECKED_CAST")
                val docLevelTrigger = values.iterator().next() as DocumentLevelTriggerRunResult
                val triggeredDocIds = docLevelTrigger.triggeredDocs.map { it.split("|")[0] }
                val expectedTriggeredDocIds = listOf("1", "2", "3", "4")
                assertEquals(expectedTriggeredDocIds, triggeredDocIds.sorted())

                val getAlertsResponse = assertAlerts(docLevelMonitorResponse.id, docCustomAlertsIndex, 4)
                assertAcknowledges(getAlertsResponse.alerts, docLevelMonitorResponse.id, 4)
                assertFindings(docLevelMonitorResponse.id, docCustomFindingsIndex, 4, 4, listOf("1", "2", "3", "4"))
            }
        }
    }

    fun `test execute workflow with custom alerts and finding index when doc level delegate is used in chained finding`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"test_value_2\"", name = "1")
        val docQuery2 = DocLevelQuery(query = "test_field_1:\"test_value_3\"", name = "2")

        var docLevelMonitor = randomDocumentLevelMonitor(
            inputs = listOf(DocLevelMonitorInput("description", listOf(index), listOf(docQuery1, docQuery2))),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN)),
            dataSources = DataSources(
                alertsIndex = "custom_alerts_index",
                findingsIndex = "custom_findings_index",
                findingsIndexPattern = "custom_findings_index-1"
            )
        )

        val docLevelMonitorResponse = createMonitor(docLevelMonitor)!!

        val query = QueryBuilders.rangeQuery("test_strict_date_time")
            .gt("{{period_end}}||-10d")
            .lte("{{period_end}}")
            .format("epoch_millis")
        val compositeSources = listOf(
            TermsValuesSourceBuilder("test_field_1").field("test_field_1")
        )
        val compositeAgg = CompositeAggregationBuilder("composite_agg", compositeSources)
        val input = SearchInput(indices = listOf(index), query = SearchSourceBuilder().size(0).query(query).aggregation(compositeAgg))
        // Bucket level monitor will reduce the size of matched doc ids on those that belong to a bucket that contains more than 1 document after term grouping
        val triggerScript = """
            params.docCount > 1
        """.trimIndent()

        var trigger = randomBucketLevelTrigger()
        trigger = trigger.copy(
            bucketSelector = BucketSelectorExtAggregationBuilder(
                name = trigger.id,
                bucketsPathsMap = mapOf("docCount" to "_count"),
                script = Script(triggerScript),
                parentBucketPath = "composite_agg",
                filter = null,
            )
        )

        val bucketLevelMonitorResponse = createMonitor(
            randomBucketLevelMonitor(
                inputs = listOf(input),
                enabled = false,
                triggers = listOf(trigger),
                dataSources = DataSources(
                    findingsEnabled = true,
                    alertsIndex = "custom_alerts_index",
                    findingsIndex = "custom_findings_index",
                    findingsIndexPattern = "custom_findings_index-1"
                )
            )
        )!!

        var docLevelMonitor1 = randomDocumentLevelMonitor(
            // Match the documents with test_field_1: test_value_3
            inputs = listOf(DocLevelMonitorInput("description", listOf(index), listOf(docQuery2))),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN)),
            dataSources = DataSources(
                findingsEnabled = true,
                alertsIndex = "custom_alerts_index_1",
                findingsIndex = "custom_findings_index_1",
                findingsIndexPattern = "custom_findings_index_1-1"
            )
        )

        val docLevelMonitorResponse1 = createMonitor(docLevelMonitor1)!!

        val queryMonitorInput = SearchInput(
            indices = listOf(index),
            query = SearchSourceBuilder().query(
                QueryBuilders
                    .rangeQuery("test_strict_date_time")
                    .gt("{{period_end}}||-10d")
                    .lte("{{period_end}}")
                    .format("epoch_millis")
            )
        )
        val queryTriggerScript = """
            return ctx.results[0].hits.hits.size() > 0
        """.trimIndent()

        val queryLevelTrigger = randomQueryLevelTrigger(condition = Script(queryTriggerScript))
        val queryMonitorResponse =
            createMonitor(randomQueryLevelMonitor(inputs = listOf(queryMonitorInput), triggers = listOf(queryLevelTrigger)))!!

        // 1. docMonitor (chainedFinding = null) 2. bucketMonitor (chainedFinding = docMonitor) 3. docMonitor (chainedFinding = bucketMonitor) 4. queryMonitor (chainedFinding = docMonitor 3)
        var workflow = randomWorkflow(
            monitorIds = listOf(
                docLevelMonitorResponse.id,
                bucketLevelMonitorResponse.id,
                docLevelMonitorResponse1.id,
                queryMonitorResponse.id
            )
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        // Creates 5 documents
        insertSampleTimeSerializedData(
            index,
            listOf(
                "test_value_1",
                "test_value_1", // adding duplicate to verify aggregation
                "test_value_2",
                "test_value_2",
                "test_value_3",
                "test_value_3"
            )
        )

        val workflowId = workflowResponse.id
        // 1. Doc level monitor should reduce the doc findings to 4 (3 - test_value_2, 4 - test_value_2, 5 - test_value_3, 6 - test_value_3)
        // 2. Bucket level monitor will match the fetch the docs from current findings execution, although it contains rules for matching documents which has test_value_2 and test value_3
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        assertNotNull(executeWorkflowResponse)

        for (monitorRunResults in executeWorkflowResponse.workflowRunResult.workflowRunResult) {
            when (monitorRunResults.monitorName) {
                // Verify first doc level monitor execution, alerts and findings
                docLevelMonitorResponse.monitor.name -> {
                    assertEquals(1, monitorRunResults.inputResults.results.size)
                    val values = monitorRunResults.triggerResults.values
                    assertEquals(1, values.size)
                    @Suppress("UNCHECKED_CAST")
                    val docLevelTrigger = values.iterator().next() as DocumentLevelTriggerRunResult
                    val triggeredDocIds = docLevelTrigger.triggeredDocs.map { it.split("|")[0] }
                    val expectedTriggeredDocIds = listOf("3", "4", "5", "6")
                    assertEquals(expectedTriggeredDocIds, triggeredDocIds.sorted())

                    val getAlertsResponse =
                        assertAlerts(docLevelMonitorResponse.id, docLevelMonitorResponse.monitor.dataSources.alertsIndex, 4)
                    assertAcknowledges(getAlertsResponse.alerts, docLevelMonitorResponse.id, 4)
                    assertFindings(
                        docLevelMonitorResponse.id,
                        docLevelMonitorResponse.monitor.dataSources.findingsIndex,
                        4,
                        4,
                        listOf("3", "4", "5", "6")
                    )
                }
                // Verify second bucket level monitor execution, alerts and findings
                bucketLevelMonitorResponse.monitor.name -> {
                    val searchResult = monitorRunResults.inputResults.results.first()

                    @Suppress("UNCHECKED_CAST")
                    val buckets =
                        searchResult
                            .stringMap("aggregations")?.stringMap("composite_agg")?.get("buckets") as List<Map<String, Any>>
                    assertEquals("Incorrect search result", 2, buckets.size)

                    val getAlertsResponse =
                        assertAlerts(bucketLevelMonitorResponse.id, bucketLevelMonitorResponse.monitor.dataSources.alertsIndex, 2)
                    assertAcknowledges(getAlertsResponse.alerts, bucketLevelMonitorResponse.id, 2)
                    assertFindings(
                        bucketLevelMonitorResponse.id,
                        bucketLevelMonitorResponse.monitor.dataSources.findingsIndex,
                        1,
                        4,
                        listOf("3", "4", "5", "6")
                    )
                }
                // Verify third doc level monitor execution, alerts and findings
                docLevelMonitorResponse1.monitor.name -> {
                    assertEquals(1, monitorRunResults.inputResults.results.size)
                    val values = monitorRunResults.triggerResults.values
                    assertEquals(1, values.size)
                    @Suppress("UNCHECKED_CAST")
                    val docLevelTrigger = values.iterator().next() as DocumentLevelTriggerRunResult
                    val triggeredDocIds = docLevelTrigger.triggeredDocs.map { it.split("|")[0] }
                    val expectedTriggeredDocIds = listOf("5", "6")
                    assertEquals(expectedTriggeredDocIds, triggeredDocIds.sorted())

                    val getAlertsResponse =
                        assertAlerts(docLevelMonitorResponse1.id, docLevelMonitorResponse1.monitor.dataSources.alertsIndex, 2)
                    assertAcknowledges(getAlertsResponse.alerts, docLevelMonitorResponse1.id, 2)
                    assertFindings(
                        docLevelMonitorResponse1.id,
                        docLevelMonitorResponse1.monitor.dataSources.findingsIndex,
                        2,
                        2,
                        listOf("5", "6")
                    )
                }
                // Verify fourth query level monitor execution
                queryMonitorResponse.monitor.name -> {
                    assertEquals(1, monitorRunResults.inputResults.results.size)
                    val values = monitorRunResults.triggerResults.values
                    assertEquals(1, values.size)
                    @Suppress("UNCHECKED_CAST")
                    val totalHits =
                        ((monitorRunResults.inputResults.results[0]["hits"] as Map<String, Any>)["total"] as Map<String, Any>)["value"]
                    assertEquals(2, totalHits)
                    @Suppress("UNCHECKED_CAST")
                    val docIds =
                        (
                            (monitorRunResults.inputResults.results[0]["hits"] as Map<String, Any>)["hits"] as List<Map<String, String>>
                            ).map { it["_id"]!! }
                    assertEquals(listOf("5", "6"), docIds.sorted())
                }
            }
        }
    }

    fun `test execute workflow input error`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)!!
        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        deleteIndex(index)

        val response = executeWorkflow(workflowById, workflowById!!.id, false)!!
        val error = response.workflowRunResult.workflowRunResult[0].error
        assertNotNull(error)
        assertTrue(error is AlertingException)
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, (error as AlertingException).status)
        assertTrue(error.message!!.contains("IndexNotFoundException"))
    }

    fun `test execute workflow wrong workflow id`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)!!

        val workflowRequest = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflowRequest)!!
        val workflowId = workflowResponse.id
        val getWorkflowResponse = getWorkflowById(id = workflowResponse.id)

        assertNotNull(getWorkflowResponse)
        assertEquals(workflowId, getWorkflowResponse.id)

        var exception: java.lang.Exception? = null
        val badWorkflowId = getWorkflowResponse.id + "bad"
        try {
            executeWorkflow(id = badWorkflowId)
        } catch (ex: java.lang.Exception) {
            exception = ex
        }
        assertTrue(exception is ExecutionException)
        assertTrue(exception!!.cause is AlertingException)
        assertEquals(RestStatus.NOT_FOUND, (exception.cause as AlertingException).status)
        assertEquals("Can't find workflow with id: $badWorkflowId", exception.cause!!.message)
    }

    private fun assertFindings(
        monitorId: String,
        customFindingsIndex: String,
        findingSize: Int,
        matchedQueryNumber: Int,
        relatedDocIds: List<String>,
    ) {
        val findings = searchFindings(monitorId, customFindingsIndex)
        assertEquals("Findings saved for test monitor", findingSize, findings.size)

        val findingDocIds = findings.flatMap { it.relatedDocIds }

        assertEquals("Didn't match $matchedQueryNumber query", matchedQueryNumber, findingDocIds.size)
        assertTrue("Findings saved for test monitor", relatedDocIds.containsAll(findingDocIds))
    }

    private fun assertAlerts(
        monitorId: String,
        customAlertsIndex: String,
        alertSize: Int,
    ): GetAlertsResponse {
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

        return getAlertsResponse
    }

    private fun assertAcknowledges(
        alerts: List<Alert>,
        monitorId: String,
        alertSize: Int,
    ) {
        val alertIds = alerts.map { it.id }
        val acknowledgeAlertResponse = client().execute(
            AlertingActions.ACKNOWLEDGE_ALERTS_ACTION_TYPE,
            AcknowledgeAlertRequest(monitorId, alertIds, WriteRequest.RefreshPolicy.IMMEDIATE)
        ).get()

        assertEquals(alertSize, acknowledgeAlertResponse.acknowledged.size)
    }

    fun `test execute workflow with bucket-level and doc-level chained monitors`() {
        createTestIndex(TEST_HR_INDEX)

        val compositeSources = listOf(
            TermsValuesSourceBuilder("test_field").field("test_field")
        )
        val compositeAgg = CompositeAggregationBuilder("composite_agg", compositeSources)
        val input = SearchInput(
            indices = listOf(TEST_HR_INDEX),
            query = SearchSourceBuilder().size(0).query(QueryBuilders.matchAllQuery()).aggregation(compositeAgg)
        )
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
            ),
            actions = listOf()
        )
        val bucketMonitor = createMonitor(
            randomBucketLevelMonitor(
                inputs = listOf(input),
                enabled = false,
                triggers = listOf(trigger)
            )
        )
        assertNotNull("The bucket monitor was not created", bucketMonitor)

        val docQuery1 = DocLevelQuery(query = "test_field:\"a\"", name = "3")
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(DocLevelMonitorInput("description", listOf(TEST_HR_INDEX), listOf(docQuery1))),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN))
        )
        val docMonitor = createMonitor(monitor1)!!
        assertNotNull("The doc level monitor was not created", docMonitor)

        val workflow = randomWorkflow(monitorIds = listOf(bucketMonitor!!.id, docMonitor.id))
        val workflowResponse = upsertWorkflow(workflow)
        assertNotNull("The workflow was not created", workflowResponse)

        // Add a doc that is accessible to the user
        indexDoc(
            TEST_HR_INDEX,
            "1",
            """
            {
              "test_field": "a",
              "accessible": true
            }
            """.trimIndent()
        )

        // Add a second doc that is not accessible to the user
        indexDoc(
            TEST_HR_INDEX,
            "2",
            """
            {
              "test_field": "b",
              "accessible": false
            }
            """.trimIndent()
        )

        indexDoc(
            TEST_HR_INDEX,
            "3",
            """
            {
              "test_field": "c",
              "accessible": true
            }
            """.trimIndent()
        )

        val executeResult = executeWorkflow(id = workflowResponse!!.id)
        assertNotNull(executeResult)
        assertEquals(2, executeResult!!.workflowRunResult.workflowRunResult.size)
    }

    fun `test create workflow success`() {
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3")
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        val monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitorResponse1 = createMonitor(monitor1)!!
        val monitorResponse2 = createMonitor(monitor2)!!

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse1.id, monitorResponse2.id)
        )

        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)
        assertNotNull(workflowResponse.workflow)
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowResponse.id)
        assertTrue("incorrect version", workflowResponse.version > 0)

        val workflowById = searchWorkflow(workflowResponse.id)!!
        assertNotNull(workflowById)

        // Verify workflow
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowById.id)
        assertTrue("incorrect version", workflowById.version > 0)
        assertEquals("Workflow name not correct", workflow.name, workflowById.name)
        assertEquals("Workflow owner not correct", workflow.owner, workflowById.owner)
        assertEquals("Workflow input not correct", workflow.inputs, workflowById.inputs)

        // Delegate verification
        @Suppress("UNCHECKED_CAST")
        val delegates = (workflowById.inputs as List<CompositeInput>)[0].sequence.delegates.sortedBy { it.order }
        assertEquals("Delegates size not correct", 2, delegates.size)

        val delegate1 = delegates[0]
        assertNotNull(delegate1)
        assertEquals("Delegate1 order not correct", 1, delegate1.order)
        assertEquals("Delegate1 id not correct", monitorResponse1.id, delegate1.monitorId)

        val delegate2 = delegates[1]
        assertNotNull(delegate2)
        assertEquals("Delegate2 order not correct", 2, delegate2.order)
        assertEquals("Delegate2 id not correct", monitorResponse2.id, delegate2.monitorId)
        assertEquals(
            "Delegate2 Chained finding not correct", monitorResponse1.id, delegate2.chainedMonitorFindings!!.monitorId
        )
    }

    fun `test update workflow add monitor success`() {
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3")
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        val monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitorResponse1 = createMonitor(monitor1)!!
        val monitorResponse2 = createMonitor(monitor2)!!

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse1.id, monitorResponse2.id)
        )

        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)
        assertNotNull(workflowResponse.workflow)
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowResponse.id)
        assertTrue("incorrect version", workflowResponse.version > 0)

        var workflowById = searchWorkflow(workflowResponse.id)!!
        assertNotNull(workflowById)

        val monitor3 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )
        val monitorResponse3 = createMonitor(monitor3)!!

        val updatedWorkflowResponse = upsertWorkflow(
            randomWorkflow(
                monitorIds = listOf(monitorResponse1.id, monitorResponse2.id, monitorResponse3.id)
            ),
            workflowResponse.id,
            RestRequest.Method.PUT
        )!!

        assertNotNull("Workflow creation failed", updatedWorkflowResponse)
        assertNotNull(updatedWorkflowResponse.workflow)
        assertEquals("Workflow id changed", workflowResponse.id, updatedWorkflowResponse.id)
        assertTrue("incorrect version", updatedWorkflowResponse.version > 0)

        workflowById = searchWorkflow(updatedWorkflowResponse.id)!!

        // Verify workflow
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowById.id)
        assertTrue("incorrect version", workflowById.version > 0)
        assertEquals("Workflow name not correct", updatedWorkflowResponse.workflow.name, workflowById.name)
        assertEquals("Workflow owner not correct", updatedWorkflowResponse.workflow.owner, workflowById.owner)
        assertEquals("Workflow input not correct", updatedWorkflowResponse.workflow.inputs, workflowById.inputs)

        // Delegate verification
        @Suppress("UNCHECKED_CAST")
        val delegates = (workflowById.inputs as List<CompositeInput>)[0].sequence.delegates.sortedBy { it.order }
        assertEquals("Delegates size not correct", 3, delegates.size)

        val delegate1 = delegates[0]
        assertNotNull(delegate1)
        assertEquals("Delegate1 order not correct", 1, delegate1.order)
        assertEquals("Delegate1 id not correct", monitorResponse1.id, delegate1.monitorId)

        val delegate2 = delegates[1]
        assertNotNull(delegate2)
        assertEquals("Delegate2 order not correct", 2, delegate2.order)
        assertEquals("Delegate2 id not correct", monitorResponse2.id, delegate2.monitorId)
        assertEquals(
            "Delegate2 Chained finding not correct", monitorResponse1.id, delegate2.chainedMonitorFindings!!.monitorId
        )

        val delegate3 = delegates[2]
        assertNotNull(delegate3)
        assertEquals("Delegate3 order not correct", 3, delegate3.order)
        assertEquals("Delegate3 id not correct", monitorResponse3.id, delegate3.monitorId)
        assertEquals(
            "Delegate3 Chained finding not correct", monitorResponse2.id, delegate3.chainedMonitorFindings!!.monitorId
        )
    }

    fun `test update workflow change order of delegate monitors`() {
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3")
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        val monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitorResponse1 = createMonitor(monitor1)!!
        val monitorResponse2 = createMonitor(monitor2)!!

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse1.id, monitorResponse2.id)
        )

        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)
        assertNotNull(workflowResponse.workflow)
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowResponse.id)
        assertTrue("incorrect version", workflowResponse.version > 0)

        var workflowById = searchWorkflow(workflowResponse.id)!!
        assertNotNull(workflowById)

        val updatedWorkflowResponse = upsertWorkflow(
            randomWorkflow(
                monitorIds = listOf(monitorResponse2.id, monitorResponse1.id)
            ),
            workflowResponse.id,
            RestRequest.Method.PUT
        )!!

        assertNotNull("Workflow creation failed", updatedWorkflowResponse)
        assertNotNull(updatedWorkflowResponse.workflow)
        assertEquals("Workflow id changed", workflowResponse.id, updatedWorkflowResponse.id)
        assertTrue("incorrect version", updatedWorkflowResponse.version > 0)

        workflowById = searchWorkflow(updatedWorkflowResponse.id)!!

        // Verify workflow
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowById.id)
        assertTrue("incorrect version", workflowById.version > 0)
        assertEquals("Workflow name not correct", updatedWorkflowResponse.workflow.name, workflowById.name)
        assertEquals("Workflow owner not correct", updatedWorkflowResponse.workflow.owner, workflowById.owner)
        assertEquals("Workflow input not correct", updatedWorkflowResponse.workflow.inputs, workflowById.inputs)

        // Delegate verification
        @Suppress("UNCHECKED_CAST")
        val delegates = (workflowById.inputs as List<CompositeInput>)[0].sequence.delegates.sortedBy { it.order }
        assertEquals("Delegates size not correct", 2, delegates.size)

        val delegate1 = delegates[0]
        assertNotNull(delegate1)
        assertEquals("Delegate1 order not correct", 1, delegate1.order)
        assertEquals("Delegate1 id not correct", monitorResponse2.id, delegate1.monitorId)

        val delegate2 = delegates[1]
        assertNotNull(delegate2)
        assertEquals("Delegate2 order not correct", 2, delegate2.order)
        assertEquals("Delegate2 id not correct", monitorResponse1.id, delegate2.monitorId)
        assertEquals(
            "Delegate2 Chained finding not correct", monitorResponse2.id, delegate2.chainedMonitorFindings!!.monitorId
        )
    }

    fun `test update workflow remove monitor success`() {
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3")
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        val monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitorResponse1 = createMonitor(monitor1)!!
        val monitorResponse2 = createMonitor(monitor2)!!

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse1.id, monitorResponse2.id)
        )

        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)
        assertNotNull(workflowResponse.workflow)
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowResponse.id)
        assertTrue("incorrect version", workflowResponse.version > 0)

        var workflowById = searchWorkflow(workflowResponse.id)!!
        assertNotNull(workflowById)

        val updatedWorkflowResponse = upsertWorkflow(
            randomWorkflow(
                monitorIds = listOf(monitorResponse1.id)
            ),
            workflowResponse.id,
            RestRequest.Method.PUT
        )!!

        assertNotNull("Workflow creation failed", updatedWorkflowResponse)
        assertNotNull(updatedWorkflowResponse.workflow)
        assertEquals("Workflow id changed", workflowResponse.id, updatedWorkflowResponse.id)
        assertTrue("incorrect version", updatedWorkflowResponse.version > 0)

        workflowById = searchWorkflow(updatedWorkflowResponse.id)!!

        // Verify workflow
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowById.id)
        assertTrue("incorrect version", workflowById.version > 0)
        assertEquals("Workflow name not correct", updatedWorkflowResponse.workflow.name, workflowById.name)
        assertEquals("Workflow owner not correct", updatedWorkflowResponse.workflow.owner, workflowById.owner)
        assertEquals("Workflow input not correct", updatedWorkflowResponse.workflow.inputs, workflowById.inputs)

        // Delegate verification
        @Suppress("UNCHECKED_CAST")
        val delegates = (workflowById.inputs as List<CompositeInput>)[0].sequence.delegates.sortedBy { it.order }
        assertEquals("Delegates size not correct", 1, delegates.size)

        val delegate1 = delegates[0]
        assertNotNull(delegate1)
        assertEquals("Delegate1 order not correct", 1, delegate1.order)
        assertEquals("Delegate1 id not correct", monitorResponse1.id, delegate1.monitorId)
    }

    fun `test update workflow doesn't exist failure`() {
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3")
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        val monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitorResponse1 = createMonitor(monitor1)!!

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse1.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)

        try {
            upsertWorkflow(workflow, "testId", RestRequest.Method.PUT)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Workflow with testId is not found")
                )
            }
        }
    }

    fun `test get workflow`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
        )

        val monitorResponse = createMonitor(monitor)!!

        val workflowRequest = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )

        val workflowResponse = upsertWorkflow(workflowRequest)!!
        assertNotNull("Workflow creation failed", workflowResponse)
        assertNotNull(workflowResponse.workflow)
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowResponse.id)
        assertTrue("incorrect version", workflowResponse.version > 0)

        val getWorkflowResponse = getWorkflowById(id = workflowResponse.id)
        assertNotNull(getWorkflowResponse)

        val workflowById = getWorkflowResponse.workflow!!
        // Verify workflow
        assertNotEquals("response is missing Id", Monitor.NO_ID, getWorkflowResponse.id)
        assertTrue("incorrect version", getWorkflowResponse.version > 0)
        assertEquals("Workflow name not correct", workflowRequest.name, workflowById.name)
        assertEquals("Workflow owner not correct", workflowRequest.owner, workflowById.owner)
        assertEquals("Workflow input not correct", workflowRequest.inputs, workflowById.inputs)

        // Delegate verification
        @Suppress("UNCHECKED_CAST")
        val delegates = (workflowById.inputs as List<CompositeInput>)[0].sequence.delegates.sortedBy { it.order }
        assertEquals("Delegates size not correct", 1, delegates.size)

        val delegate = delegates[0]
        assertNotNull(delegate)
        assertEquals("Delegate order not correct", 1, delegate.order)
        assertEquals("Delegate id not correct", monitorResponse.id, delegate.monitorId)
    }

    fun `test get workflow for invalid id monitor index doesn't exist`() {
        // Get workflow for non existing workflow id
        try {
            getWorkflowById(id = "-1")
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Workflow not found")
                )
            }
        }
    }

    fun `test get workflow for invalid id monitor index exists`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
        )
        createMonitor(monitor)
        // Get workflow for non existing workflow id
        try {
            getWorkflowById(id = "-1")
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Workflow not found")
                )
            }
        }
    }

    fun `test delete workflow keeping delegate monitor`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)!!

        val workflowRequest = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflowRequest)!!
        val workflowId = workflowResponse.id
        val getWorkflowResponse = getWorkflowById(id = workflowResponse.id)

        assertNotNull(getWorkflowResponse)
        assertEquals(workflowId, getWorkflowResponse.id)

        deleteWorkflow(workflowId, false)
        // Verify that the workflow is deleted
        try {
            getWorkflowById(workflowId)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Workflow not found.")
                )
            }
        }
        // Verify that the monitor is not deleted
        val existingDelegate = getMonitorResponse(monitorResponse.id)
        assertNotNull(existingDelegate)
    }

    fun `test delete workflow delegate monitor deleted`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)!!

        val workflowRequest = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflowRequest)!!
        val workflowId = workflowResponse.id
        val getWorkflowResponse = getWorkflowById(id = workflowResponse.id)

        assertNotNull(getWorkflowResponse)
        assertEquals(workflowId, getWorkflowResponse.id)

        deleteWorkflow(workflowId, true)
        // Verify that the workflow is deleted
        try {
            getWorkflowById(workflowId)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Workflow not found.")
                )
            }
        }
        // Verify that the monitor is deleted
        try {
            getMonitorResponse(monitorResponse.id)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetMonitor Action error ",
                    it.contains("Monitor not found")
                )
            }
        }
    }

    fun `test delete executed workflow with metadata deleted`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3")
        val docLevelInput1 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1)
        )
        val monitorResponse = createMonitor(monitor1)!!

        val docQuery2 = DocLevelQuery(query = "source.ip.v6.v2:16645", name = "4")
        val docLevelInput2 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery2))
        val trigger2 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput2),
            triggers = listOf(trigger2),
        )

        val monitorResponse2 = createMonitor(monitor2)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)

        val workflowId = workflowResponse.id
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        val monitorsRunResults = executeWorkflowResponse.workflowRunResult.workflowRunResult
        assertEquals(2, monitorsRunResults.size)

        val workflowMetadata = searchWorkflowMetadata(workflowId)
        assertNotNull(workflowMetadata)

        val monitorMetadataId1 = getDelegateMonitorMetadataId(workflowMetadata, monitorResponse)
        val monitorMetadata1 = searchMonitorMetadata(monitorMetadataId1)
        assertNotNull(monitorMetadata1)

        val monitorMetadataId2 = getDelegateMonitorMetadataId(workflowMetadata, monitorResponse2)
        val monitorMetadata2 = searchMonitorMetadata(monitorMetadataId2)
        assertNotNull(monitorMetadata2)

        assertFalse(monitorMetadata1!!.id == monitorMetadata2!!.id)

        deleteWorkflow(workflowId, true)
        // Verify that the workflow is deleted
        try {
            getWorkflowById(workflowId)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Workflow not found.")
                )
            }
        }
        // Verify that the workflow metadata is deleted
        try {
            searchWorkflowMetadata(workflowId)
            fail("expected searchWorkflowMetadata method to throw exception")
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetMonitor Action error ",
                    it.contains("List is empty")
                )
            }
        }
        // Verify that the monitors metadata are deleted
        try {
            searchMonitorMetadata(monitorMetadataId1)
            fail("expected searchMonitorMetadata method to throw exception")
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetMonitor Action error ",
                    it.contains("List is empty")
                )
            }
        }

        try {
            searchMonitorMetadata(monitorMetadataId2)
            fail("expected searchMonitorMetadata method to throw exception")
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetMonitor Action error ",
                    it.contains("List is empty")
                )
            }
        }
    }

    private fun getDelegateMonitorMetadataId(
        workflowMetadata: WorkflowMetadata?,
        monitorResponse: IndexMonitorResponse,
    ) = "${workflowMetadata!!.id}-${monitorResponse.id}-metadata"

    fun `test delete workflow delegate monitor part of another workflow not deleted`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)!!

        val workflowRequest = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflowRequest)!!
        val workflowId = workflowResponse.id
        val getWorkflowResponse = getWorkflowById(id = workflowResponse.id)

        assertNotNull(getWorkflowResponse)
        assertEquals(workflowId, getWorkflowResponse.id)

        val workflowRequest2 = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse2 = upsertWorkflow(workflowRequest2)!!
        val workflowId2 = workflowResponse2.id
        val getWorkflowResponse2 = getWorkflowById(id = workflowResponse2.id)

        assertNotNull(getWorkflowResponse2)
        assertEquals(workflowId2, getWorkflowResponse2.id)

        try {
            deleteWorkflow(workflowId, true)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("[Not allowed to delete ${monitorResponse.id} monitors")
                )
            }
        }
        val existingMonitor = getMonitorResponse(monitorResponse.id)
        assertNotNull(existingMonitor)
    }

    fun `test trying to delete monitor that is part of workflow sequence`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)!!

        val workflowRequest = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )

        val workflowResponse = upsertWorkflow(workflowRequest)!!
        val workflowId = workflowResponse.id
        val getWorkflowResponse = getWorkflowById(id = workflowResponse.id)

        assertNotNull(getWorkflowResponse)
        assertEquals(workflowId, getWorkflowResponse.id)

        // Verify that the monitor can't be deleted because it's included in the workflow
        try {
            deleteMonitor(monitorResponse.id)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning DeleteMonitor Action error ",
                    it.contains("Monitor can't be deleted because it is a part of workflow(s)")
                )
            }
        }
    }

    fun `test delete workflow for invalid id monitor index doesn't exists`() {
        // Try deleting non-existing workflow
        try {
            deleteWorkflow("-1")
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning DeleteWorkflow Action error ",
                    it.contains("Workflow not found.")
                )
            }
        }
    }

    fun `test delete workflow for invalid id monitor index exists`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
        )
        createMonitor(monitor)
        // Try deleting non-existing workflow
        try {
            deleteWorkflow("-1")
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning DeleteWorkflow Action error ",
                    it.contains("Workflow not found.")
                )
            }
        }
    }

    fun `test create workflow without delegate failure`() {
        val workflow = randomWorkflow(
            monitorIds = Collections.emptyList()
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Delegates list can not be empty.")
                )
            }
        }
    }

    fun `test create workflow with 26 delegates failure`() {
        val monitorsIds = mutableListOf<String>()
        for (i in 0..25) {
            monitorsIds.add(UUID.randomUUID().toString())
        }
        val workflow = randomWorkflow(
            monitorIds = monitorsIds
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Delegates list can not be larger then 25.")
                )
            }
        }
    }

    fun `test update workflow without delegate failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
        )

        val monitorResponse1 = createMonitor(monitor1)!!
        val monitorResponse2 = createMonitor(monitor2)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse1.id, monitorResponse2.id)
        )

        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)

        workflow = randomWorkflow(
            id = workflowResponse.id,
            monitorIds = Collections.emptyList()
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Delegates list can not be empty.")
                )
            }
        }
    }

    fun `test create workflow duplicate delegate failure`() {
        val workflow = randomWorkflow(
            monitorIds = listOf("1", "1", "2")
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Duplicate delegates not allowed")
                )
            }
        }
    }

    fun `test update workflow duplicate delegate failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )

        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)

        workflow = randomWorkflow(
            id = workflowResponse.id,
            monitorIds = listOf("1", "1", "2")
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Duplicate delegates not allowed")
                )
            }
        }
    }

    fun `test create workflow delegate monitor doesn't exist failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)!!

        val workflow = randomWorkflow(
            monitorIds = listOf("-1", monitorResponse.id)
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("are not valid monitor ids")
                )
            }
        }
    }

    fun `test update workflow delegate monitor doesn't exist failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)

        workflow = randomWorkflow(
            id = workflowResponse.id,
            monitorIds = listOf("-1", monitorResponse.id)
        )

        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("are not valid monitor ids")
                )
            }
        }
    }

    fun `test create workflow sequence order not correct failure`() {
        val delegates = listOf(
            Delegate(1, "monitor-1"),
            Delegate(1, "monitor-2"),
            Delegate(2, "monitor-3")
        )
        val workflow = randomWorkflowWithDelegates(
            delegates = delegates
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Sequence ordering of delegate monitor shouldn't contain duplicate order values")
                )
            }
        }
    }

    fun `test update workflow sequence order not correct failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)

        val delegates = listOf(
            Delegate(1, "monitor-1"),
            Delegate(1, "monitor-2"),
            Delegate(2, "monitor-3")
        )
        workflow = randomWorkflowWithDelegates(
            id = workflowResponse.id,
            delegates = delegates
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Sequence ordering of delegate monitor shouldn't contain duplicate order values")
                )
            }
        }
    }

    fun `test create workflow chained findings monitor not in sequence failure`() {
        val delegates = listOf(
            Delegate(1, "monitor-1"),
            Delegate(2, "monitor-2", ChainedMonitorFindings("monitor-1")),
            Delegate(3, "monitor-3", ChainedMonitorFindings("monitor-x"))
        )
        val workflow = randomWorkflowWithDelegates(
            delegates = delegates
        )

        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Chained Findings Monitor monitor-x doesn't exist in sequence")
                )
            }
        }
    }

    fun `test create workflow query monitor chained findings monitor failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val docMonitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val docMonitorResponse = createMonitor(docMonitor)!!

        val queryMonitor = randomQueryLevelMonitor()
        val queryMonitorResponse = createMonitor(queryMonitor)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(queryMonitorResponse.id, docMonitorResponse.id)
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Query level monitor can't be part of chained findings")
                )
            }
        }
    }

    fun `test create workflow delegate and chained finding monitor different indices failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val docMonitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val docMonitorResponse = createMonitor(docMonitor)!!

        val index1 = "$index-1"
        createTestIndex(index1)

        val docLevelInput1 = DocLevelMonitorInput(
            "description", listOf(index1), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )

        val docMonitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger)
        )
        val docMonitorResponse1 = createMonitor(docMonitor1)!!

        val workflow = randomWorkflow(
            monitorIds = listOf(docMonitorResponse1.id, docMonitorResponse.id)
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Delegate monitor and it's chained finding monitor must query the same indices")
                )
            }
        }
    }

    fun `test create workflow when monitor index not initialized failure`() {
        val delegates = listOf(
            Delegate(1, "monitor-1")
        )
        val workflow = randomWorkflowWithDelegates(
            delegates = delegates
        )

        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Monitors not found")
                )
            }
        }
    }

    fun `test update workflow chained findings monitor not in sequence failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)

        val delegates = listOf(
            Delegate(1, "monitor-1"),
            Delegate(2, "monitor-2", ChainedMonitorFindings("monitor-1")),
            Delegate(3, "monitor-3", ChainedMonitorFindings("monitor-x"))
        )
        workflow = randomWorkflowWithDelegates(
            id = workflowResponse.id,
            delegates = delegates
        )

        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Chained Findings Monitor monitor-x doesn't exist in sequence")
                )
            }
        }
    }

    fun `test create workflow chained findings order not correct failure`() {
        val delegates = listOf(
            Delegate(1, "monitor-1"),
            Delegate(3, "monitor-2", ChainedMonitorFindings("monitor-1")),
            Delegate(2, "monitor-3", ChainedMonitorFindings("monitor-2"))
        )
        val workflow = randomWorkflowWithDelegates(
            delegates = delegates
        )

        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Chained Findings Monitor monitor-2 should be executed before monitor monitor-3")
                )
            }
        }
    }

    fun `test update workflow chained findings order not correct failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)

        val delegates = listOf(
            Delegate(1, "monitor-1"),
            Delegate(3, "monitor-2", ChainedMonitorFindings("monitor-1")),
            Delegate(2, "monitor-3", ChainedMonitorFindings("monitor-2"))
        )
        workflow = randomWorkflowWithDelegates(
            delegates = delegates
        )

        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Chained Findings Monitor monitor-2 should be executed before monitor monitor-3")
                )
            }
        }
    }
}
