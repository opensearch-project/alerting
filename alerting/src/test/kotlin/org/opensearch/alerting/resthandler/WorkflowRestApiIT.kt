/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.opensearch.alerting.ALWAYS_RUN
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.WORKFLOW_ALERTING_BASE_URI
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.randomBucketLevelMonitor
import org.opensearch.alerting.randomDocumentLevelMonitor
import org.opensearch.alerting.randomDocumentLevelTrigger
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.alerting.randomWorkflow
import org.opensearch.alerting.randomWorkflowWithDelegates
import org.opensearch.client.ResponseException
import org.opensearch.commons.alerting.model.ChainedMonitorFindings
import org.opensearch.commons.alerting.model.CompositeInput
import org.opensearch.commons.alerting.model.Delegate
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.junit.annotations.TestLogging
import java.util.Collections
import java.util.Locale
import java.util.UUID

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class WorkflowRestApiIT : AlertingRestTestCase() {

    fun `test create workflow success`() {
        val index = createTestIndex()
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3")
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )

        val createResponse = client().makeRequest("POST", WORKFLOW_ALERTING_BASE_URI, emptyMap(), workflow.toHttpEntity())

        assertEquals("Create workflow failed", RestStatus.CREATED, createResponse.restStatus())

        val responseBody = createResponse.asMap()
        val createdId = responseBody["_id"] as String
        val createdVersion = responseBody["_version"] as Int

        assertNotEquals("response is missing Id", Workflow.NO_ID, createdId)
        assertTrue("incorrect version", createdVersion > 0)
        assertEquals("Incorrect Location header", "$WORKFLOW_ALERTING_BASE_URI/$createdId", createResponse.getHeader("Location"))
    }

    fun `test create workflow with different monitor types success`() {
        val index = createTestIndex()
        val docQuery = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3")
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val docLevelMonitorResponse = createMonitor(monitor)

        val bucketLevelMonitor = randomBucketLevelMonitor(
            inputs = listOf(
                SearchInput(
                    listOf(index),
                    SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                        .aggregation(TermsAggregationBuilder("test_agg").field("test_field"))
                )
            )
        )
        val bucketLevelMonitorResponse = createMonitor(bucketLevelMonitor)

        val workflow = randomWorkflow(
            monitorIds = listOf(docLevelMonitorResponse.id, bucketLevelMonitorResponse.id)
        )

        val createResponse = client().makeRequest("POST", WORKFLOW_ALERTING_BASE_URI, emptyMap(), workflow.toHttpEntity())

        assertEquals("Create workflow failed", RestStatus.CREATED, createResponse.restStatus())

        val responseBody = createResponse.asMap()
        val createdId = responseBody["_id"] as String
        val createdVersion = responseBody["_version"] as Int

        assertNotEquals("response is missing Id", Workflow.NO_ID, createdId)
        assertTrue("incorrect version", createdVersion > 0)
        assertEquals("Incorrect Location header", "$WORKFLOW_ALERTING_BASE_URI/$createdId", createResponse.getHeader("Location"))

        val workflowById = getWorkflow(createdId)
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
        assertEquals("Delegate1 id not correct", docLevelMonitorResponse.id, delegate1.monitorId)

        val delegate2 = delegates[1]
        assertNotNull(delegate2)
        assertEquals("Delegate2 order not correct", 2, delegate2.order)
        assertEquals("Delegate2 id not correct", bucketLevelMonitorResponse.id, delegate2.monitorId)
        assertEquals(
            "Delegate2 Chained finding not correct", docLevelMonitorResponse.id, delegate2.chainedMonitorFindings!!.monitorId
        )
    }

    fun `test create workflow without delegate failure`() {
        val workflow = randomWorkflow(
            monitorIds = Collections.emptyList()
        )
        try {
            createWorkflow(workflow)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
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
            createWorkflow(workflow)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Duplicate delegates not allowed")
                )
            }
        }
    }

    fun `test create workflow delegate monitor doesn't exist failure`() {
        val index = createTestIndex()
        val docQuery = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3")
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val docLevelMonitorResponse = createMonitor(monitor)

        val workflow = randomWorkflow(
            monitorIds = listOf("-1", docLevelMonitorResponse.id)
        )
        try {
            createWorkflow(workflow)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
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
            createWorkflow(workflow)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
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
            createWorkflow(workflow)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
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
            createWorkflow(workflow)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Chained Findings Monitor monitor-2 should be executed before monitor monitor-3")
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
            createWorkflow(workflow)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Monitors not found")
                )
            }
        }
    }

    fun `test create workflow delegate and chained finding monitor different indices failure`() {
        val index = randomAlphaOfLength(10).lowercase(Locale.ROOT)
        createTestIndex(index)

        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val docMonitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val docMonitorResponse = createMonitor(docMonitor)

        val index1 = "$index-1"
        createTestIndex(index1)

        val docLevelInput1 = DocLevelMonitorInput(
            "description", listOf(index1), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )

        val docMonitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger)
        )
        val docMonitorResponse1 = createMonitor(docMonitor1)

        val workflow = randomWorkflow(
            monitorIds = listOf(docMonitorResponse1.id, docMonitorResponse.id)
        )
        try {
            createWorkflow(workflow)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Delegate monitor and it's chained finding monitor must query the same indices")
                )
            }
        }
    }

    fun `test create workflow query monitor chained findings monitor failure`() {
        val index = createTestIndex()
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val docMonitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val docMonitorResponse = createMonitor(docMonitor)

        val queryMonitor = randomQueryLevelMonitor()
        val queryMonitorResponse = createMonitor(queryMonitor)

        val workflow = randomWorkflow(
            monitorIds = listOf(queryMonitorResponse.id, docMonitorResponse.id)
        )
        try {
            createWorkflow(workflow)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Query level monitor can't be part of chained findings")
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
            createWorkflow(workflow)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Delegates list can not be larger then 25.")
                )
            }
        }
    }

    fun `test update workflow add monitor success`() {
        val index = createTestIndex()
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3")
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )

        val createResponse = client().makeRequest("POST", WORKFLOW_ALERTING_BASE_URI, emptyMap(), workflow.toHttpEntity())

        assertEquals("Create workflow failed", RestStatus.CREATED, createResponse.restStatus())

        val responseBody = createResponse.asMap()
        val createdId = responseBody["_id"] as String
        val createdVersion = responseBody["_version"] as Int

        assertNotEquals("response is missing Id", Workflow.NO_ID, createdId)
        assertTrue("incorrect version", createdVersion > 0)
        assertEquals("Incorrect Location header", "$WORKFLOW_ALERTING_BASE_URI/$createdId", createResponse.getHeader("Location"))

        val monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse2 = createMonitor(monitor2)

        val updatedWorkflow = randomWorkflow(
            id = createdId,
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id)
        )

        val updateResponse = client().makeRequest("PUT", updatedWorkflow.relativeUrl(), emptyMap(), updatedWorkflow.toHttpEntity())

        assertEquals("Update workflow failed", RestStatus.OK, updateResponse.restStatus())

        val updateResponseBody = updateResponse.asMap()
        val updatedId = updateResponseBody["_id"] as String
        val updatedVersion = updateResponseBody["_version"] as Int

        assertNotEquals("response is missing Id", Workflow.NO_ID, updatedId)
        assertTrue("incorrect version", updatedVersion > 0)

        val workflowById = getWorkflow(updatedId)
        assertNotNull(workflowById)
        // Delegate verification
        @Suppress("UNCHECKED_CAST")
        val delegates = (workflowById.inputs as List<CompositeInput>)[0].sequence.delegates.sortedBy { it.order }
        assertEquals("Delegates size not correct", 2, delegates.size)

        val delegate1 = delegates[0]
        assertNotNull(delegate1)
        assertEquals("Delegate1 order not correct", 1, delegate1.order)
        assertEquals("Delegate1 id not correct", monitorResponse.id, delegate1.monitorId)

        val delegate2 = delegates[1]
        assertNotNull(delegate2)
        assertEquals("Delegate2 order not correct", 2, delegate2.order)
        assertEquals("Delegate2 id not correct", monitorResponse2.id, delegate2.monitorId)
        assertEquals(
            "Delegate2 Chained finding not correct", monitorResponse.id, delegate2.chainedMonitorFindings!!.monitorId
        )
    }

    fun `test update workflow remove monitor success`() {
        val index = createTestIndex()
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3")
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)

        val monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse2 = createMonitor(monitor2)

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id)
        )

        val createResponse = client().makeRequest("POST", WORKFLOW_ALERTING_BASE_URI, emptyMap(), workflow.toHttpEntity())

        assertEquals("Create workflow failed", RestStatus.CREATED, createResponse.restStatus())

        val responseBody = createResponse.asMap()
        val createdId = responseBody["_id"] as String
        val createdVersion = responseBody["_version"] as Int

        assertNotEquals("response is missing Id", Workflow.NO_ID, createdId)
        assertTrue("incorrect version", createdVersion > 0)
        assertEquals("Incorrect Location header", "$WORKFLOW_ALERTING_BASE_URI/$createdId", createResponse.getHeader("Location"))

        var workflowById = getWorkflow(createdId)
        assertNotNull(workflowById)
        // Delegate verification
        @Suppress("UNCHECKED_CAST")
        var delegates = (workflowById.inputs as List<CompositeInput>)[0].sequence.delegates.sortedBy { it.order }
        assertEquals("Delegates size not correct", 2, delegates.size)

        val updatedWorkflow = randomWorkflow(
            id = createdId,
            monitorIds = listOf(monitorResponse.id)
        )

        val updateResponse = client().makeRequest("PUT", updatedWorkflow.relativeUrl(), emptyMap(), updatedWorkflow.toHttpEntity())

        assertEquals("Update workflow failed", RestStatus.OK, updateResponse.restStatus())

        val updateResponseBody = updateResponse.asMap()
        val updatedId = updateResponseBody["_id"] as String
        val updatedVersion = updateResponseBody["_version"] as Int

        assertNotEquals("response is missing Id", Workflow.NO_ID, updatedId)
        assertTrue("incorrect version", updatedVersion > 0)

        workflowById = getWorkflow(updatedId)
        assertNotNull(workflowById)
        // Delegate verification
        @Suppress("UNCHECKED_CAST")
        delegates = (workflowById.inputs as List<CompositeInput>)[0].sequence.delegates.sortedBy { it.order }
        assertEquals("Delegates size not correct", 1, delegates.size)

        val delegate1 = delegates[0]
        assertNotNull(delegate1)
        assertEquals("Delegate1 order not correct", 1, delegate1.order)
        assertEquals("Delegate1 id not correct", monitorResponse.id, delegate1.monitorId)
    }

    fun `test update workflow change order of delegate monitors`() {
        val index = createTestIndex()
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3")
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse1 = createMonitor(monitor1)
        val monitorResponse2 = createMonitor(monitor2)

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse1.id, monitorResponse2.id)
        )

        val workflowResponse = createWorkflow(workflow)
        assertNotNull("Workflow creation failed", workflowResponse)
        assertNotNull(workflow)
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowResponse.id)
        assertTrue("incorrect version", workflowResponse.version > 0)

        var workflowById = getWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        val updatedWorkflowResponse = updateWorkflow(
            randomWorkflow(
                id = workflowById.id,
                monitorIds = listOf(monitorResponse2.id, monitorResponse1.id)
            )
        )

        assertNotNull("Workflow creation failed", updatedWorkflowResponse)
        assertNotNull(updatedWorkflowResponse)
        assertEquals(
            "Workflow id changed",
            workflowResponse.id,
            updatedWorkflowResponse.id
        )
        assertTrue("incorrect version", updatedWorkflowResponse.version > 0)

        workflowById = getWorkflow(updatedWorkflowResponse.id)

        // Verify workflow
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowById.id)
        assertTrue("incorrect version", workflowById.version > 0)
        assertEquals(
            "Workflow name not correct",
            updatedWorkflowResponse.name,
            workflowById.name
        )
        assertEquals(
            "Workflow owner not correct",
            updatedWorkflowResponse.owner,
            workflowById.owner
        )
        assertEquals(
            "Workflow input not correct",
            updatedWorkflowResponse.inputs,
            workflowById.inputs
        )

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

    fun `test update workflow doesn't exist failure`() {
        val index = createTestIndex()
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3")
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN))
        )

        val monitorResponse1 = createMonitor(monitor1)

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse1.id)
        )
        val workflowResponse = createWorkflow(workflow)
        assertNotNull("Workflow creation failed", workflowResponse)

        try {
            updateWorkflow(workflow.copy(id = "testId"))
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Workflow with testId is not found")
                )
            }
        }
        val updatedWorkflow = updateWorkflow(workflowResponse.copy(enabled = true))
        assertNotNull(updatedWorkflow)
        val getWorkflow = getWorkflow(workflowId = updatedWorkflow.id)
        assertTrue(getWorkflow.enabled)
    }

    fun `test update workflow duplicate delegate failure`() {
        val index = createTestIndex()
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )

        val workflowResponse = createWorkflow(workflow)
        assertNotNull("Workflow creation failed", workflowResponse)

        workflow = randomWorkflow(
            id = workflowResponse.id,
            monitorIds = listOf("1", "1", "2")
        )
        try {
            updateWorkflow(workflow)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Duplicate delegates not allowed")
                )
            }
        }
    }

    fun `test update workflow delegate monitor doesn't exist failure`() {
        val index = createTestIndex()
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = createWorkflow(workflow)
        assertNotNull("Workflow creation failed", workflowResponse)

        workflow = randomWorkflow(
            id = workflowResponse.id,
            monitorIds = listOf("-1", monitorResponse.id)
        )

        try {
            updateWorkflow(workflow)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("are not valid monitor ids")
                )
            }
        }
    }

    fun `test update workflow sequence order not correct failure`() {
        val index = createTestIndex()
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = createWorkflow(workflow)
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
            updateWorkflow(workflow)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Sequence ordering of delegate monitor shouldn't contain duplicate order values")
                )
            }
        }
    }

    fun `test update workflow chained findings monitor not in sequence failure`() {
        val index = createTestIndex()
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = createWorkflow(workflow)
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
            updateWorkflow(workflow)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Chained Findings Monitor monitor-x doesn't exist in sequence")
                )
            }
        }
    }

    fun `test update workflow chained findings order not correct failure`() {
        val index = createTestIndex()
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = createWorkflow(workflow)
        assertNotNull("Workflow creation failed", workflowResponse)

        val delegates = listOf(
            Delegate(1, "monitor-1"),
            Delegate(3, "monitor-2", ChainedMonitorFindings("monitor-1")),
            Delegate(2, "monitor-3", ChainedMonitorFindings("monitor-2"))
        )
        workflow = randomWorkflowWithDelegates(
            id = workflowResponse.id,
            delegates = delegates
        )

        try {
            updateWorkflow(workflow)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Chained Findings Monitor monitor-2 should be executed before monitor monitor-3")
                )
            }
        }
    }

    @Throws(Exception::class)
    fun `test getting a workflow`() {
        val query = randomQueryLevelMonitor()
        val monitor = createMonitor(query)
        val storedMonitor = getMonitor(monitor.id)

        assertEquals("Indexed and retrieved monitor differ", monitor, storedMonitor)

        val workflow = createRandomWorkflow(monitorIds = listOf(monitor.id))

        val storedWorkflow = getWorkflow(workflow.id)

        assertEquals("Indexed and retrieved workflow differ", workflow.id, storedWorkflow.id)
        val delegates = (storedWorkflow.inputs[0] as CompositeInput).sequence.delegates
        assertEquals("Delegate list not correct", 1, delegates.size)
        assertEquals("Delegate order id not correct", 1, delegates[0].order)
        assertEquals("Delegate id list not correct", monitor.id, delegates[0].monitorId)
    }

    @Throws(Exception::class)
    fun `test getting a workflow that doesn't exist`() {
        try {
            getWorkflow(randomAlphaOfLength(20))
            fail("expected response exception")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test checking if a workflow exists`() {
        val query = randomQueryLevelMonitor()
        val monitor = createMonitor(query)

        // val monitor = createMonitor(docLevelMonitor)
        val storedMonitor = getMonitor(monitor.id)
        assertEquals("Indexed and retrieved monitor differ", monitor, storedMonitor)
        val workflow = createRandomWorkflow(monitorIds = listOf(monitor.id))

        val headResponse = client().makeRequest("HEAD", workflow.relativeUrl())
        assertEquals("Unable to HEAD workflow", RestStatus.OK, headResponse.restStatus())
        assertNull("Workflow response contains unexpected body", headResponse.entity)
    }

    fun `test checking if a non-existent workflow exists`() {
        val headResponse = client().makeRequest("HEAD", "$WORKFLOW_ALERTING_BASE_URI/foobarbaz")
        assertEquals("Unexpected status", RestStatus.NOT_FOUND, headResponse.restStatus())
    }

    fun `test delete workflow`() {
        val query = randomQueryLevelMonitor()
        val monitor = createMonitor(query)

        val workflowRequest = randomWorkflow(
            monitorIds = listOf(monitor.id)
        )
        val workflowResponse = createWorkflow(workflowRequest)
        val workflowId = workflowResponse.id
        val getWorkflowResponse = getWorkflow(workflowResponse.id)

        assertNotNull(getWorkflowResponse)
        assertEquals(workflowId, getWorkflowResponse.id)

        client().makeRequest("DELETE", getWorkflowResponse.relativeUrl())

        // Verify that the workflow is deleted
        try {
            getWorkflow(workflowId)
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Workflow not found.")
                )
            }
        }
    }

    fun `test delete workflow delete delegate monitors`() {
        val query = randomQueryLevelMonitor()
        val monitor = createMonitor(query)

        val workflowRequest = randomWorkflow(
            monitorIds = listOf(monitor.id)
        )
        val workflowResponse = createWorkflow(workflowRequest)
        val workflowId = workflowResponse.id
        val getWorkflowResponse = getWorkflow(workflowResponse.id)

        assertNotNull(getWorkflowResponse)
        assertEquals(workflowId, getWorkflowResponse.id)

        client().makeRequest("DELETE", getWorkflowResponse.relativeUrl().plus("?deleteDelegateMonitors=true"))

        // Verify that the workflow is deleted
        try {
            getWorkflow(workflowId)
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Workflow not found.")
                )
            }
        }

        // Verify that delegate monitor is deleted
        try {
            getMonitor(monitor.id)
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Monitor not found.")
                )
            }
        }
    }

    fun `test delete workflow preserve delegate monitors`() {
        val query = randomQueryLevelMonitor()
        val monitor = createMonitor(query)

        val workflowRequest = randomWorkflow(
            monitorIds = listOf(monitor.id)
        )
        val workflowResponse = createWorkflow(workflowRequest)
        val workflowId = workflowResponse.id
        val getWorkflowResponse = getWorkflow(workflowResponse.id)

        assertNotNull(getWorkflowResponse)
        assertEquals(workflowId, getWorkflowResponse.id)

        client().makeRequest("DELETE", getWorkflowResponse.relativeUrl().plus("?deleteDelegateMonitors=false"))

        // Verify that the workflow is deleted
        try {
            getWorkflow(workflowId)
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Workflow not found.")
                )
            }
        }

        // Verify that delegate monitor is not deleted
        val delegateMonitor = getMonitor(monitor.id)
        assertNotNull(delegateMonitor)
    }

    @Throws(Exception::class)
    fun `test deleting a workflow that doesn't exist`() {
        try {
            client().makeRequest("DELETE", "$WORKFLOW_ALERTING_BASE_URI/foobarbaz")
            fail("expected 404 ResponseException")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
