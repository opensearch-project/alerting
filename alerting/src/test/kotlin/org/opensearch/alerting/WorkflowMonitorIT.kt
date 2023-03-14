/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.transport.WorkflowSingleNodeTestCase
import org.opensearch.commons.alerting.model.ChainedFindings
import org.opensearch.commons.alerting.model.CompositeInput
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.Delegate
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.rest.RestRequest
import java.util.Collections

class WorkflowMonitorIT : WorkflowSingleNodeTestCase() {

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
            "Delegate2 Chained finding not correct", monitorResponse1.id, delegate2.chainedFindings!!.monitorId
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
            "Delegate2 Chained finding not correct", monitorResponse1.id, delegate2.chainedFindings!!.monitorId
        )

        val delegate3 = delegates[2]
        assertNotNull(delegate3)
        assertEquals("Delegate3 order not correct", 3, delegate3.order)
        assertEquals("Delegate3 id not correct", monitorResponse3.id, delegate3.monitorId)
        assertEquals(
            "Delegate3 Chained finding not correct", monitorResponse2.id, delegate3.chainedFindings!!.monitorId
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
            "Delegate2 Chained finding not correct", monitorResponse2.id, delegate2.chainedFindings!!.monitorId
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
            Delegate(2, "monitor-2", ChainedFindings("monitor-1")),
            Delegate(3, "monitor-3", ChainedFindings("monitor-x"))
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
            Delegate(2, "monitor-2", ChainedFindings("monitor-1")),
            Delegate(3, "monitor-3", ChainedFindings("monitor-x"))
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
            Delegate(3, "monitor-2", ChainedFindings("monitor-1")),
            Delegate(2, "monitor-3", ChainedFindings("monitor-2"))
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
            Delegate(3, "monitor-2", ChainedFindings("monitor-1")),
            Delegate(2, "monitor-3", ChainedFindings("monitor-2"))
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
