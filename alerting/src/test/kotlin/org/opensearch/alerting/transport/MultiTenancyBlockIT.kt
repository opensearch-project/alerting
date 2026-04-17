/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.action.ExecuteMonitorAction
import org.opensearch.alerting.action.ExecuteMonitorRequest
import org.opensearch.alerting.action.ExecuteWorkflowAction
import org.opensearch.alerting.action.ExecuteWorkflowRequest
import org.opensearch.alerting.randomClusterMetricsMonitor
import org.opensearch.alerting.randomDocumentLevelMonitor
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.DeleteWorkflowRequest
import org.opensearch.commons.alerting.action.GetWorkflowAlertsRequest
import org.opensearch.commons.alerting.action.GetWorkflowRequest
import org.opensearch.commons.alerting.action.IndexWorkflowRequest
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.Table
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.rest.RestRequest
import java.time.Instant

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class MultiTenancyBlockIT : AlertingSingleNodeTestCase() {

    override fun nodeSettings(): Settings {
        return Settings.builder()
            .put(super.nodeSettings())
            .put("plugins.alerting.multi_tenancy_enabled", true)
            .build()
    }

    // --- Workflow tests ---

    fun `test index workflow fails when multi-tenancy is enabled`() {
        val request = IndexWorkflowRequest(
            workflowId = Workflow.NO_ID,
            seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE,
            method = RestRequest.Method.POST,
            workflow = org.opensearch.alerting.randomWorkflow(monitorIds = emptyList())
        )
        val exception = expectThrows(Exception::class.java) {
            client().execute(AlertingActions.INDEX_WORKFLOW_ACTION_TYPE, request).actionGet()
        }
        assertTrue(exception.message!!.contains("Workflow operations are not allowed"))
    }

    fun `test get workflow fails when multi-tenancy is enabled`() {
        val exception = expectThrows(Exception::class.java) {
            client().execute(AlertingActions.GET_WORKFLOW_ACTION_TYPE, GetWorkflowRequest("test-id", RestRequest.Method.GET)).actionGet()
        }
        assertTrue(exception.message!!.contains("Workflow operations are not allowed"))
    }

    fun `test delete workflow fails when multi-tenancy is enabled`() {
        val exception = expectThrows(Exception::class.java) {
            client().execute(AlertingActions.DELETE_WORKFLOW_ACTION_TYPE, DeleteWorkflowRequest("test-id", false)).actionGet()
        }
        assertTrue(exception.message!!.contains("Workflow operations are not allowed"))
    }

    fun `test execute workflow fails when multi-tenancy is enabled`() {
        val exception = expectThrows(Exception::class.java) {
            client().execute(
                ExecuteWorkflowAction.INSTANCE,
                ExecuteWorkflowRequest(true, TimeValue(Instant.now().toEpochMilli()), "test-id", null)
            ).actionGet()
        }
        assertTrue(exception.message!!.contains("Workflow operations are not allowed"))
    }

    fun `test get workflow alerts fails when multi-tenancy is enabled`() {
        val request = GetWorkflowAlertsRequest(
            table = Table("asc", "monitor_id", null, 100, 0, null),
            severityLevel = "ALL",
            alertState = Alert.State.ACTIVE.name,
            alertIndex = "",
            associatedAlertsIndex = "",
            monitorIds = emptyList(),
            workflowIds = listOf("test-id"),
            alertIds = emptyList(),
            getAssociatedAlerts = false
        )
        val exception = expectThrows(Exception::class.java) {
            client().execute(AlertingActions.GET_WORKFLOW_ALERTS_ACTION_TYPE, request).actionGet()
        }
        assertTrue(exception.message!!.contains("Workflow operations are not allowed"))
    }

    // --- Monitor type tests ---

    fun `test index doc-level monitor fails when multi-tenancy is enabled`() {
        val exception = expectThrows(Exception::class.java) {
            createMonitor(randomDocumentLevelMonitor())
        }
        assertTrue(exception.message!!.contains("not allowed when multi-tenancy is enabled"))
    }

    fun `test index cluster-metrics monitor fails when multi-tenancy is enabled`() {
        val exception = expectThrows(Exception::class.java) {
            createMonitor(randomClusterMetricsMonitor())
        }
        assertTrue(exception.message!!.contains("not allowed when multi-tenancy is enabled"))
    }

    fun `test index query-level monitor succeeds when multi-tenancy is enabled`() {
        val response = createMonitor(randomQueryLevelMonitor())
        assertNotNull(response)
        assertNotEquals(Monitor.NO_ID, response!!.id)
    }

    fun `test execute inline doc-level monitor fails when multi-tenancy is enabled`() {
        val exception = expectThrows(Exception::class.java) {
            val request = ExecuteMonitorRequest(true, TimeValue(Instant.now().toEpochMilli()), null, randomDocumentLevelMonitor())
            client().execute(ExecuteMonitorAction.INSTANCE, request).actionGet()
        }
        assertTrue(exception.message!!.contains("not allowed when multi-tenancy is enabled"))
    }
}
