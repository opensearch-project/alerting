/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.cleanup

import org.opensearch.alerting.core.lock.LockModel
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.index.query.TermQueryBuilder
import org.opensearch.index.query.TermsQueryBuilder
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant

class AlertCleanupTaskTests : OpenSearchTestCase() {

    fun `test a task round trips through xcontent`() {
        val task = AlertCleanupTask(
            jobId = "monitor-1",
            scope = CleanupScope.MONITOR,
            alertIndex = "custom-alerts",
            alertHistoryIndex = "custom-alerts-history-write",
            survivingTriggerIds = listOf("trigger-a", "trigger-b"),
            jobDeleted = false,
            cursor = 4_211L,
            movedCount = 900L,
            // Persisted as epoch millis, so a nanosecond-precision Instant would not compare equal.
            createdAt = Instant.ofEpochMilli(1_700_000_000_000L)
        )

        val parsed = parse(task)

        assertEquals(task, parsed)
    }

    fun `test jobDeleted survives a task with no surviving triggers`() {
        // Removing a monitor's last trigger leaves no surviving trigger ids while the monitor itself is still live, so
        // the two cannot be conflated: a task parsed as jobDeleted would be discarded by the drain and the alerts of the
        // removed trigger would leak.
        val lastTriggerRemoved = AlertCleanupTask(
            jobId = "monitor-1",
            scope = CleanupScope.MONITOR,
            alertIndex = "alerts",
            alertHistoryIndex = "history",
            survivingTriggerIds = emptyList(),
            jobDeleted = false
        )

        assertFalse("An empty surviving trigger list must not imply the job was deleted", parse(lastTriggerRemoved).jobDeleted)
        assertTrue(parse(lastTriggerRemoved.copy(jobDeleted = true)).jobDeleted)
    }

    fun `test the task id is not the job's execution lock id`() {
        // "<jobId>-lock" is the monitor's execution lock, and the delete path deletes that document. A cleanup lock
        // sharing the key would contend with a run in flight and then be deleted out from under the cleanup.
        val jobId = "monitor-1"
        val monitorTaskId = AlertCleanupTask.taskId(jobId, CleanupScope.MONITOR)
        val workflowTaskId = AlertCleanupTask.taskId(jobId, CleanupScope.WORKFLOW)

        assertNotEquals(jobId, monitorTaskId)
        assertNotEquals(LockModel.generateLockId(jobId), LockModel.generateLockId(monitorTaskId))
        assertNotEquals("Each scope must drain under its own lock", monitorTaskId, workflowTaskId)
        assertTrue(monitorTaskId.contains(jobId))
    }

    fun `test a monitor scoped query selects only that monitor's alerts`() {
        val task = AlertCleanupTask(
            jobId = "monitor-1",
            scope = CleanupScope.MONITOR,
            alertIndex = "alerts",
            alertHistoryIndex = "history",
            survivingTriggerIds = emptyList(),
            jobDeleted = true
        )

        val query = task.query()

        val monitorFilter = query.filter().single() as TermQueryBuilder
        assertEquals(Alert.MONITOR_ID_FIELD, monitorFilter.fieldName())
        assertEquals("monitor-1", monitorFilter.value())
        assertTrue("Nothing is excluded when no trigger survives", query.mustNot().isEmpty())
    }

    fun `test a workflow scoped query selects chained alerts only`() {
        val task = AlertCleanupTask(
            jobId = "workflow-1",
            scope = CleanupScope.WORKFLOW,
            alertIndex = "alerts",
            alertHistoryIndex = "history",
            survivingTriggerIds = emptyList(),
            jobDeleted = true
        )

        val clauses = task.query().must().map { it as TermQueryBuilder }.associate { it.fieldName() to it.value() }

        assertEquals("workflow-1", clauses[Alert.WORKFLOW_ID_FIELD])
        // A delegate monitor's own alerts also carry the workflow id. They must be left alone because the delegate is
        // still live, and an empty monitor id is what tells the two apart.
        assertEquals("", clauses[Alert.MONITOR_ID_FIELD])
    }

    fun `test surviving triggers are excluded from the drain`() {
        val task = AlertCleanupTask(
            jobId = "monitor-1",
            scope = CleanupScope.MONITOR,
            alertIndex = "alerts",
            alertHistoryIndex = "history",
            survivingTriggerIds = listOf("kept-trigger"),
            jobDeleted = false
        )

        val excluded = task.query().mustNot().single() as TermsQueryBuilder

        assertEquals(Alert.TRIGGER_ID_FIELD, excluded.fieldName())
        assertEquals(listOf("kept-trigger"), excluded.values())
    }

    private fun parse(task: AlertCleanupTask): AlertCleanupTask {
        val builder = task.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
        val xcp = XContentType.JSON.xContent().createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            BytesReference.bytes(builder).utf8ToString()
        )
        xcp.nextToken()
        return AlertCleanupTask.parse(xcp, task.seqNo, task.primaryTerm)
    }
}
