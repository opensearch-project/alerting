/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.cleanup

import org.opensearch.commons.alerting.model.Alert
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.seqno.SequenceNumbers
import java.time.Instant

/**
 * Which family of alerts a task drains.
 *
 * A delete notification carries only a job id, and a monitor id and a workflow id are drawn from the same space, so
 * both scopes are recorded for every delete and each drains independently. A scope whose query matches nothing
 * finishes on its first work unit.
 */
enum class CleanupScope { MONITOR, WORKFLOW }

/**
 * A durable record of alerts that must be moved to the history index because the job (or trigger) that owned them no
 * longer exists.
 *
 * The task is what makes cleanup recoverable. It is written by the shard copy that acted as primary for the
 * delete/update, before any alert is touched, and removed only once the drain has finished. Any node that finds a task
 * whose lock has expired resumes it from [cursor] -- nothing has to inspect the alerts index and guess which alerts
 * look abandoned, because the task names the job exactly.
 */
data class AlertCleanupTask(
    val jobId: String,
    val scope: CleanupScope,
    val alertIndex: String,
    val alertHistoryIndex: String,
    /**
     * Triggers that still exist on the job. Alerts belonging to any *other* trigger are drained. Empty means the job
     * itself is gone (or has no triggers left), so every alert of the job is drained.
     */
    val survivingTriggerIds: List<String>,
    /**
     * Whether the job itself was deleted, as opposed to a trigger being removed from a job that survives.
     *
     * Recorded explicitly rather than inferred from an empty [survivingTriggerIds], which is also what removing a
     * monitor's last remaining trigger produces. It selects whether the drain confirms the job is gone before touching
     * anything -- see AlertCleanupService.isTaskStillApplicable.
     */
    val jobDeleted: Boolean,
    /** `_seq_no` of the last alert of the last completed page. `-1` means nothing has been drained yet. */
    val cursor: Long = NO_CURSOR,
    val movedCount: Long = 0L,
    val createdAt: Instant = Instant.now(),
    /**
     * Version of the document this task was read from, used to guard the cursor write with
     * `setIfSeqNo`/`setIfPrimaryTerm`.
     *
     * Moving alerts is idempotent, but advancing the cursor is not: a holder that has lost the lock without noticing
     * must not be able to overwrite a takeover's cursor, because a cursor that jumps forward past a range that was
     * never drained reproduces the leak this whole mechanism exists to close. Not persisted.
     */
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
) : ToXContentObject {

    /** Document id of this task, and the id the cleanup lock is keyed on. */
    val taskId: String get() = taskId(jobId, scope)

    /**
     * The alerts this task is responsible for.
     *
     * For [CleanupScope.WORKFLOW] only chained alerts belong to the workflow itself: a delegate monitor's own alerts
     * also carry the workflow id (see AlertService.composeMonitorErrorAlert/composeQueryLevelAlert) and must be left
     * alone because the delegate monitor is still live. The `monitor_id == ""` filter is the same discriminator
     * AlertService.searchAlerts(workflow, ...) uses.
     */
    fun query(): BoolQueryBuilder {
        val query = when (scope) {
            CleanupScope.MONITOR ->
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery(Alert.MONITOR_ID_FIELD, jobId))
            CleanupScope.WORKFLOW ->
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery(Alert.WORKFLOW_ID_FIELD, jobId))
                    .must(QueryBuilders.termQuery(Alert.MONITOR_ID_FIELD, ""))
        }
        if (survivingTriggerIds.isNotEmpty()) {
            query.mustNot(QueryBuilders.termsQuery(Alert.TRIGGER_ID_FIELD, survivingTriggerIds))
        }
        return query
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(JOB_ID_FIELD, jobId)
            .field(SCOPE_FIELD, scope.name)
            .field(ALERT_INDEX_FIELD, alertIndex)
            .field(ALERT_HISTORY_INDEX_FIELD, alertHistoryIndex)
            .field(SURVIVING_TRIGGER_IDS_FIELD, survivingTriggerIds.toTypedArray())
            .field(JOB_DELETED_FIELD, jobDeleted)
            .field(CURSOR_FIELD, cursor)
            .field(MOVED_COUNT_FIELD, movedCount)
            .field(CREATED_AT_FIELD, createdAt.toEpochMilli())
            .endObject()
    }

    companion object {
        const val JOB_ID_FIELD = "job_id"
        const val SCOPE_FIELD = "scope"
        const val ALERT_INDEX_FIELD = "alert_index"
        const val ALERT_HISTORY_INDEX_FIELD = "alert_history_index"
        const val SURVIVING_TRIGGER_IDS_FIELD = "surviving_trigger_ids"
        const val JOB_DELETED_FIELD = "job_deleted"
        const val CURSOR_FIELD = "cursor"
        const val MOVED_COUNT_FIELD = "moved_count"
        const val CREATED_AT_FIELD = "created_at"

        /** Cursor value meaning "no page has completed yet". `_seq_no` starts at 0, so -1 sorts before every alert. */
        const val NO_CURSOR = -1L

        /**
         * The task id, which is also what the cleanup lock is keyed on.
         *
         * Deliberately *not* the job id. `"<jobId>-lock"` is the monitor's execution lock, and the delete path deletes
         * that document as one of its steps (DeleteMonitorService.deleteLock) -- a cleanup lock sharing that key would
         * contend with a run in flight and then be deleted out from under the cleanup.
         */
        fun taskId(jobId: String, scope: CleanupScope): String = "alert-cleanup-${scope.name.lowercase()}-$jobId"

        fun parse(
            xcp: XContentParser,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
        ): AlertCleanupTask {
            lateinit var jobId: String
            lateinit var scope: CleanupScope
            lateinit var alertIndex: String
            lateinit var alertHistoryIndex: String
            val survivingTriggerIds = mutableListOf<String>()
            var jobDeleted = false
            var cursor = NO_CURSOR
            var movedCount = 0L
            var createdAt = Instant.now()

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    JOB_ID_FIELD -> jobId = xcp.text()
                    SCOPE_FIELD -> scope = CleanupScope.valueOf(xcp.text())
                    ALERT_INDEX_FIELD -> alertIndex = xcp.text()
                    ALERT_HISTORY_INDEX_FIELD -> alertHistoryIndex = xcp.text()
                    SURVIVING_TRIGGER_IDS_FIELD -> {
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            survivingTriggerIds.add(xcp.text())
                        }
                    }
                    JOB_DELETED_FIELD -> jobDeleted = xcp.booleanValue()
                    CURSOR_FIELD -> cursor = xcp.longValue()
                    MOVED_COUNT_FIELD -> movedCount = xcp.longValue()
                    CREATED_AT_FIELD -> createdAt = Instant.ofEpochMilli(xcp.longValue())
                }
            }

            return AlertCleanupTask(
                jobId = jobId,
                scope = scope,
                alertIndex = alertIndex,
                alertHistoryIndex = alertHistoryIndex,
                survivingTriggerIds = survivingTriggerIds,
                jobDeleted = jobDeleted,
                cursor = cursor,
                movedCount = movedCount,
                createdAt = createdAt,
                seqNo = seqNo,
                primaryTerm = primaryTerm
            )
        }
    }
}
