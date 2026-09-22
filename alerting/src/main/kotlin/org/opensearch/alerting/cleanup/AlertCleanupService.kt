/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.cleanup

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALERT_HISTORY_WRITE_INDEX
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALERT_INDEX
import org.opensearch.alerting.alerts.AlertMover
import org.opensearch.alerting.cleanup.action.AlertCleanupAction
import org.opensearch.alerting.cleanup.action.AlertCleanupRequest
import org.opensearch.alerting.cleanup.action.AlertCleanupResponse
import org.opensearch.alerting.core.lock.LockModel
import org.opensearch.alerting.core.lock.LockService
import org.opensearch.alerting.opensearchapi.retry
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.ScheduledJobUtils
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.CompositeInput
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.engine.VersionConflictEngineException
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import kotlin.coroutines.CoroutineContext

private val log = LogManager.getLogger(AlertCleanupService::class.java)

/**
 * Moves the alerts of a deleted job -- or of a trigger removed from a surviving job -- into the alert history index.
 *
 * The unit of work is a durable [AlertCleanupTask] document rather than an in-flight coroutine, which is what makes the
 * cleanup survive the loss of the node performing it:
 *
 * 1. **Record.** The task is written by the delete/update path *before* the job document is removed, because that is
 *    the only point at which the monitor's configured alert indices are still readable. Recording before the point of
 *    no return also means a crash can leave a task with no delete, which [isTaskStillApplicable] discards -- the
 *    reverse ordering would leave a delete with no task, which nothing could ever discover.
 * 2. **Announce.** Once the delete has committed, the cluster is notified and every data node races for the task's
 *    lock. The winner drains; the rest drop the notification.
 * 3. **Drain in bounded work units.** The lock is handed to any claimant once it is [LockService.LOCK_EXPIRED_MINUTES]
 *    old and its holder cannot refresh it, so the holder moves at most [AlertMover.MAX_PAGES_PER_WORK_UNIT] pages,
 *    persists the cursor, releases the lock, and re-acquires for the next unit.
 * 4. **Resume.** [resumeAbandonedTasks] runs periodically and offers every outstanding task to the cluster again, so a
 *    task whose holder died -- or whose announcement was never delivered -- is picked up and finished from its cursor.
 *
 * Nothing here inspects the alerts index to decide which alerts *look* orphaned. The task names the job exactly, and a
 * wrong positive would move a live monitor's alerts into history.
 */
object AlertCleanupService : CoroutineScope {

    private val supervisorJob = SupervisorJob()
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + supervisorJob

    const val CLEANUP_TASK_INDEX = ".opensearch-alerting-cleanup-tasks"

    private lateinit var client: Client
    private lateinit var clusterService: ClusterService
    private lateinit var lockService: LockService
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var threadPool: ThreadPool

    @Volatile
    private var resumePass: Scheduler.Cancellable? = null

    @Volatile
    var resumeInterval: TimeValue = AlertingSettings.ALERT_CLEANUP_RESUME_INTERVAL.getDefault(Settings.EMPTY)
        private set

    @Volatile
    var multiTenancyEnabled: Boolean = false

    /**
     * Applied to a work unit that moved nothing, so a transient rejection is retried while the lock is still held
     * rather than waiting for the next resume pass. Driven by `plugins.alerting.move_alerts_backoff_*`.
     */
    @Volatile
    var retryPolicy: BackoffPolicy = BackoffPolicy.exponentialBackoff()

    /**
     * Bulk rejections surface as 429s on the individual bulk items rather than as a retriable 5xx on the request, so
     * they must be opted into explicitly for [retryPolicy] to have any effect on alert cleanup.
     */
    private val RETRY_ON = listOf(RestStatus.TOO_MANY_REQUESTS)

    fun initialize(
        client: Client,
        clusterService: ClusterService,
        lockService: LockService,
        xContentRegistry: NamedXContentRegistry,
    ): AlertCleanupService {
        this.client = client
        this.clusterService = clusterService
        this.lockService = lockService
        this.xContentRegistry = xContentRegistry
        return this
    }

    fun cleanupTaskMapping(): String =
        AlertCleanupService::class.java.getResource("alert_cleanup_task_mapping.json").readText()

    // ---------------------------------------------------------------------------------------------------------------
    // Record
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Records the cleanup owed for [monitor], resolving the alert indices from the monitor itself.
     *
     * [survivingTriggerIds] is empty when the monitor is being deleted, and the ids of the triggers that remain when a
     * trigger is being removed from a monitor that survives.
     */
    suspend fun recordMonitorCleanupTask(
        monitor: Monitor,
        survivingTriggerIds: List<String>,
        jobDeleted: Boolean,
    ): AlertCleanupTask? {
        if (multiTenancyEnabled) return null
        return recordTask(
            AlertCleanupTask(
                jobId = monitor.id,
                scope = CleanupScope.MONITOR,
                alertIndex = monitor.dataSources.alertsIndex,
                alertHistoryIndex = monitor.dataSources.alertsHistoryIndex ?: ALERT_HISTORY_WRITE_INDEX,
                survivingTriggerIds = survivingTriggerIds,
                jobDeleted = jobDeleted
            )
        )
    }

    /**
     * Records the cleanup owed for [workflow]'s chained alerts.
     *
     * A workflow has no alert indices of its own, so they are taken from the first delegate monitor that still exists.
     * Resolving this at record time rather than at drain time is deliberate: on the delete path the delegates may
     * themselves be gone by the time the drain runs.
     */
    suspend fun recordWorkflowCleanupTask(
        workflow: Workflow,
        survivingTriggerIds: List<String>,
        jobDeleted: Boolean,
    ): AlertCleanupTask? {
        if (multiTenancyEnabled) return null
        val (alertIndex, alertHistoryIndex) = resolveWorkflowAlertIndices(workflow)
        return recordTask(
            AlertCleanupTask(
                jobId = workflow.id,
                scope = CleanupScope.WORKFLOW,
                alertIndex = alertIndex,
                alertHistoryIndex = alertHistoryIndex,
                survivingTriggerIds = survivingTriggerIds,
                jobDeleted = jobDeleted
            )
        )
    }

    /**
     * Records tasks for a job whose deletion was observed without the job object -- a document removed straight from
     * the config index rather than through the delete API.
     *
     * The scope of such a delete is unknowable from an id, so one task is recorded per scope; the scope that matches
     * nothing finishes on its first work unit. The alert indices can only be the defaults, so a monitor configured with
     * custom alert indices is not cleaned up by this path. Existing tasks are left alone, since a task recorded by the
     * delete API carries better information than this one can.
     */
    suspend fun recordFallbackCleanupTasks(jobId: String): List<AlertCleanupTask> {
        if (multiTenancyEnabled) return emptyList()
        return CleanupScope.entries.mapNotNull { scope ->
            if (findTask(AlertCleanupTask.taskId(jobId, scope)) != null) {
                null
            } else {
                recordTask(
                    AlertCleanupTask(
                        jobId = jobId,
                        scope = scope,
                        alertIndex = ALERT_INDEX,
                        alertHistoryIndex = ALERT_HISTORY_WRITE_INDEX,
                        survivingTriggerIds = emptyList(),
                        jobDeleted = true
                    )
                )
            }
        }
    }

    private suspend fun recordTask(task: AlertCleanupTask): AlertCleanupTask? {
        return try {
            // Most saves and deletes have no alerts to move. Counting first keeps the task index, and the lock index
            // behind it, free of documents that would be created only to be drained empty and removed again.
            val outstanding = countOutstanding(task)
            if (outstanding == 0L) return null
            log.info("$outstanding alert(s) to move for [${task.taskId}]; recording a cleanup task.")
            createTaskIndexIfAbsent()
            val request = IndexRequest(CLEANUP_TASK_INDEX)
                .id(task.taskId)
                .source(task.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            val response: IndexResponse = client.suspendUntil { index(request, it) }
            task.copy(seqNo = response.seqNo, primaryTerm = response.primaryTerm)
        } catch (e: Exception) {
            // The job delete has not happened yet, so failing here leaves the alerts attached to a live job rather
            // than orphaning them. Surface it and let the delete proceed.
            log.error("Failed to record alert cleanup task [${task.taskId}].", e)
            null
        }
    }

    /**
     * How many alerts [task] would move.
     *
     * `size(0)` with `trackTotalHits(true)`, so the answer is not capped by the default search size of 10 -- the same
     * cap that silently truncated the original cleanup.
     */
    private suspend fun countOutstanding(task: AlertCleanupTask): Long {
        return try {
            val request = SearchRequest(task.alertIndex)
                .routing(task.jobId)
                .source(SearchSourceBuilder.searchSource().query(task.query()).size(0).trackTotalHits(true))
            val response: SearchResponse = client.suspendUntil { search(request, it) }
            response.hits.totalHits?.value ?: 0L
        } catch (e: Exception) {
            if (e is IndexNotFoundException || e.cause is IndexNotFoundException) return 0L
            // Unknown rather than zero. Recording the task is the safe error: a drain that finds nothing removes it.
            log.warn("Failed to count alerts outstanding for [${task.taskId}]; recording the task anyway.", e)
            -1L
        }
    }

    private suspend fun resolveWorkflowAlertIndices(workflow: Workflow): Pair<String, String> {
        var alertIndex = ALERT_INDEX
        var alertHistoryIndex = ALERT_HISTORY_WRITE_INDEX
        val input = workflow.inputs.firstOrNull()
        if (input !is CompositeInput) return alertIndex to alertHistoryIndex

        try {
            for (delegate in input.sequence.delegates) {
                val getResponse: GetResponse = client.suspendUntil {
                    get(GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, delegate.monitorId), it)
                }
                if (getResponse.isExists) {
                    val monitor = ScheduledJobUtils.parseMonitorFromScheduledJobDocSource(xContentRegistry, getResponse)
                    alertIndex = monitor.dataSources.alertsIndex
                    monitor.dataSources.alertsHistoryIndex?.let { alertHistoryIndex = it }
                    break
                }
            }
        } catch (e: Exception) {
            log.error("Failed to get a delegate monitor for workflow ${workflow.id}. Assuming default alert indices.", e)
        }
        return alertIndex to alertHistoryIndex
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Announce and execute
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Offers the tasks of [jobId] to every data node.
     *
     * Only data nodes are addressed. Any node could hold the lock and drain correctly, but a paginated search plus two
     * bulk operations per page is data-plane work, and a dedicated cluster manager node's stability is what the
     * cluster's ability to apply state depends on.
     */
    fun nodeIdsForCleanup(): Array<String> =
        clusterService.state().nodes.dataNodes.keys.toTypedArray()

    /**
     * Announces [jobId]'s outstanding cleanup to the cluster.
     *
     * Fire-and-forget. A dropped announcement costs a delay, not the cleanup: the task document remains and
     * [resumeAbandonedTasks] offers it again.
     */
    fun announce(jobId: String) {
        if (multiTenancyEnabled) return
        val nodeIds = nodeIdsForCleanup()
        if (nodeIds.isEmpty()) {
            log.warn("No data nodes available to clean up alerts for job $jobId; leaving it to the periodic resume.")
            return
        }
        client.execute(
            AlertCleanupAction.INSTANCE,
            AlertCleanupRequest(jobId, nodeIds),
            object : ActionListener<AlertCleanupResponse> {
                override fun onResponse(response: AlertCleanupResponse) {
                    log.debug(
                        "Announced alert cleanup for job {} to {} node(s), {} failure(s).",
                        jobId, response.nodes.size, response.failures().size
                    )
                }

                override fun onFailure(e: Exception) {
                    log.error("Failed to announce alert cleanup for job $jobId; the periodic resume will retry.", e)
                }
            }
        )
    }

    /**
     * Runs whatever cleanup is outstanding for [jobId] on this node, if this node wins the lock for it.
     *
     * A task with no work left, or one whose job turns out to still exist, is removed rather than drained.
     */
    suspend fun runTasksFor(jobId: String) {
        CleanupScope.entries.forEach { scope -> runTask(AlertCleanupTask.taskId(jobId, scope)) }
    }

    /**
     * Drains [taskId] to completion, one bounded work unit per lock acquisition.
     *
     * Returns without doing anything if another node holds the lock, which is the normal outcome for all but one
     * recipient of an announcement.
     */
    suspend fun runTask(taskId: String) {
        // Most ids a delete notification carries owe nothing: a monitor's metadata document is deleted from the same
        // config index, a monitor delete is announced for both scopes, and a job whose alerts were all already closed
        // never had a task recorded. Checking before the lock keeps all of those out of the lock index.
        if (findTask(taskId) == null) return
        var unitsRun = 0
        while (true) {
            if (!awaitLockIndex()) {
                log.warn("The alert cleanup lock index is not ready for [$taskId]; the periodic resume will retry.")
                return
            }
            val lock: LockModel? = try {
                // Retried rather than abandoned, because the failure here is a 503 from a lock index whose shards are
                // still recovering, and at this call site that is indistinguishable from losing the race. Treating it as
                // a loss would leave a task that no node is draining until the next resume pass came round.
                retryPolicy.retry(log) {
                    client.suspendUntil<Client, LockModel?> { lockService.acquireLockWithId(taskId, it) }
                }
            } catch (e: Exception) {
                // No work is lost: the task document is still there and the next resume pass offers it again.
                log.warn("Could not acquire the alert cleanup lock for [$taskId]; the periodic resume will retry.", e)
                return
            }
            if (lock == null) {
                if (unitsRun > 0) {
                    log.info("Alert cleanup lock for [$taskId] was taken over after $unitsRun work unit(s).")
                }
                return
            }

            val keepGoing = try {
                runWorkUnit(taskId)
            } catch (e: Exception) {
                log.error("Alert cleanup work unit for [$taskId] failed.", e)
                false
            } finally {
                try {
                    client.suspendUntil<Client, Boolean> { lockService.release(lock, it) }
                } catch (e: Exception) {
                    log.error("Failed to release alert cleanup lock for [$taskId].", e)
                }
            }
            unitsRun++
            if (!keepGoing) return
        }
    }

    /**
     * Waits, bounded by [retryPolicy], for the lock index to be able to serve a read.
     *
     * The lock index is created by whichever node first needs a lock, so the other recipients of the same announcement
     * routinely ask for their lock while its shards are still recovering. That read fails with a 503 which, at the call
     * site, is indistinguishable from losing the race -- so without this wait the losers would abandon a task that no
     * one is draining, and the alerts would sit until the next resume pass.
     */
    private suspend fun awaitLockIndex(): Boolean {
        val backoff = retryPolicy.iterator()
        while (!isLockIndexReadable()) {
            if (!backoff.hasNext()) return false
            delay(backoff.next().millis)
        }
        return true
    }

    /** Whether the lock index can serve a read. An index that does not exist yet is created by [LockService] itself. */
    private fun isLockIndexReadable(): Boolean {
        val routing = clusterService.state().routingTable().index(LockService.LOCK_INDEX_NAME) ?: return true
        return routing.allPrimaryShardsActive()
    }

    /**
     * One work unit: drain a bounded number of pages, then either persist the advanced cursor or remove the finished
     * task. Returns true when there is more to do.
     */
    private suspend fun runWorkUnit(taskId: String): Boolean {
        val task = findTask(taskId) ?: return false
        if (!isTaskStillApplicable(task)) {
            deleteTask(taskId)
            return false
        }

        val result = retryPolicy.retry(log, RETRY_ON) {
            val attempt = AlertMover.drainAlerts(client, task)
            // A unit that moved nothing has made no progress, so retrying it here -- while the lock is still held --
            // is worth a backoff. A partial success is left to the next unit, which resumes from the new cursor.
            val failure = attempt.failure
            if (failure != null && attempt.movedCount == 0L) throw failure
            attempt
        }

        if (result.finished) {
            if (result.failure == null) {
                deleteTask(taskId)
                return false
            }
            // Pages remain unmoved. Leave the task in place with its cursor unchanged so that the periodic resume
            // retries the same range rather than skipping it; moving alerts is idempotent, so a retry is safe.
            log.warn("Alert cleanup for [$taskId] reached the end of its range with failures; will retry.", result.failure)
            return false
        }

        val advanced = task.copy(cursor = result.nextCursor!!, movedCount = task.movedCount + result.movedCount)
        if (!persistCursor(advanced)) return false
        // A work unit that failed to move anything is not making progress; hand it to the periodic resume instead of
        // spinning on it while holding the lock.
        return result.failure == null || result.movedCount > 0
    }

    /**
     * Guards against the task having been recorded for a delete that never committed.
     *
     * The task is written before the job document is removed, so a failure in between leaves a task naming a job that
     * is still live. Draining it would move a live monitor's alerts into history, which is worse than the leak this
     * service exists to close. The check is on the recorded job's identity, not on the alerts.
     */
    private suspend fun isTaskStillApplicable(task: AlertCleanupTask): Boolean {
        // A task recorded for a trigger removal belongs to a job that is expected to still exist.
        if (!task.jobDeleted) return true
        return try {
            val response: GetResponse = client.suspendUntil {
                get(GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, task.jobId), it)
            }
            if (response.isExists) {
                log.warn(
                    "Alert cleanup task [${task.taskId}] names a job that still exists; discarding the task. " +
                        "Either the delete did not commit or the id was reused."
                )
                false
            } else {
                true
            }
        } catch (e: IndexNotFoundException) {
            // No config index at all, so no job can exist.
            true
        } catch (e: Exception) {
            log.error("Failed to confirm job ${task.jobId} is deleted; leaving cleanup task in place.", e)
            false
        }
    }

    /** Writes the advanced cursor back, guarded on the version the task was read at. */
    private suspend fun persistCursor(task: AlertCleanupTask): Boolean {
        return try {
            val request = IndexRequest(CLEANUP_TASK_INDEX)
                .id(task.taskId)
                .source(task.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .setIfSeqNo(task.seqNo)
                .setIfPrimaryTerm(task.primaryTerm)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            client.suspendUntil<Client, IndexResponse> { index(request, it) }
            true
        } catch (e: VersionConflictEngineException) {
            // Another node took the task over and has already moved the cursor. Its copy wins; a stale cursor written
            // over a newer one would skip a range and silently leave alerts behind.
            log.info("Alert cleanup task [${task.taskId}] was advanced by another node; yielding.")
            false
        } catch (e: Exception) {
            log.error("Failed to persist the cursor for alert cleanup task [${task.taskId}].", e)
            false
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Resume
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Re-offers every outstanding task to the cluster.
     *
     * This is what notices a task whose holder left the cluster mid-drain, and what covers a task whose announcement
     * was never delivered. It reads the task index only -- it never decides for itself that an alert is orphaned.
     */
    fun scheduleResume(threadPool: ThreadPool, interval: TimeValue): Scheduler.Cancellable {
        this.threadPool = threadPool
        this.resumeInterval = interval
        val scheduled = threadPool.scheduleWithFixedDelay(
            {
                launch {
                    try {
                        resumeAbandonedTasks()
                    } catch (e: Exception) {
                        log.error("The periodic alert cleanup resume pass failed.", e)
                    }
                }
            },
            interval,
            ThreadPool.Names.GENERIC
        )
        resumePass = scheduled
        return scheduled
    }

    /** Applies a new [ALERT_CLEANUP_RESUME_INTERVAL] without a node restart. */
    fun rescheduleResume(interval: TimeValue) {
        if (!this::threadPool.isInitialized) {
            resumeInterval = interval
            return
        }
        resumePass?.cancel()
        scheduleResume(threadPool, interval)
        log.info("The alert cleanup resume pass now runs every $interval.")
    }

    suspend fun resumeAbandonedTasks() {
        if (multiTenancyEnabled) return
        // The drain is data-plane work; a dedicated cluster manager node does not take it on.
        if (!clusterService.localNode().isDataNode) return
        if (!clusterService.state().routingTable().hasIndex(CLEANUP_TASK_INDEX)) return
        val taskIds = try {
            val request = SearchRequest(CLEANUP_TASK_INDEX).source(
                SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()).size(MAX_TASKS_PER_RESUME)
                    .fetchSource(false)
            )
            val response: SearchResponse = client.suspendUntil { search(request, it) }
            response.hits.hits.map { it.id }
        } catch (e: Exception) {
            log.error("Failed to list outstanding alert cleanup tasks.", e)
            return
        }
        if (taskIds.isEmpty()) return
        log.info("Resuming ${taskIds.size} outstanding alert cleanup task(s).")
        taskIds.forEach { runTask(it) }
    }

    /** Bound on one resume pass, so a large backlog is worked through over several passes instead of in one burst. */
    const val MAX_TASKS_PER_RESUME = 100

    // ---------------------------------------------------------------------------------------------------------------
    // Storage
    // ---------------------------------------------------------------------------------------------------------------

    private suspend fun findTask(taskId: String): AlertCleanupTask? {
        return try {
            val response: GetResponse = client.suspendUntil { get(GetRequest(CLEANUP_TASK_INDEX, taskId), it) }
            if (!response.isExists) return null
            val xcp = XContentType.JSON.xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, response.sourceAsString)
            xcp.nextToken()
            AlertCleanupTask.parse(xcp, response.seqNo, response.primaryTerm)
        } catch (e: IndexNotFoundException) {
            null
        } catch (e: Exception) {
            log.error("Failed to read alert cleanup task [$taskId].", e)
            null
        }
    }

    private suspend fun deleteTask(taskId: String) {
        try {
            val request = DeleteRequest(CLEANUP_TASK_INDEX, taskId)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            client.suspendUntil<Client, DeleteResponse> { delete(request, it) }
            log.info("Alert cleanup task [$taskId] is complete and has been removed.")
        } catch (e: Exception) {
            // A task left behind is re-offered by the periodic resume, finds nothing to move, and is removed then.
            log.error("Failed to remove completed alert cleanup task [$taskId].", e)
        }
    }

    private suspend fun createTaskIndexIfAbsent() {
        if (clusterService.state().routingTable().hasIndex(CLEANUP_TASK_INDEX)) return
        try {
            val request = CreateIndexRequest(CLEANUP_TASK_INDEX)
                .mapping(cleanupTaskMapping())
                .settings(Settings.builder().put("index.hidden", true).build())
            client.suspendUntil<Client, CreateIndexResponse> { admin().indices().create(request, it) }
        } catch (e: Exception) {
            if (e is ResourceAlreadyExistsException || e.cause is ResourceAlreadyExistsException) return
            throw e
        }
    }
}
