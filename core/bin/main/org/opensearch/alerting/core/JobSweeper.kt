/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core

import org.apache.logging.log4j.LogManager
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.search.SearchRequest
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.core.schedule.JobScheduler
import org.opensearch.alerting.core.settings.ScheduledJobSettings.Companion.REQUEST_TIMEOUT
import org.opensearch.alerting.core.settings.ScheduledJobSettings.Companion.SWEEPER_ENABLED
import org.opensearch.alerting.core.settings.ScheduledJobSettings.Companion.SWEEP_BACKOFF_MILLIS
import org.opensearch.alerting.core.settings.ScheduledJobSettings.Companion.SWEEP_BACKOFF_RETRY_COUNT
import org.opensearch.alerting.core.settings.ScheduledJobSettings.Companion.SWEEP_PAGE_SIZE
import org.opensearch.alerting.core.settings.ScheduledJobSettings.Companion.SWEEP_PERIOD
import org.opensearch.alerting.elasticapi.firstFailureOrNull
import org.opensearch.alerting.elasticapi.retry
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.routing.IndexShardRoutingTable
import org.opensearch.cluster.routing.Murmur3HashFunction
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.Strings
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.component.LifecycleListener
import org.opensearch.common.logging.Loggers
import org.opensearch.common.lucene.uid.Versions
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.util.concurrent.OpenSearchExecutors
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.engine.Engine
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.shard.IndexingOperationListener
import org.opensearch.index.shard.ShardId
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.FieldSortBuilder
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import java.util.TreeMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

typealias JobId = String
typealias JobVersion = Long

/**
 * 'Sweeping' is the process of listening for new and updated [ScheduledJob]s and deciding if they should be scheduled for
 * execution on this node. The [JobSweeper] runs on every node, sweeping all local active shards that are present on the node.
 *
 * A [consistent hash][ShardNodes] is used to distribute jobs across all nodes that contain an active instance of the same shard.
 * This minimizes any interruptions in job execution when the cluster configuration changes.
 *
 * There are two types of sweeps:
 * - *Full sweeps* occur when the [routing table][IndexShardRoutingTable] for the shard changes (for e.g. a replica has been
 *  added or removed).  The full sweep re-reads all jobs in the shard, deciding which ones to run locally. All full sweeps
 *  happen asynchronously in the background in a serial manner. See the [sweepAllShards] method.
 * - *Single job sweeps* occur when a new version of the job is indexed or deleted. An [IndexingOperationListener] listens
 * for index changes and synchronously schedules or removes the job from the scheduler.
 */
class JobSweeper(
    private val settings: Settings,
    private val client: Client,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool,
    private val xContentRegistry: NamedXContentRegistry,
    private val scheduler: JobScheduler,
    private val sweepableJobTypes: List<String>
) : ClusterStateListener, IndexingOperationListener, LifecycleListener() {
    private val logger = LogManager.getLogger(javaClass)

    private val fullSweepExecutor = Executors.newSingleThreadExecutor(OpenSearchExecutors.daemonThreadFactory("opendistro_job_sweeper"))

    private val sweptJobs = ConcurrentHashMap<ShardId, ConcurrentHashMap<JobId, JobVersion>>()

    private var scheduledFullSweep: Scheduler.Cancellable? = null

    @Volatile private var lastFullSweepTimeNano = System.nanoTime()

    @Volatile private var requestTimeout = REQUEST_TIMEOUT.get(settings)
    @Volatile private var sweepPeriod = SWEEP_PERIOD.get(settings)
    @Volatile private var sweeperEnabled = SWEEPER_ENABLED.get(settings)
    @Volatile private var sweepPageSize = SWEEP_PAGE_SIZE.get(settings)
    @Volatile private var sweepBackoffMillis = SWEEP_BACKOFF_MILLIS.get(settings)
    @Volatile private var sweepBackoffRetryCount = SWEEP_BACKOFF_RETRY_COUNT.get(settings)
    @Volatile private var sweepSearchBackoff = BackoffPolicy.exponentialBackoff(sweepBackoffMillis, sweepBackoffRetryCount)

    init {
        clusterService.addListener(this)
        clusterService.addLifecycleListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(SWEEP_PERIOD) {
            // if sweep period change, restart background sweep with new sweep period
            logger.debug("Reinitializing background full sweep with period: ${sweepPeriod.minutes()}")
            sweepPeriod = it
            initBackgroundSweep()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(SWEEPER_ENABLED) {
            sweeperEnabled = it
            if (!sweeperEnabled) disable() else enable()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(SWEEP_BACKOFF_MILLIS) {
            sweepBackoffMillis = it
            sweepSearchBackoff = BackoffPolicy.exponentialBackoff(sweepBackoffMillis, sweepBackoffRetryCount)
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(SWEEP_BACKOFF_RETRY_COUNT) {
            sweepBackoffRetryCount = it
            sweepSearchBackoff = BackoffPolicy.exponentialBackoff(sweepBackoffMillis, sweepBackoffRetryCount)
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(SWEEP_PAGE_SIZE) { sweepPageSize = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(REQUEST_TIMEOUT) { requestTimeout = it }
    }

    override fun afterStart() {
        initBackgroundSweep()
    }

    override fun beforeStop() {
        scheduledFullSweep?.cancel()
    }

    override fun beforeClose() {
        fullSweepExecutor.shutdown()
    }

    /**
     * Initiates a full sweep of all local shards when the index routing table is changed (for e.g. when the node joins
     * the cluster, a replica is added, removed or promoted to primary).
     *
     * This callback won't be invoked concurrently since cluster state changes are applied serially to the node
     * in the order they occur on the master. However we can't block this callback for the duration of a full sweep so
     * we perform the sweep in the background in a single threaded executor [fullSweepExecutor].
     */
    override fun clusterChanged(event: ClusterChangedEvent) {
        if (!isSweepingEnabled()) return

        if (!event.indexRoutingTableChanged(ScheduledJob.SCHEDULED_JOBS_INDEX)) return

        logger.debug("Scheduled Jobs routing table changed. Running full sweep...")
        fullSweepExecutor.submit {
            sweepAllShards()
        }
    }

    /**
     * This callback is invoked when a new job (or new version of a job) is indexed. If the job is assigned to the node
     * it is scheduled. Relies on all indexing operations using optimistic concurrency control to ensure that stale versions
     * of jobs are not scheduled. It schedules job only if it is one of the [sweepableJobTypes]
     *
     */
    override fun postIndex(shardId: ShardId, index: Engine.Index, result: Engine.IndexResult) {
        if (!isSweepingEnabled()) return

        if (result.resultType != Engine.Result.Type.SUCCESS) {
            val shardJobs = sweptJobs[shardId] ?: emptyMap<JobId, JobVersion>()
            val currentVersion = shardJobs[index.id()] ?: Versions.NOT_FOUND
            logger.debug("Indexing failed for ScheduledJob: ${index.id()}. Continuing with current version $currentVersion")
            return
        }

        if (isOwningNode(shardId, index.id())) {
            val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, index.source(), XContentType.JSON)
            if (isSweepableJobType(xcp)) {
                val job = parseAndSweepJob(xcp, shardId, index.id(), result.version, index.source(), true)
                if (job != null) scheduler.postIndex(job)
            } else {
                logger.debug("Not a valid job type in document ${index.id()} to sweep.")
            }
        }
    }

    /**
     * This callback is invoked when a job is deleted from a shard. The job is descheduled. Relies on all delete operations
     * using optimistic concurrency control to ensure that stale versions of jobs are not scheduled.
     */
    override fun postDelete(shardId: ShardId, delete: Engine.Delete, result: Engine.DeleteResult) {
        if (!isSweepingEnabled()) return

        if (result.resultType != Engine.Result.Type.SUCCESS) {
            val shardJobs = sweptJobs[shardId] ?: emptyMap<JobId, JobVersion>()
            val currentVersion = shardJobs[delete.id()] ?: Versions.NOT_FOUND
            logger.debug("Deletion failed for ScheduledJob: ${delete.id()}. Continuing with current version $currentVersion")
            return
        }

        if (isOwningNode(shardId, delete.id())) {
            if (scheduler.scheduledJobs().contains(delete.id())) {
                sweep(shardId, delete.id(), result.version, null)
            }
            scheduler.postDelete(delete.id())
        }
    }

    fun enable() {
        // initialize background sweep
        initBackgroundSweep()
        // set sweeperEnabled flag to true to make the listeners aware of this setting
        sweeperEnabled = true
    }

    fun disable() {
        // cancel background sweep
        scheduledFullSweep?.cancel()
        // deschedule existing jobs on this node
        logger.info("Descheduling all jobs as sweeping is disabled")
        scheduler.deschedule(scheduler.scheduledJobs())
        // set sweeperEnabled flag to false to make the listeners aware of this setting
        sweeperEnabled = false
    }

    public fun isSweepingEnabled(): Boolean {
        // Although it is a single link check, keeping it as a separate function, so we
        // can abstract out logic of finding out whether to proceed or not
        return sweeperEnabled == true
    }

    private fun initBackgroundSweep() {

        // if sweeping disabled, background sweep should not be triggered
        if (!isSweepingEnabled()) return

        // cancel existing background thread if present
        scheduledFullSweep?.cancel()

        // Manually sweep all shards before scheduling the background sweep so it picks up any changes immediately
        // since the first run of a task submitted with scheduleWithFixedDelay() happens after the interval has passed.
        logger.debug("Performing sweep of scheduled jobs.")
        fullSweepExecutor.submit {
            sweepAllShards()
        }

        // Setup an anti-entropy/self-healing background sweep, in case a sweep that was triggered by an event fails.
        val scheduledSweep = Runnable {
            val elapsedTime = getFullSweepElapsedTime()

            // Rate limit to at most one full sweep per sweep period
            // The schedule runs may wake up a few milliseconds early.
            // Delta will be giving some buffer on the schedule to allow waking up slightly earlier.
            val delta = sweepPeriod.millis - elapsedTime.millis
            if (delta < 20L) { // give 20ms buffer.
                fullSweepExecutor.submit {
                    logger.debug("Performing background sweep of scheduled jobs.")
                    sweepAllShards()
                }
            }
        }
        scheduledFullSweep = threadPool.scheduleWithFixedDelay(scheduledSweep, sweepPeriod, ThreadPool.Names.SAME)
    }

    private fun sweepAllShards() {
        val clusterState = clusterService.state()
        if (!clusterState.routingTable.hasIndex(ScheduledJob.SCHEDULED_JOBS_INDEX)) {
            scheduler.deschedule(scheduler.scheduledJobs())
            sweptJobs.clear()
            lastFullSweepTimeNano = System.nanoTime()
            return
        }

        // Find all shards that are currently assigned to this node.
        val localNodeId = clusterState.nodes.localNodeId
        val localShards = clusterState.routingTable.allShards(ScheduledJob.SCHEDULED_JOBS_INDEX)
            // Find all active shards
            .filter { it.active() }
            // group by shardId
            .groupBy { it.shardId() }
            // assigned to local node
            .filter { (_, shards) -> shards.any { it.currentNodeId() == localNodeId } }

        // Remove all jobs on shards that are no longer assigned to this node.
        val removedShards = sweptJobs.keys - localShards.keys
        removedShards.forEach { shardId ->
            val shardJobs = sweptJobs.remove(shardId) ?: emptyMap<JobId, JobVersion>()
            scheduler.deschedule(shardJobs.keys)
        }

        // resweep all shards that are assigned to this node.
        localShards.forEach { (shardId, shards) ->
            try {
                sweepShard(shardId, ShardNodes(localNodeId, shards.map { it.currentNodeId() }))
            } catch (e: Exception) {
                val shardLogger = Loggers.getLogger(javaClass, shardId)
                shardLogger.error("Error while sweeping shard $shardId", e)
            }
        }
        lastFullSweepTimeNano = System.nanoTime()
    }

    private fun sweepShard(shardId: ShardId, shardNodes: ShardNodes, startAfter: String = "") {
        val logger = Loggers.getLogger(javaClass, shardId)
        logger.debug("Sweeping shard $shardId")

        // Remove any jobs that are currently scheduled that are no longer owned by this node
        val currentJobs = sweptJobs.getOrPut(shardId) { ConcurrentHashMap() }
        currentJobs.keys.filterNot { shardNodes.isOwningNode(it) }.forEach {
            scheduler.deschedule(it)
            currentJobs.remove(it)
        }

        // sweep the shard for new and updated jobs. Uses a search after query to paginate, assuming that any concurrent
        // updates and deletes are handled by the index operation listener.
        var searchAfter: String? = startAfter
        while (searchAfter != null) {
            val boolQueryBuilder = BoolQueryBuilder()
            sweepableJobTypes.forEach { boolQueryBuilder.should(QueryBuilders.existsQuery(it)) }
            val jobSearchRequest = SearchRequest()
                .indices(ScheduledJob.SCHEDULED_JOBS_INDEX)
                .preference("_shards:${shardId.id}|_only_local")
                .source(
                    SearchSourceBuilder.searchSource()
                        .version(true)
                        .sort(
                            FieldSortBuilder("_id")
                                .unmappedType("keyword")
                                .missing("_last")
                        )
                        .searchAfter(arrayOf(searchAfter))
                        .size(sweepPageSize)
                        .query(boolQueryBuilder)
                )

            val response = sweepSearchBackoff.retry {
                client.search(jobSearchRequest).actionGet(requestTimeout)
            }
            if (response.status() != RestStatus.OK) {
                logger.error("Error sweeping shard $shardId.", response.firstFailureOrNull())
                return
            }
            for (hit in response.hits) {
                if (shardNodes.isOwningNode(hit.id)) {
                    val xcp = XContentHelper.createParser(
                        xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                        hit.sourceRef, XContentType.JSON
                    )
                    parseAndSweepJob(xcp, shardId, hit.id, hit.version, hit.sourceRef)
                }
            }
            searchAfter = response.hits.lastOrNull()?.id
        }
    }

    private fun sweep(
        shardId: ShardId,
        jobId: JobId,
        newVersion: JobVersion,
        job: ScheduledJob?,
        failedToParse: Boolean = false
    ) {
        sweptJobs.getOrPut(shardId) { ConcurrentHashMap() }
            // Use [compute] to update atomically in case another thread concurrently indexes/deletes the same job
            .compute(jobId) { _, currentVersion ->
                val jobCurrentlyScheduled = scheduler.scheduledJobs().contains(jobId)

                if (newVersion <= (currentVersion ?: Versions.NOT_FOUND)) {
                    if (unchangedJobToBeRescheduled(newVersion, currentVersion, jobCurrentlyScheduled, job)) {
                        logger.debug("Not skipping job $jobId since it is an unchanged job slated to be rescheduled")
                    } else {
                        logger.debug("Skipping job $jobId, $newVersion <= $currentVersion")
                        return@compute currentVersion
                    }
                }

                // deschedule the currently scheduled version
                if (jobCurrentlyScheduled) {
                    scheduler.deschedule(jobId)
                }

                if (failedToParse) {
                    return@compute currentVersion
                }
                if (job != null) {
                    if (job.enabled) {
                        scheduler.schedule(job)
                    }
                    return@compute newVersion
                } else {
                    return@compute null
                }
            }
    }

    /*
     * During the job sweep, normally jobs where the currentVersion is equal to the newVersion are skipped since
     * there was no change.
     *
     * However, there exists an edge-case where a job could have been de-scheduled by flipping [SWEEPER_ENABLED]
     * to false and then not have undergone any changes when the sweeper is re-enabled. In this case, the job should
     * not be skipped so it can be re-scheduled. This utility method checks for this condition so the sweep() method
     * can account for it.
     */
    private fun unchangedJobToBeRescheduled(
        newVersion: JobVersion,
        currentVersion: JobVersion?,
        jobCurrentlyScheduled: Boolean,
        job: ScheduledJob?
    ): Boolean {
        // newVersion should not be [Versions.NOT_FOUND] here since it's passed in from existing search hits
        // or successful doc delete operations
        val versionWasUnchanged = newVersion == (currentVersion ?: Versions.NOT_FOUND)
        val jobEnabled = job?.enabled ?: false

        return versionWasUnchanged && !jobCurrentlyScheduled && jobEnabled
    }

    private fun parseAndSweepJob(
        xcp: XContentParser,
        shardId: ShardId,
        jobId: JobId,
        jobVersion: JobVersion,
        jobSource: BytesReference,
        typeIsParsed: Boolean = false
    ): ScheduledJob? {
        return try {
            val job = parseScheduledJob(xcp, jobId, jobVersion, typeIsParsed)
            sweep(shardId, jobId, jobVersion, job)
            job
        } catch (e: Exception) {
            logger.warn(
                "Unable to parse ScheduledJob source: {}",
                Strings.cleanTruncate(jobSource.utf8ToString(), 1000)
            )
            sweep(shardId, jobId, jobVersion, null, true)
            null
        }
    }

    private fun parseScheduledJob(xcp: XContentParser, jobId: JobId, jobVersion: JobVersion, typeIsParsed: Boolean): ScheduledJob {
        return if (typeIsParsed) {
            ScheduledJob.parse(xcp, xcp.currentName(), jobId, jobVersion)
        } else {
            ScheduledJob.parse(xcp, jobId, jobVersion)
        }
    }

    private fun getFullSweepElapsedTime(): TimeValue {
        return TimeValue.timeValueNanos(System.nanoTime() - lastFullSweepTimeNano)
    }

    fun getJobSweeperMetrics(): JobSweeperMetrics {
        if (!isSweepingEnabled()) {
            return JobSweeperMetrics(-1, true)
        }
        val elapsedTime = getFullSweepElapsedTime()
        return JobSweeperMetrics(elapsedTime.millis, elapsedTime.millis <= sweepPeriod.millis)
    }

    private fun isSweepableJobType(xcp: XContentParser): Boolean {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, xcp.nextToken(), xcp)
        val jobType = xcp.currentName()
        return sweepableJobTypes.contains(jobType)
    }

    private fun isOwningNode(shardId: ShardId, jobId: JobId): Boolean {
        val localNodeId = clusterService.localNode().id
        val shardNodeIds = clusterService.state().routingTable.shardRoutingTable(shardId)
            .filter { it.active() }
            .map { it.currentNodeId() }
        val shardNodes = ShardNodes(localNodeId, shardNodeIds)
        return shardNodes.isOwningNode(jobId)
    }
}

/**
 * A group of nodes in the cluster that contain active instances of a single OpenSearch shard.  This uses a consistent hash to divide
 * the jobs indexed in that shard amongst the nodes such that each job is "owned" by exactly one of the nodes.
 * The local node must have an active instance of the shard.
 *
 * Implementation notes: This class is not thread safe. It uses the same [hash function][Murmur3HashFunction] that OpenSearch uses
 * for routing. For each real node `100` virtual nodes are added to provide a good distribution.
 */
private class ShardNodes(val localNodeId: String, activeShardNodeIds: Collection<String>) {

    private val circle = TreeMap<Int, String>()

    companion object {
        private const val VIRTUAL_NODE_COUNT = 100
    }

    init {
        for (node in activeShardNodeIds) {
            for (i in 0 until VIRTUAL_NODE_COUNT) {
                circle[Murmur3HashFunction.hash(node + i)] = node
            }
        }
    }

    fun isOwningNode(id: JobId): Boolean {
        if (circle.isEmpty()) {
            return false
        }
        val hash = Murmur3HashFunction.hash(id)
        val nodeId = (circle.higherEntry(hash) ?: circle.firstEntry()).value
        return (localNodeId == nodeId)
    }
}
