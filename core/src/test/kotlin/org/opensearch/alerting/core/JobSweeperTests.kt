/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core

import org.junit.After
import org.junit.Before
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.opensearch.Version
import org.opensearch.alerting.core.schedule.JobScheduler
import org.opensearch.alerting.core.schedule.MockJobRunner
import org.opensearch.alerting.core.settings.ScheduledJobSettings.Companion.REQUEST_TIMEOUT
import org.opensearch.alerting.core.settings.ScheduledJobSettings.Companion.SWEEPER_ENABLED
import org.opensearch.alerting.core.settings.ScheduledJobSettings.Companion.SWEEP_BACKOFF_MILLIS
import org.opensearch.alerting.core.settings.ScheduledJobSettings.Companion.SWEEP_BACKOFF_RETRY_COUNT
import org.opensearch.alerting.core.settings.ScheduledJobSettings.Companion.SWEEP_PAGE_SIZE
import org.opensearch.alerting.core.settings.ScheduledJobSettings.Companion.SWEEP_PERIOD
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.routing.IndexShardRoutingTable
import org.opensearch.cluster.routing.RoutingTable
import org.opensearch.cluster.routing.ShardRouting
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.alerting.model.IntervalSchedule
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.common.transport.TransportAddress
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.engine.Engine
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.net.InetAddress
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Unit tests for the election that decides which node performs the cluster-wide cleanup after a scheduled job document
 * is written or removed. That cleanup is `JobRunner.postIndex`/`postDelete`, which is what moves the ACTIVE alerts of a
 * deleted monitor -- or of a trigger removed by an update -- into the alert history index.
 *
 * These are deliberately unit tests rather than integration tests. The defect they cover is that *every* shard copy can
 * simultaneously compute "I do not own this job" from its own asynchronously-applied cluster state, and no integration
 * test can force that interleaving. Here the divergent view is supplied directly: the mocked [ClusterService] returns a
 * cluster state in which the local node holds no copy of the config shard, so the consistent hash elects some other
 * node -- and, on that node, a state that has not converged yet elects a different one again, so nobody acts.
 *
 * Before the fix, the whole of `postIndex`/`postDelete` was gated on that check, so the deleted monitor's alerts were
 * never moved and nothing ever repaired them: the periodic sweep only discovers work by searching for job documents
 * that still exist, and the delete request itself returns 200.
 */
class JobSweeperTests {

    private val settings: Settings = Settings.builder().put("node.name", LOCAL_NODE_ID).build()

    private lateinit var threadPool: ThreadPool
    private lateinit var jobRunner: MockJobRunner
    private lateinit var jobScheduler: JobScheduler

    private val shardId = ShardId(Index(ScheduledJob.SCHEDULED_JOBS_INDEX, "config-index-uuid"), 0)

    @Before
    fun setupSweeper() {
        threadPool = ThreadPool(settings)
        jobRunner = MockJobRunner()
        jobScheduler = JobScheduler(threadPool, jobRunner)
    }

    @After
    fun tearDownSweeper() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS)
    }

    @Test
    fun `deleting a job runs cleanup on the primary when ownership resolved to another node`() {
        val sweeper = newSweeper(stateWithConfigShardOn(OTHER_NODE_IDS))

        sweeper.handleJobDeleted(shardId, JOB_ID, 2L, Engine.Operation.Origin.PRIMARY)

        assertEquals(
            1,
            jobRunner.numberOfDelete,
            "Cleanup for the deleted job was skipped, so its ACTIVE alerts would stay orphaned forever"
        )
    }

    @Test
    fun `deleting a job runs cleanup on the primary even when the routing table is missing`() {
        // A node can see a cluster state that has no routing table for the config index at all, e.g. while the index is
        // being created or right after it is deleted. Reading ownership out of that state used to throw
        // IndexNotFoundException straight into the indexing path.
        val sweeper = newSweeper(ClusterState.builder(ClusterName.DEFAULT).build())

        sweeper.handleJobDeleted(shardId, JOB_ID, 2L, Engine.Operation.Origin.PRIMARY)

        assertEquals(
            1,
            jobRunner.numberOfDelete,
            "Cleanup must not depend on the local cluster state having a routing table for the config index"
        )
    }

    @Test
    fun `deleting a job runs cleanup exactly once across all operation origins`() {
        val sweeper = newSweeper(stateWithConfigShardOn(OTHER_NODE_IDS))

        for (origin in Engine.Operation.Origin.values()) {
            sweeper.handleJobDeleted(shardId, JOB_ID, 2L, origin)
        }

        assertEquals(
            1,
            jobRunner.numberOfDelete,
            "Cleanup must run exactly once per delete: on the PRIMARY origin and on no shard copy"
        )
    }

    @Test
    fun `non-primary origins never run cleanup`() {
        val sweeper = newSweeper(stateWithConfigShardOn(OTHER_NODE_IDS))

        for (origin in Engine.Operation.Origin.values().filter { it != Engine.Operation.Origin.PRIMARY }) {
            sweeper.handleJobDeleted(shardId, JOB_ID, 2L, origin)
        }

        assertEquals(0, jobRunner.numberOfDelete, "A shard copy must not duplicate the cluster-wide cleanup")
    }

    @Test
    fun `indexing a job runs the post index hook on the primary when ownership resolved to another node`() {
        val sweeper = newSweeper(stateWithConfigShardOn(OTHER_NODE_IDS))

        sweeper.handleJobIndexed(shardId, JOB_ID, 2L, monitorSource(), Engine.Operation.Origin.PRIMARY)

        assertEquals(
            1,
            jobRunner.numberOfIndex,
            "postIndex was skipped, so the alerts of any trigger removed by this update would stay orphaned"
        )
    }

    @Test
    fun `indexing a job does not run the post index hook on a shard copy`() {
        val sweeper = newSweeper(stateWithConfigShardOn(OTHER_NODE_IDS))

        for (origin in Engine.Operation.Origin.values().filter { it != Engine.Operation.Origin.PRIMARY }) {
            sweeper.handleJobIndexed(shardId, JOB_ID, 2L, monitorSource(), origin)
        }

        assertEquals(0, jobRunner.numberOfIndex, "A shard copy must not duplicate the cluster-wide cleanup")
    }

    @Test
    fun `indexing a job on the primary does not schedule it on a node that does not own it`() {
        val sweeper = newSweeper(stateWithConfigShardOn(OTHER_NODE_IDS))

        sweeper.handleJobIndexed(shardId, JOB_ID, 2L, monitorSource(), Engine.Operation.Origin.PRIMARY)

        assertEquals(
            emptySet(),
            jobScheduler.scheduledJobs(),
            "Job scheduling must stay with the consistent-hash owner rather than follow the primary"
        )
    }

    @Test
    fun `indexing a job schedules it locally when this node is the consistent hash owner`() {
        val sweeper = newSweeper(stateWithConfigShardOn(listOf(LOCAL_NODE_ID)))

        sweeper.handleJobIndexed(shardId, JOB_ID, 2L, monitorSource(), Engine.Operation.Origin.PRIMARY)

        assertEquals(
            setOf(JOB_ID),
            jobScheduler.scheduledJobs(),
            "Electing the cleanup on the primary must not stop the hash owner from scheduling the job"
        )
    }

    private fun newSweeper(clusterState: ClusterState): JobSweeper {
        val clusterSettings = ClusterSettings(
            settings,
            setOf(SWEEP_PERIOD, SWEEPER_ENABLED, SWEEP_BACKOFF_MILLIS, SWEEP_BACKOFF_RETRY_COUNT, SWEEP_PAGE_SIZE, REQUEST_TIMEOUT)
        )
        val localNode = DiscoveryNode(
            LOCAL_NODE_ID,
            TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        )
        val clusterService = mock(ClusterService::class.java)
        `when`(clusterService.clusterSettings).thenReturn(clusterSettings)
        `when`(clusterService.localNode()).thenReturn(localNode)
        `when`(clusterService.state()).thenReturn(clusterState)

        return JobSweeper(
            settings,
            mock(Client::class.java),
            clusterService,
            threadPool,
            NamedXContentRegistry(listOf(Monitor.XCONTENT_REGISTRY)),
            jobScheduler,
            listOf(MONITOR_TYPE)
        )
    }

    /**
     * A cluster state whose config-index shard has an active copy on exactly [nodeIds].
     *
     * The routing table is mocked rather than built with `IndexShardRoutingTable.Builder`, because that builder pulls a
     * `Random` out of `RandomizedContext`, which only exists under the randomized test runner. `isOwningNode` reads
     * nothing from a shard routing beyond [ShardRouting.active] and [ShardRouting.currentNodeId].
     */
    private fun stateWithConfigShardOn(nodeIds: List<String>): ClusterState {
        val shardRoutings = nodeIds.map { nodeId ->
            val shardRouting = mock(ShardRouting::class.java)
            `when`(shardRouting.active()).thenReturn(true)
            `when`(shardRouting.currentNodeId()).thenReturn(nodeId)
            shardRouting
        }
        val shardRoutingTable = mock(IndexShardRoutingTable::class.java)
        // A fresh iterator per call: isOwningNode iterates the table on every invocation.
        `when`(shardRoutingTable.iterator()).thenAnswer { shardRoutings.iterator() }

        val routingTable = mock(RoutingTable::class.java)
        `when`(routingTable.shardRoutingTable(shardId)).thenReturn(shardRoutingTable)

        val clusterState = mock(ClusterState::class.java)
        `when`(clusterState.routingTable).thenReturn(routingTable)
        return clusterState
    }

    /**
     * A `monitor`-shaped source document, built from the real model so that it is guaranteed to round-trip through
     * `ScheduledJob.parse`. [JobSweeper.handleJobIndexed] needs a sweepable job type; the monitor is never run.
     */
    private fun monitorSource(): BytesReference = BytesReference.bytes(
        Monitor(
            id = JOB_ID,
            version = 2L,
            name = JOB_ID,
            enabled = true,
            schedule = IntervalSchedule(1, ChronoUnit.MINUTES),
            lastUpdateTime = Instant.ofEpochMilli(1700000000000L),
            enabledTime = Instant.ofEpochMilli(1700000000000L),
            monitorType = Monitor.MonitorType.QUERY_LEVEL_MONITOR.value,
            user = null,
            schemaVersion = 0,
            inputs = listOf(),
            triggers = listOf(),
            uiMetadata = mapOf()
        ).toXContentWithType(XContentFactory.jsonBuilder())
    )

    companion object {
        private const val JOB_ID = "monitor-1"
        private const val MONITOR_TYPE = "monitor"
        private const val LOCAL_NODE_ID = "node-0"

        /** Shard copies on nodes other than the local one, so the consistent hash can never elect this node. */
        private val OTHER_NODE_IDS = listOf("node-1", "node-2")
    }
}
