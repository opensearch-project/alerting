/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.destinationmigration

import com.carrotsearch.randomizedtesting.ThreadFilter
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters
import org.junit.After
import org.junit.Before
import org.mockito.Mockito
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.node.DiscoveryNodes
import org.opensearch.cluster.routing.RoutingTable
import org.opensearch.cluster.service.ClusterService
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import org.mockito.Mockito.`when` as whenever

/**
 * Unit tests for [DestinationMigrationCoordinator.clusterChanged].
 *
 * Control flow after the fix:
 *   1. Demotion guard (fail-closed): if NOT cluster manager → cancel migration & return.
 *      This runs unconditionally so demotion always stops in-progress work.
 *   2. Index-scope guard: if index EXISTS AND routing UNCHANGED AND node was ALREADY manager → return.
 *      Allows initiation on routing changes, bootstrap (index absent), and manager-promotion events.
 *   3. Migration initiation logic (finishFlag reset, runningLock, initMigrateDestinations).
 *
 * Strategy: construct the coordinator with a mocked [ClusterService] so the
 * `init` block's `addListener` / `addLifecycleListener` calls succeed without
 * a real cluster, then drive [clusterChanged] with a mocked [ClusterChangedEvent]
 * configured to exercise each branch.
 */
@ThreadLeakFilters(filters = [DestinationMigrationCoordinatorTests.CoroutineThreadFilter::class])
class DestinationMigrationCoordinatorTests : OpenSearchTestCase() {

    /** Suppress leaked coroutine dispatcher threads from reaching the scheduler path. */
    class CoroutineThreadFilter : ThreadFilter {
        override fun reject(t: Thread): Boolean = t.name.startsWith("DefaultDispatcher-worker")
    }

    private lateinit var client: Client
    private lateinit var clusterService: ClusterService
    private lateinit var threadPool: ThreadPool
    private lateinit var scheduledJobIndices: ScheduledJobIndices
    private lateinit var coordinator: DestinationMigrationCoordinator

    @Before
    fun setup() {
        client = Mockito.mock(Client::class.java)
        clusterService = Mockito.mock(ClusterService::class.java)
        threadPool = Mockito.mock(ThreadPool::class.java)
        scheduledJobIndices = Mockito.mock(ScheduledJobIndices::class.java)

        coordinator = DestinationMigrationCoordinator(client, clusterService, threadPool, scheduledJobIndices)
    }

    @After
    override fun tearDown() {
        // Reset global state so tests do not leak into one another.
        DestinationMigrationUtilService.finishFlag = false
        super.tearDown()
    }

    // -------------------------------------------------------------------------
    // Test 1: demotion cancels active migration regardless of index changes
    //         (fail-closed: the demotion guard runs before the routing-table guard)
    // -------------------------------------------------------------------------

    fun `test clusterChanged cancels active migration when node is demoted from cluster manager`() {
        // Simulate an active, non-cancelled scheduled migration.
        val activeMigration = Mockito.mock(Scheduler.Cancellable::class.java)
        whenever(activeMigration.isCancelled).thenReturn(false)

        // Seed the coordinator with the active migration via the manager path first.
        // We do this by using reflection to set scheduledMigration directly.
        val field = DestinationMigrationCoordinator::class.java.getDeclaredField("scheduledMigration")
        field.isAccessible = true
        field.set(coordinator, activeMigration)

        // Demotion event: NOT the cluster manager. Routing table is irrelevant —
        // the demotion guard runs unconditionally before the routing-table guard.
        val event = Mockito.mock(ClusterChangedEvent::class.java)
        whenever(event.localNodeClusterManager()).thenReturn(false)
        // Even if SCHEDULED_JOBS_INDEX did not change, cancellation must still fire.
        whenever(event.indexRoutingTableChanged(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(false)

        coordinator.clusterChanged(event)

        // The active migration must have been cancelled.
        verify(activeMigration).cancel()
    }

    // -------------------------------------------------------------------------
    // Test 2: index-scope guard short-circuits when the scheduled-jobs index
    //         did NOT change and node IS cluster manager
    // -------------------------------------------------------------------------

    fun `test clusterChanged skips migration initiation when indexRoutingTableChanged returns false`() {
        val event = Mockito.mock(ClusterChangedEvent::class.java)
        // Node is cluster manager — demotion guard does not fire.
        whenever(event.localNodeClusterManager()).thenReturn(true)
        // Scheduled-jobs index did not change — routing-table guard fires.
        whenever(event.indexRoutingTableChanged(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(false)

        // Stub for the new guard variables: index exists, was already manager → guard short-circuits.
        val currentState = Mockito.mock(ClusterState::class.java)
        val previousState = Mockito.mock(ClusterState::class.java)
        val currentRoutingTable = Mockito.mock(RoutingTable::class.java)
        val currentNodes = Mockito.mock(DiscoveryNodes::class.java)
        val previousNodes = Mockito.mock(DiscoveryNodes::class.java)
        whenever(event.state()).thenReturn(currentState)
        whenever(event.previousState()).thenReturn(previousState)
        whenever(currentState.routingTable()).thenReturn(currentRoutingTable)
        whenever(currentState.nodes()).thenReturn(currentNodes)
        whenever(previousState.nodes()).thenReturn(previousNodes)
        whenever(currentRoutingTable.hasIndex(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(true)
        whenever(currentNodes.isLocalNodeElectedClusterManager).thenReturn(true)
        whenever(previousNodes.isLocalNodeElectedClusterManager).thenReturn(true) // not a new promotion

        coordinator.clusterChanged(event)

        // initMigrateDestinations() must never be entered.
        verify(scheduledJobIndices, never()).scheduledJobIndexExists()
    }

    // -------------------------------------------------------------------------
    // Test 3: finishFlag is reset when both guards pass (node is manager, index changed)
    // -------------------------------------------------------------------------

    fun `test clusterChanged resets finishFlag when both guards pass and finishFlag was true`() {
        DestinationMigrationUtilService.finishFlag = true

        val clusterState = Mockito.mock(ClusterState::class.java)
        val discoveryNodes = Mockito.mock(DiscoveryNodes::class.java)
        whenever(clusterService.state()).thenReturn(clusterState)
        whenever(clusterState.nodes()).thenReturn(discoveryNodes)
        // isLocalNodeElectedClusterManager is checked inside initMigrateDestinations.
        whenever(discoveryNodes.isLocalNodeElectedClusterManager).thenReturn(false)

        val event = Mockito.mock(ClusterChangedEvent::class.java)
        whenever(event.localNodeClusterManager()).thenReturn(true)
        whenever(event.indexRoutingTableChanged(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(true)

        // Stub for the new guard variables: index exists, indexChanged=true → guard passes.
        val currentState = Mockito.mock(ClusterState::class.java)
        val previousState = Mockito.mock(ClusterState::class.java)
        val currentRoutingTable = Mockito.mock(RoutingTable::class.java)
        val currentNodes = Mockito.mock(DiscoveryNodes::class.java)
        val previousNodes = Mockito.mock(DiscoveryNodes::class.java)
        whenever(event.state()).thenReturn(currentState)
        whenever(event.previousState()).thenReturn(previousState)
        whenever(currentState.routingTable()).thenReturn(currentRoutingTable)
        whenever(currentState.nodes()).thenReturn(currentNodes)
        whenever(previousState.nodes()).thenReturn(previousNodes)
        whenever(currentRoutingTable.hasIndex(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(true)
        whenever(currentNodes.isLocalNodeElectedClusterManager).thenReturn(true)
        whenever(previousNodes.isLocalNodeElectedClusterManager).thenReturn(true) // already was manager

        coordinator.clusterChanged(event)

        assertFalse(
            "finishFlag should be reset to false after clusterChanged processes a relevant index change",
            DestinationMigrationUtilService.finishFlag,
        )
    }

    // -------------------------------------------------------------------------
    // Test 4: unrelated index change with node as cluster manager does not
    //         trigger migration initiation
    // -------------------------------------------------------------------------

    fun `test clusterChanged skips logic for unrelated index routing table change`() {
        val event = Mockito.mock(ClusterChangedEvent::class.java)
        whenever(event.localNodeClusterManager()).thenReturn(true)
        // Only some other index changed, not SCHEDULED_JOBS_INDEX.
        whenever(event.indexRoutingTableChanged(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(false)
        whenever(event.indexRoutingTableChanged("some-other-index")).thenReturn(true)

        // Stub for the new guard variables: index exists, was already manager → guard short-circuits.
        val currentState = Mockito.mock(ClusterState::class.java)
        val previousState = Mockito.mock(ClusterState::class.java)
        val currentRoutingTable = Mockito.mock(RoutingTable::class.java)
        val currentNodes = Mockito.mock(DiscoveryNodes::class.java)
        val previousNodes = Mockito.mock(DiscoveryNodes::class.java)
        whenever(event.state()).thenReturn(currentState)
        whenever(event.previousState()).thenReturn(previousState)
        whenever(currentState.routingTable()).thenReturn(currentRoutingTable)
        whenever(currentState.nodes()).thenReturn(currentNodes)
        whenever(previousState.nodes()).thenReturn(previousNodes)
        whenever(currentRoutingTable.hasIndex(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(true)
        whenever(currentNodes.isLocalNodeElectedClusterManager).thenReturn(true)
        whenever(previousNodes.isLocalNodeElectedClusterManager).thenReturn(true) // not a new promotion

        coordinator.clusterChanged(event)

        verify(scheduledJobIndices, never()).scheduledJobIndexExists()
    }

    // -------------------------------------------------------------------------
    // Test 5: cluster manager path enters initMigrateDestinations when both
    //         guards pass
    // -------------------------------------------------------------------------

    fun `test clusterChanged enters migration init path when node is cluster manager and index changed`() {
        val clusterState = Mockito.mock(ClusterState::class.java)
        val discoveryNodes = Mockito.mock(DiscoveryNodes::class.java)
        whenever(clusterService.state()).thenReturn(clusterState)
        whenever(clusterState.nodes()).thenReturn(discoveryNodes)
        whenever(discoveryNodes.isLocalNodeElectedClusterManager).thenReturn(true)

        // scheduledJobIndexExists() returns false so initMigrateDestinations bails
        // early without scheduling anything via threadPool.
        whenever(scheduledJobIndices.scheduledJobIndexExists()).thenReturn(false)

        val event = Mockito.mock(ClusterChangedEvent::class.java)
        whenever(event.localNodeClusterManager()).thenReturn(true)
        whenever(event.indexRoutingTableChanged(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(true)

        // Stub for the new guard variables: index exists, indexChanged=true → guard passes.
        val currentState = Mockito.mock(ClusterState::class.java)
        val previousState = Mockito.mock(ClusterState::class.java)
        val currentRoutingTable = Mockito.mock(RoutingTable::class.java)
        val currentNodes = Mockito.mock(DiscoveryNodes::class.java)
        val previousNodes = Mockito.mock(DiscoveryNodes::class.java)
        whenever(event.state()).thenReturn(currentState)
        whenever(event.previousState()).thenReturn(previousState)
        whenever(currentState.routingTable()).thenReturn(currentRoutingTable)
        whenever(currentState.nodes()).thenReturn(currentNodes)
        whenever(previousState.nodes()).thenReturn(previousNodes)
        whenever(currentRoutingTable.hasIndex(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(true)
        whenever(currentNodes.isLocalNodeElectedClusterManager).thenReturn(true)
        whenever(previousNodes.isLocalNodeElectedClusterManager).thenReturn(true) // already was manager

        coordinator.clusterChanged(event)

        // initMigrateDestinations() must have been entered — scheduledJobIndexExists()
        // is the first call inside it.
        verify(scheduledJobIndices).scheduledJobIndexExists()
    }

    // -------------------------------------------------------------------------
    // Test 6: demotion with no active migration does not throw or call cancel
    // -------------------------------------------------------------------------

    fun `test clusterChanged handles demotion gracefully when no migration is active`() {
        // scheduledMigration is null by default after construction.
        val event = Mockito.mock(ClusterChangedEvent::class.java)
        whenever(event.localNodeClusterManager()).thenReturn(false)

        // Should not throw.
        coordinator.clusterChanged(event)

        // No cancellable exists, so scheduledJobIndices must never be touched.
        verify(scheduledJobIndices, never()).scheduledJobIndexExists()
    }

    // -------------------------------------------------------------------------
    // Test 7: bootstrap path — index absent, node is cluster manager
    //         indexRoutingTableChanged returns false but index does not exist, so
    //         the guard must NOT short-circuit and migration initiation must fire.
    // -------------------------------------------------------------------------

    fun `test clusterChanged triggers migration when index does not exist yet (bootstrap path)`() {
        val clusterState = Mockito.mock(ClusterState::class.java)
        val discoveryNodes = Mockito.mock(DiscoveryNodes::class.java)
        whenever(clusterService.state()).thenReturn(clusterState)
        whenever(clusterState.nodes()).thenReturn(discoveryNodes)
        whenever(discoveryNodes.isLocalNodeElectedClusterManager).thenReturn(true)

        // scheduledJobIndexExists() returns false so initMigrateDestinations bails early.
        whenever(scheduledJobIndices.scheduledJobIndexExists()).thenReturn(false)

        val event = Mockito.mock(ClusterChangedEvent::class.java)
        whenever(event.localNodeClusterManager()).thenReturn(true)
        // Routing table did not change (index never existed), but...
        whenever(event.indexRoutingTableChanged(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(false)

        // ...the index does not exist in the routing table — bootstrap path.
        val currentState = Mockito.mock(ClusterState::class.java)
        val previousState = Mockito.mock(ClusterState::class.java)
        val currentRoutingTable = Mockito.mock(RoutingTable::class.java)
        val currentNodes = Mockito.mock(DiscoveryNodes::class.java)
        val previousNodes = Mockito.mock(DiscoveryNodes::class.java)
        whenever(event.state()).thenReturn(currentState)
        whenever(event.previousState()).thenReturn(previousState)
        whenever(currentState.routingTable()).thenReturn(currentRoutingTable)
        whenever(currentState.nodes()).thenReturn(currentNodes)
        whenever(previousState.nodes()).thenReturn(previousNodes)
        whenever(currentRoutingTable.hasIndex(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(false) // index absent
        whenever(currentNodes.isLocalNodeElectedClusterManager).thenReturn(true)
        whenever(previousNodes.isLocalNodeElectedClusterManager).thenReturn(true)

        coordinator.clusterChanged(event)

        // Guard must not short-circuit: initMigrateDestinations() is entered.
        verify(scheduledJobIndices).scheduledJobIndexExists()
    }

    // -------------------------------------------------------------------------
    // Test 8: manager promotion — index exists, routing unchanged, node just
    //         became cluster manager; migration initiation must still be triggered.
    // -------------------------------------------------------------------------

    fun `test clusterChanged triggers migration when node just became cluster manager (promotion path)`() {
        val clusterState = Mockito.mock(ClusterState::class.java)
        val discoveryNodes = Mockito.mock(DiscoveryNodes::class.java)
        whenever(clusterService.state()).thenReturn(clusterState)
        whenever(clusterState.nodes()).thenReturn(discoveryNodes)
        whenever(discoveryNodes.isLocalNodeElectedClusterManager).thenReturn(true)

        // scheduledJobIndexExists() returns false so initMigrateDestinations bails early.
        whenever(scheduledJobIndices.scheduledJobIndexExists()).thenReturn(false)

        val event = Mockito.mock(ClusterChangedEvent::class.java)
        whenever(event.localNodeClusterManager()).thenReturn(true)
        // Index routing table did not change — but the node was just promoted.
        whenever(event.indexRoutingTableChanged(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(false)

        val currentState = Mockito.mock(ClusterState::class.java)
        val previousState = Mockito.mock(ClusterState::class.java)
        val currentRoutingTable = Mockito.mock(RoutingTable::class.java)
        val currentNodes = Mockito.mock(DiscoveryNodes::class.java)
        val previousNodes = Mockito.mock(DiscoveryNodes::class.java)
        whenever(event.state()).thenReturn(currentState)
        whenever(event.previousState()).thenReturn(previousState)
        whenever(currentState.routingTable()).thenReturn(currentRoutingTable)
        whenever(currentState.nodes()).thenReturn(currentNodes)
        whenever(previousState.nodes()).thenReturn(previousNodes)
        whenever(currentRoutingTable.hasIndex(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(true) // index exists
        whenever(currentNodes.isLocalNodeElectedClusterManager).thenReturn(true) // now is manager
        whenever(previousNodes.isLocalNodeElectedClusterManager).thenReturn(false) // was NOT manager before

        coordinator.clusterChanged(event)

        // Guard must not short-circuit: initMigrateDestinations() is entered.
        verify(scheduledJobIndices).scheduledJobIndexExists()
    }
}
