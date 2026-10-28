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
import org.opensearch.cluster.service.ClusterService
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import org.mockito.Mockito.`when` as whenever

/**
 * Unit tests for [DestinationMigrationCoordinator.clusterChanged].
 *
 * These tests focus on the index-scope guard added at the top of [clusterChanged]:
 *   if (!event.indexRoutingTableChanged(ScheduledJob.SCHEDULED_JOBS_INDEX)) return
 *
 * Strategy: construct the coordinator with a mocked [ClusterService] so the
 * `init` block's `addListener` / `addLifecycleListener` calls succeed without
 * a real cluster, then drive [clusterChanged] with a mocked [ClusterChangedEvent]
 * whose [ClusterChangedEvent.indexRoutingTableChanged] return value controls
 * which branch is exercised.
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
    // Test 1: guard short-circuits when the scheduled-jobs index did NOT change
    // -------------------------------------------------------------------------

    fun `test clusterChanged skips all logic when indexRoutingTableChanged returns false`() {
        val event = Mockito.mock(ClusterChangedEvent::class.java)
        whenever(event.indexRoutingTableChanged(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(false)

        coordinator.clusterChanged(event)

        // scheduledJobIndices must never be touched — that method is only called
        // inside initMigrateDestinations(), which is unreachable when the guard fires.
        verify(scheduledJobIndices, never()).scheduledJobIndexExists()
        // localNodeClusterManager() must not be evaluated either.
        verify(event, never()).localNodeClusterManager()
    }

    // -------------------------------------------------------------------------
    // Test 2: guard passes through when the scheduled-jobs index DID change
    //         and the node is not the cluster manager (non-manager branch)
    // -------------------------------------------------------------------------

    fun `test clusterChanged passes guard when indexRoutingTableChanged returns true for SCHEDULED_JOBS_INDEX`() {
        val event = Mockito.mock(ClusterChangedEvent::class.java)
        whenever(event.indexRoutingTableChanged(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(true)
        // Not the cluster manager — avoids triggering the scheduling path.
        whenever(event.localNodeClusterManager()).thenReturn(false)

        coordinator.clusterChanged(event)

        // The guard was passed, so localNodeClusterManager() must have been evaluated.
        verify(event).localNodeClusterManager()
    }

    // -------------------------------------------------------------------------
    // Test 3: finishFlag is reset to false when the guard passes and finishFlag
    //         was true before the call
    // -------------------------------------------------------------------------

    fun `test clusterChanged resets finishFlag when guard passes and finishFlag was true`() {
        DestinationMigrationUtilService.finishFlag = true

        val event = Mockito.mock(ClusterChangedEvent::class.java)
        whenever(event.indexRoutingTableChanged(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(true)
        whenever(event.localNodeClusterManager()).thenReturn(false)

        coordinator.clusterChanged(event)

        assertFalse(
            "finishFlag should be reset to false when clusterChanged processes a relevant index change",
            DestinationMigrationUtilService.finishFlag,
        )
    }

    // -------------------------------------------------------------------------
    // Test 4: migration does NOT start when the node is not the cluster manager,
    //         even though the guard passes
    // -------------------------------------------------------------------------

    fun `test clusterChanged does not start migration when node is not cluster manager`() {
        val event = Mockito.mock(ClusterChangedEvent::class.java)
        whenever(event.indexRoutingTableChanged(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(true)
        whenever(event.localNodeClusterManager()).thenReturn(false)

        coordinator.clusterChanged(event)

        // initMigrateDestinations() is guarded by localNodeClusterManager() == true.
        // When that is false the method must never be entered, so scheduledJobIndices
        // must remain untouched.
        verify(scheduledJobIndices, never()).scheduledJobIndexExists()
    }

    // -------------------------------------------------------------------------
    // Test 5: guard does NOT short-circuit for an unrelated index change
    //         — verifying the guard checks the exact SCHEDULED_JOBS_INDEX name
    // -------------------------------------------------------------------------

    fun `test clusterChanged skips logic for unrelated index routing table change`() {
        val event = Mockito.mock(ClusterChangedEvent::class.java)
        // The event reports that some other index changed, not SCHEDULED_JOBS_INDEX.
        whenever(event.indexRoutingTableChanged(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(false)
        whenever(event.indexRoutingTableChanged("some-other-index")).thenReturn(true)

        coordinator.clusterChanged(event)

        verify(scheduledJobIndices, never()).scheduledJobIndexExists()
        verify(event, never()).localNodeClusterManager()
    }

    // -------------------------------------------------------------------------
    // Test 6: cluster manager path calls scheduledJobIndexExists() when guard
    //         passes and the node IS the cluster manager
    // -------------------------------------------------------------------------

    fun `test clusterChanged enters migration init path when node is cluster manager and guard passes`() {
        val clusterState = Mockito.mock(ClusterState::class.java)
        val discoveryNodes = Mockito.mock(DiscoveryNodes::class.java)
        whenever(clusterService.state()).thenReturn(clusterState)
        whenever(clusterState.nodes()).thenReturn(discoveryNodes)
        whenever(discoveryNodes.isLocalNodeElectedClusterManager).thenReturn(true)

        // scheduledJobIndexExists() returns false so initMigrateDestinations bails
        // early without attempting to schedule anything via threadPool.
        whenever(scheduledJobIndices.scheduledJobIndexExists()).thenReturn(false)

        val event = Mockito.mock(ClusterChangedEvent::class.java)
        whenever(event.indexRoutingTableChanged(ScheduledJob.SCHEDULED_JOBS_INDEX)).thenReturn(true)
        whenever(event.localNodeClusterManager()).thenReturn(true)

        coordinator.clusterChanged(event)

        // initMigrateDestinations() must have been entered — scheduledJobIndexExists()
        // is the first call inside it.
        verify(scheduledJobIndices).scheduledJobIndexExists()
    }
}
