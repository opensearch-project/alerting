/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core

import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.PlainActionFuture
import org.opensearch.alerting.core.model.MockScheduledJob
import org.opensearch.alerting.core.schedule.JobScheduler
import org.opensearch.alerting.core.schedule.MockJobRunner
import org.opensearch.alerting.core.settings.LegacyOpenDistroScheduledJobSettings
import org.opensearch.alerting.core.settings.ScheduledJobSettings
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.routing.OperationRouting
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.model.IntervalSchedule
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.IndexNotFoundException
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import org.mockito.Mockito.`when` as whenever

/**
 * Coverage for the sweep's orphan reconciliation.
 *
 * The situation it exists for cannot be produced by the indexing listener alone: on a remote-store enabled scheduled
 * jobs index the replica copy does not execute deletes, so a node that owns a job but holds the replica never observes
 * the delete and keeps running the job from memory. These tests drive [JobSweeper.reconcileOrphans] with the state that
 * leaves behind -- a job in the sweep's memory, still scheduled, that the shard search did not return.
 *
 * The scheduler is real rather than a double, because what is being asserted is that the job actually leaves the
 * scheduler, not that a method was called on it.
 */
class JobSweeperOrphanReconciliationTests {

    private val testSettings: Settings = Settings.builder().put("node.name", "node-0").build()
    private val shardId = ShardId(Index(ScheduledJob.SCHEDULED_JOBS_INDEX, "uuid"), 0)

    private lateinit var client: Client
    private lateinit var clusterService: ClusterService
    private lateinit var threadPool: ThreadPool
    private lateinit var scheduler: JobScheduler
    private lateinit var sweeper: JobSweeper

    /** Served by the stubbed client for the next existence check, or an exception for it to fail with. */
    private var documentExists: Boolean = false
    private var getFailure: Exception? = null

    @Before
    fun setup() {
        client = Mockito.mock(Client::class.java)
        clusterService = Mockito.mock(ClusterService::class.java)
        threadPool = ThreadPool(testSettings)
        scheduler = JobScheduler(threadPool, MockJobRunner())
        documentExists = false
        getFailure = null

        val settingsSet = setOf(
            ScheduledJobSettings.REQUEST_TIMEOUT,
            ScheduledJobSettings.SWEEP_BACKOFF_MILLIS,
            ScheduledJobSettings.SWEEP_BACKOFF_RETRY_COUNT,
            ScheduledJobSettings.SWEEP_PERIOD,
            ScheduledJobSettings.SWEEP_PAGE_SIZE,
            ScheduledJobSettings.SWEEPER_ENABLED,
            ScheduledJobSettings.SWEEP_ORPHAN_RECONCILIATION_ENABLED,
            LegacyOpenDistroScheduledJobSettings.REQUEST_TIMEOUT,
            LegacyOpenDistroScheduledJobSettings.SWEEP_BACKOFF_MILLIS,
            LegacyOpenDistroScheduledJobSettings.SWEEP_BACKOFF_RETRY_COUNT,
            LegacyOpenDistroScheduledJobSettings.SWEEP_PERIOD,
            LegacyOpenDistroScheduledJobSettings.SWEEP_PAGE_SIZE,
            LegacyOpenDistroScheduledJobSettings.SWEEPER_ENABLED
        )
        whenever(clusterService.clusterSettings).thenReturn(ClusterSettings(Settings.EMPTY, settingsSet))
        whenever(clusterService.state()).thenReturn(Mockito.mock(ClusterState::class.java))
        routeEverythingTo(shardId)

        whenever(client.get(Mockito.any())).thenAnswer {
            val future = PlainActionFuture<GetResponse>()
            val failure = getFailure
            if (failure != null) {
                future.onFailure(failure)
            } else {
                val response = Mockito.mock(GetResponse::class.java)
                whenever(response.isExists).thenReturn(documentExists)
                future.onResponse(response)
            }
            future
        }

        sweeper = newSweeper(reconciliationEnabled = true)
    }

    @After
    fun tearDown() {
        threadPool.shutdownNow()
    }

    private fun routeEverythingTo(target: ShardId) {
        val routing = Mockito.mock(OperationRouting::class.java)
        whenever(
            routing.shardId(
                Mockito.any(ClusterState::class.java), Mockito.anyString(), Mockito.anyString(), Mockito.isNull()
            )
        ).thenReturn(target)
        whenever(clusterService.operationRouting()).thenReturn(routing)
    }

    private fun newSweeper(reconciliationEnabled: Boolean): JobSweeper {
        val settings = Settings.builder()
            .put(ScheduledJobSettings.SWEEP_ORPHAN_RECONCILIATION_ENABLED.key, reconciliationEnabled)
            .build()
        return JobSweeper(settings, client, clusterService, threadPool, NamedXContentRegistry.EMPTY, scheduler, listOf("monitor"))
    }

    /** Puts [jobId] into the real scheduler, which is the state a node that never saw the delete is left in. */
    private fun schedule(jobId: String) {
        val job = MockScheduledJob(
            jobId, 1L, "$jobId-name", "MockScheduledJob", true,
            IntervalSchedule(1, ChronoUnit.MINUTES), Instant.now(), Instant.now()
        )
        assertTrue("Precondition: the job must be scheduled", scheduler.schedule(job))
    }

    private fun inMemory(vararg jobIds: String): ConcurrentHashMap<JobId, JobVersion> {
        val jobs = ConcurrentHashMap<JobId, JobVersion>()
        jobIds.forEach { jobs[it] = 1L }
        return jobs
    }

    @Test
    fun `test an orphaned job whose document is gone is descheduled`() {
        // The delete was never observed on this node, so the job is still scheduled and still in the sweep's memory,
        // but the shard search no longer returns it.
        schedule("orphan")
        documentExists = false
        val jobs = inMemory("orphan")

        sweeper.reconcileOrphans(shardId, jobs, emptySet())

        assertFalse("The orphan must leave the scheduler", scheduler.scheduledJobs().contains("orphan"))
        assertTrue("The stale sweep entry must be dropped", jobs.isEmpty())
    }

    @Test
    fun `test a job the sweep returned is left alone`() {
        schedule("live")
        val jobs = inMemory("live")

        sweeper.reconcileOrphans(shardId, jobs, setOf("live"))

        assertTrue(scheduler.scheduledJobs().contains("live"))
        verify(client, never()).get(Mockito.any())
        assertEquals(setOf<JobId>("live"), jobs.keys)
    }

    @Test
    fun `test a stale entry for a job that is no longer scheduled here is dropped without a get`() {
        // postDelete already descheduled it on this node; only the version entry is left behind, and confirming the
        // document's absence would be a pointless round trip.
        val jobs = inMemory("already-gone")

        sweeper.reconcileOrphans(shardId, jobs, emptySet())

        verify(client, never()).get(Mockito.any())
        assertTrue(jobs.isEmpty())
    }

    @Test
    fun `test a job whose document still exists is not descheduled`() {
        // The sweep search is refresh-bound, so a job re-indexed after the last refresh is absent from the results
        // while its document is live. The realtime primary GET is what keeps it scheduled.
        schedule("just-reindexed")
        documentExists = true
        val jobs = inMemory("just-reindexed")

        sweeper.reconcileOrphans(shardId, jobs, emptySet())

        assertTrue("A live job must stay scheduled", scheduler.scheduledJobs().contains("just-reindexed"))
        assertEquals(setOf<JobId>("just-reindexed"), jobs.keys)
    }

    @Test
    fun `test a failed existence check leaves the job scheduled`() {
        // An unreadable index says nothing about whether the job is there, and descheduling a live monitor is worse
        // than leaving an orphan for one more sweep.
        schedule("unknown")
        getFailure = IndexNotFoundException(ScheduledJob.SCHEDULED_JOBS_INDEX)
        val jobs = inMemory("unknown")

        sweeper.reconcileOrphans(shardId, jobs, emptySet())

        assertTrue(scheduler.scheduledJobs().contains("unknown"))
        assertEquals(setOf<JobId>("unknown"), jobs.keys)
    }

    @Test
    fun `test reconciliation does nothing when disabled`() {
        val disabled = newSweeper(reconciliationEnabled = false)
        schedule("orphan")
        documentExists = false
        val jobs = inMemory("orphan")

        disabled.reconcileOrphans(shardId, jobs, emptySet())

        assertTrue(scheduler.scheduledJobs().contains("orphan"))
        verify(client, never()).get(Mockito.any())
        assertEquals(setOf<JobId>("orphan"), jobs.keys)
    }

    @Test
    fun `test a job that does not route to the swept shard is skipped`() {
        // A document indexed with custom routing cannot be verified by a GET on id, so it is left alone rather than
        // descheduled on inconclusive evidence.
        routeEverythingTo(ShardId(Index(ScheduledJob.SCHEDULED_JOBS_INDEX, "uuid"), 1))
        schedule("custom-routed")
        val jobs = inMemory("custom-routed")

        sweeper.reconcileOrphans(shardId, jobs, emptySet())

        verify(client, never()).get(Mockito.any())
        assertTrue(scheduler.scheduledJobs().contains("custom-routed"))
        assertEquals(setOf<JobId>("custom-routed"), jobs.keys)
    }
}
