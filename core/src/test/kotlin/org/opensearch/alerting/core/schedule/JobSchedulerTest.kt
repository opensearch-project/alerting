/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.schedule

import org.junit.Before
import org.opensearch.alerting.core.model.MockScheduledJob
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.model.CronSchedule
import org.opensearch.commons.alerting.model.IntervalSchedule
import org.opensearch.threadpool.ThreadPool
import java.time.Instant
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class JobSchedulerTest {

    private var testSettings: Settings = Settings.builder().put("node.name", "node-0").build()
    private val testThreadPool = ThreadPool(testSettings)
    private var jobRunner: MockJobRunner = MockJobRunner()
    private var jobScheduler: JobScheduler = JobScheduler(ThreadPool(testSettings), jobRunner)

    @Before
    fun `setup`() {
        jobRunner = MockJobRunner()
        jobScheduler = JobScheduler(ThreadPool(testSettings), jobRunner)
    }

    @Test
    fun `schedule and deschedule`() {
        val mockScheduledJob = MockScheduledJob(
            "mockScheduledJob-id",
            1L,
            "mockScheduledJob-name",
            "MockScheduledJob",
            true,
            IntervalSchedule(1, ChronoUnit.MINUTES),
            Instant.now(),
            Instant.now()
        )

        assertTrue(jobScheduler.schedule(mockScheduledJob))

        assertEquals(setOf("mockScheduledJob-id"), jobScheduler.scheduledJobs(), "List of ScheduledJobs are not the same.")
        assertEquals(0, jobRunner.numberOfRun, "Number of JobRunner ran is wrong.")
        assertTrue(jobScheduler.deschedule("mockScheduledJob-id"), "Descheduling should be true.")
    }

    @Test
    fun `schedule cron past year`() {
        // This is to run cron in Feb 30 which we should never run.
        val cronExpression = "0/5 * 30 2 *"
        val jobRunner = MockJobRunner()
        val jobScheduler = JobScheduler(testThreadPool, jobRunner)
        val mockScheduledJob = MockScheduledJob(
            "mockScheduledJob-id",
            1L,
            "mockScheduledJob-name",
            "MockScheduledJob",
            true,
            CronSchedule(cronExpression, ZoneId.of("UTC")),
            Instant.now(),
            Instant.now()
        )

        assertTrue(jobScheduler.schedule(mockScheduledJob))
        assertEquals(setOf("mockScheduledJob-id"), jobScheduler.scheduledJobs(), "List of ScheduledJobs are not the same.")

        assertEquals(0, jobRunner.numberOfRun, "Number of JobRunner ran is wrong.")

        assertTrue(jobScheduler.deschedule("mockScheduledJob-id"), "Descheduling should be true.")
    }

    @Test
    fun `schedule disabled`() {
        val cronExpression = "0/5 * * * *"
        val jobRunner = MockJobRunner()
        val jobScheduler = JobScheduler(testThreadPool, jobRunner)
        val mockScheduledJob = MockScheduledJob(
            "mockScheduledJob-id",
            1L,
            "mockScheduledJob-name",
            "MockScheduledJob",
            false,
            CronSchedule(cronExpression, ZoneId.of("UTC")),
            Instant.now(),
            Instant.now()
        )

        assertFalse(jobScheduler.schedule(mockScheduledJob), "We should return false if we try to schedule disabled schedule.")
        assertEquals(setOf(), jobScheduler.scheduledJobs(), "List of ScheduledJobs are not the same.")
    }

    @Test
    fun `deschedule non existing schedule`() {
        val cronExpression = "0/5 * * * *"
        val jobRunner = MockJobRunner()
        val jobScheduler = JobScheduler(testThreadPool, jobRunner)
        val mockScheduledJob = MockScheduledJob(
            "mockScheduledJob-id",
            1L,
            "mockScheduledJob-name",
            "MockScheduledJob",
            true,
            CronSchedule(cronExpression, ZoneId.of("UTC")),
            Instant.now(),
            Instant.now()
        )

        assertTrue(jobScheduler.schedule(mockScheduledJob))
        assertEquals(setOf("mockScheduledJob-id"), jobScheduler.scheduledJobs(), "List of ScheduledJobs are not the same.")

        assertEquals(0, jobRunner.numberOfRun, "Number of JobRunner ran is wrong.")

        assertTrue(jobScheduler.deschedule("mockScheduledJob-invalid"), "Descheduling should be true.")
        assertTrue(jobScheduler.deschedule("mockScheduledJob-id"), "Descheduling should be true.")
    }

    @Test
    fun `schedule multiple jobs`() {
        val cronExpression = "0/5 * * * *"
        val mockScheduledJob1 = MockScheduledJob(
            "mockScheduledJob-1",
            1L,
            "mockScheduledJob-name",
            "MockScheduledJob",
            true,
            CronSchedule(cronExpression, ZoneId.of("UTC")),
            Instant.now(),
            Instant.now()
        )
        val mockScheduledJob2 = MockScheduledJob(
            "mockScheduledJob-2",
            1L,
            "mockScheduledJob-name",
            "MockScheduledJob",
            true,
            CronSchedule(cronExpression, ZoneId.of("UTC")),
            Instant.now(),
            Instant.now()
        )

        assertTrue(jobScheduler.schedule(mockScheduledJob1, mockScheduledJob2).isEmpty())
    }

    @Test
    fun `schedule null enabled time job`() {
        val cronExpression = "0/5 * * * *"
        val mockScheduledJob2 = MockScheduledJob(
            "mockScheduledJob-2", 1L, "mockScheduledJob-name", "MockScheduledJob", true,
            CronSchedule(cronExpression, ZoneId.of("UTC")), Instant.now(), null
        )

        assertFalse(jobScheduler.schedule(mockScheduledJob2))
    }

    @Test
    fun `schedule disabled job`() {
        val cronExpression = "0/5 * * * *"
        val mockScheduledJob1 = MockScheduledJob(
            "mockScheduledJob-1", 1L, "mockScheduledJob-name", "MockScheduledJob", false,
            CronSchedule(cronExpression, ZoneId.of("UTC")), Instant.now(), Instant.now()
        )

        assertFalse(jobScheduler.schedule(mockScheduledJob1))
    }

    @Test
    fun `run Job`() {
        val cronExpression = "0/5 * * * *"
        val mockScheduledJob = MockScheduledJob(
            "mockScheduledJob-id",
            1L,
            "mockScheduledJob-name",
            "MockScheduledJob",
            true,
            CronSchedule(cronExpression, ZoneId.of("UTC")),
            Instant.now(),
            Instant.now()
        )

        jobRunner.runJob(mockScheduledJob, Instant.now(), Instant.now())
    }
}
