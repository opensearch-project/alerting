/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.action.node

import org.apache.logging.log4j.LogManager
import org.opensearch.action.FailedNodeException
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.nodes.BaseNodeRequest
import org.opensearch.action.support.nodes.TransportNodesAction
import org.opensearch.alerting.core.JobSweeper
import org.opensearch.alerting.core.JobSweeperMetrics
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.core.schedule.JobScheduler
import org.opensearch.alerting.core.schedule.JobSchedulerMetrics
import org.opensearch.cluster.health.ClusterIndexHealth
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.io.IOException

private val log = LogManager.getLogger(ScheduledJobsStatsTransportAction::class.java)

class ScheduledJobsStatsTransportAction : TransportNodesAction<ScheduledJobsStatsRequest, ScheduledJobsStatsResponse,
        ScheduledJobsStatsTransportAction.ScheduledJobStatusRequest, ScheduledJobStats> {

    private val jobSweeper: JobSweeper
    private val jobScheduler: JobScheduler
    private val scheduledJobIndices: ScheduledJobIndices

    @Inject
    constructor(
        threadPool: ThreadPool,
        clusterService: ClusterService,
        transportService: TransportService,
        actionFilters: ActionFilters,
        jobSweeper: JobSweeper,
        jobScheduler: JobScheduler,
        scheduledJobIndices: ScheduledJobIndices
    ) : super(
        ScheduledJobsStatsAction.NAME,
        threadPool,
        clusterService,
        transportService,
        actionFilters,
        { ScheduledJobsStatsRequest(it) },
        { ScheduledJobStatusRequest(it) },
        ThreadPool.Names.MANAGEMENT,
        ScheduledJobStats::class.java
    ) {
        this.jobSweeper = jobSweeper
        this.jobScheduler = jobScheduler
        this.scheduledJobIndices = scheduledJobIndices
    }

    override fun newNodeRequest(request: ScheduledJobsStatsRequest): ScheduledJobStatusRequest {
        return ScheduledJobStatusRequest(request)
    }

    override fun newNodeResponse(si: StreamInput): ScheduledJobStats {
        return ScheduledJobStats(si)
    }

    override fun newResponse(
        request: ScheduledJobsStatsRequest,
        responses: MutableList<ScheduledJobStats>,
        failures: MutableList<FailedNodeException>
    ): ScheduledJobsStatsResponse {
        val scheduledJobEnabled = jobSweeper.isSweepingEnabled()
        val scheduledJobIndexExist = scheduledJobIndices.scheduledJobIndexExists()
        val indexHealth: ClusterIndexHealth? = if (scheduledJobIndexExist) scheduledJobIndices.scheduledJobIndexHealth() else null

        return ScheduledJobsStatsResponse(
            clusterService.clusterName,
            responses,
            failures,
            scheduledJobEnabled,
            scheduledJobIndexExist,
            indexHealth
        )
    }

    override fun nodeOperation(request: ScheduledJobStatusRequest): ScheduledJobStats {
        return createScheduledJobStatus(request.request)
    }

    private fun createScheduledJobStatus(
        scheduledJobsStatusRequest: ScheduledJobsStatsRequest
    ): ScheduledJobStats {
        val jobSweeperMetrics = jobSweeper.getJobSweeperMetrics()
        val jobSchedulerMetrics = jobScheduler.getJobSchedulerMetric()

        val status: ScheduledJobStats.ScheduleStatus = evaluateStatus(jobSchedulerMetrics, jobSweeperMetrics)
        return ScheduledJobStats(
            this.transportService.localNode,
            status,
            if (scheduledJobsStatusRequest.jobSchedulingMetrics) jobSweeperMetrics else null,
            if (scheduledJobsStatusRequest.jobsInfo) jobSchedulerMetrics.toTypedArray() else null
        )
    }

    private fun evaluateStatus(
        jobsInfo: List<JobSchedulerMetrics>,
        jobSweeperMetrics: JobSweeperMetrics
    ): ScheduledJobStats.ScheduleStatus {
        val allJobsRunningOnTime = jobsInfo.all { it.runningOnTime }
        if (allJobsRunningOnTime && jobSweeperMetrics.fullSweepOnTime) {
            return ScheduledJobStats.ScheduleStatus.GREEN
        }
        log.info("Jobs Running on time: $allJobsRunningOnTime, Sweeper on time: ${jobSweeperMetrics.fullSweepOnTime}")
        return ScheduledJobStats.ScheduleStatus.RED
    }

    class ScheduledJobStatusRequest : BaseNodeRequest {

        lateinit var request: ScheduledJobsStatsRequest

        constructor() : super()

        constructor(si: StreamInput) : super(si) {
            request = ScheduledJobsStatsRequest(si)
        }

        constructor(request: ScheduledJobsStatsRequest) : super() {
            this.request = request
        }

        @Throws(IOException::class)
        override fun writeTo(out: StreamOutput) {
            super.writeTo(out)
            request.writeTo(out)
        }
    }
}
