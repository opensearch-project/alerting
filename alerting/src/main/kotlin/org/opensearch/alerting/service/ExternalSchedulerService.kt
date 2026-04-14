/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.util.ScheduleTranslator
import org.opensearch.commons.alerting.model.Monitor
import java.time.ZoneId

/**
 * Manages external EventBridge schedules for OASIS monitor execution.
 *
 * Called from TransportIndexMonitorAction (create/update) and
 * TransportDeleteMonitorAction (delete) during monitor CRUD.
 *
 * AWS SDK SchedulerClient calls will be wired in OSSA-608.
 */
object ExternalSchedulerService {

    private val log = LogManager.getLogger(ExternalSchedulerService::class.java)

    const val SCHEDULE_NAME_PREFIX = "monitor-"
    const val EB_CELL_ACCOUNT_ID_KEY = "oasis.eb_cell_account_id"
    const val EB_CELL_REGION_KEY = "oasis.eb_cell_region"
    const val EB_CELL_QUEUE_ARN_KEY = "oasis.eb_cell_queue_arn"
    const val EB_CELL_ROLE_ARN_KEY = "oasis.eb_cell_role_arn"

    fun scheduleName(monitorId: String): String = "$SCHEDULE_NAME_PREFIX$monitorId"

    /**
     * Creates an EventBridge schedule for a newly created monitor.
     */
    fun createSchedule(
        monitor: Monitor,
        ebCellAccountId: String,
        ebCellRegion: String,
        queueArn: String,
        crossAccountRoleArn: String,
        targetInput: String
    ) {
        val (scheduleExpression, timezone) = ScheduleTranslator.toEventBridgeExpression(monitor.schedule)
        val request = ScheduleRequest(
            name = scheduleName(monitor.id),
            scheduleExpression = scheduleExpression,
            timezone = timezone,
            targetArn = queueArn,
            targetRoleArn = crossAccountRoleArn,
            targetInput = targetInput,
            enabled = monitor.enabled,
            ebCellAccountId = ebCellAccountId,
            ebCellRegion = ebCellRegion
        )
        log.info("Creating EB schedule ${request.name} in cell $ebCellAccountId")
        // TODO OSSA-608: SchedulerClient.createSchedule() with AssumeRole into EB cell account
    }

    /**
     * Updates an EventBridge schedule. Always refreshes Target.Input with latest config.
     */
    fun updateSchedule(
        monitor: Monitor,
        ebCellAccountId: String,
        ebCellRegion: String,
        queueArn: String,
        crossAccountRoleArn: String,
        targetInput: String
    ) {
        val (scheduleExpression, timezone) = ScheduleTranslator.toEventBridgeExpression(monitor.schedule)
        val request = ScheduleRequest(
            name = scheduleName(monitor.id),
            scheduleExpression = scheduleExpression,
            timezone = timezone,
            targetArn = queueArn,
            targetRoleArn = crossAccountRoleArn,
            targetInput = targetInput,
            enabled = monitor.enabled,
            ebCellAccountId = ebCellAccountId,
            ebCellRegion = ebCellRegion
        )
        log.info("Updating EB schedule ${request.name} in cell $ebCellAccountId")
        // TODO OSSA-608: SchedulerClient.updateSchedule() with AssumeRole into EB cell account
    }

    /**
     * Deletes an EventBridge schedule. Idempotent — if not found, proceeds silently.
     */
    fun deleteSchedule(
        monitorId: String,
        ebCellAccountId: String,
        ebCellRegion: String,
        crossAccountRoleArn: String
    ) {
        log.info("Deleting EB schedule ${scheduleName(monitorId)} from cell $ebCellAccountId")
        // TODO OSSA-608: SchedulerClient.deleteSchedule() with AssumeRole into EB cell account
    }

    /**
     * Data class carrying all info needed to create/update an EB schedule.
     */
    data class ScheduleRequest(
        val name: String,
        val scheduleExpression: String,
        val timezone: ZoneId?,
        val targetArn: String,
        val targetRoleArn: String,
        val targetInput: String,
        val enabled: Boolean,
        val ebCellAccountId: String,
        val ebCellRegion: String
    )
}
