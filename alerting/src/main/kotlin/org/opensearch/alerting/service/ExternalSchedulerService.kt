/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import org.apache.logging.log4j.LogManager
import org.opensearch.commons.alerting.util.ScheduleTranslator
import org.opensearch.commons.alerting.model.Monitor
import java.time.ZoneId

/**
 * Manages external EventBridge schedules for monitor execution.
 *
 * Called from TransportIndexMonitorAction (create/update) and
 * TransportDeleteMonitorAction (delete) during monitor CRUD.
 */
object ExternalSchedulerService {

    private val log = LogManager.getLogger(ExternalSchedulerService::class.java)

    const val SCHEDULE_NAME_PREFIX = "monitor-"
    const val SCHEDULER_ACCOUNT_ID_KEY = "scheduler.account_id"
    const val SCHEDULER_QUEUE_ARN_KEY = "scheduler.queue_arn"
    const val SCHEDULER_ROLE_ARN_KEY = "scheduler.role_arn"

    fun scheduleName(monitorId: String): String = "$SCHEDULE_NAME_PREFIX$monitorId"

    /**
     * Creates an EventBridge schedule for a newly created monitor.
     */
    fun createSchedule(
        monitor: Monitor,
        schedulerAccountId: String,
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
            schedulerAccountId = schedulerAccountId
        )
        log.info("Creating EB schedule ${request.name} in account $schedulerAccountId")
        // TODO: SchedulerClient.createSchedule() with AssumeRole into scheduler account
    }

    /**
     * Updates an EventBridge schedule. Always refreshes Target.Input with latest config.
     */
    fun updateSchedule(
        monitor: Monitor,
        schedulerAccountId: String,
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
            schedulerAccountId = schedulerAccountId
        )
        log.info("Updating EB schedule ${request.name} in account $schedulerAccountId")
        // TODO: SchedulerClient.updateSchedule() with AssumeRole into scheduler account
    }

    /**
     * Deletes an EventBridge schedule. Idempotent — if not found, proceeds silently.
     */
    fun deleteSchedule(
        monitorId: String,
        schedulerAccountId: String,
        crossAccountRoleArn: String
    ) {
        log.info("Deleting EB schedule ${scheduleName(monitorId)} from account $schedulerAccountId")
        // TODO: SchedulerClient.deleteSchedule() with AssumeRole into scheduler account
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
        val schedulerAccountId: String
    )
}
