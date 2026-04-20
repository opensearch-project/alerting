/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import org.apache.logging.log4j.LogManager
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.util.ScheduleTranslator

/**
 * Manages external EventBridge schedules for monitor execution.
 *
 * Called from TransportIndexMonitorAction (create/update) and
 * TransportDeleteMonitorAction (delete) during monitor CRUD.
 */
object ExternalSchedulerService {

    private val log = LogManager.getLogger(ExternalSchedulerService::class.java)

    const val SCHEDULE_NAME_PREFIX = "monitor-"

    /**
     * Transient ThreadContext key used to override the default `account_id` plugin setting
     * on a per-request basis. Other routing fields (queue_arn, role_arn) come from plugin
     * settings only and are not overridable via ThreadContext.
     */
    const val SCHEDULER_ACCOUNT_ID_KEY = "scheduler.account_id"

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
        log.info(
            "Creating EB schedule ${scheduleName(monitor.id)} in account $schedulerAccountId " +
                "expr=$scheduleExpression tz=$timezone enabled=${monitor.enabled} " +
                "queue=$queueArn role=$crossAccountRoleArn inputBytes=${targetInput.length}"
        )
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
        log.info(
            "Updating EB schedule ${scheduleName(monitor.id)} in account $schedulerAccountId " +
                "expr=$scheduleExpression tz=$timezone enabled=${monitor.enabled} " +
                "queue=$queueArn role=$crossAccountRoleArn inputBytes=${targetInput.length}"
        )
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
        log.info("Deleting EB schedule ${scheduleName(monitorId)} from account $schedulerAccountId role=$crossAccountRoleArn")
        // TODO: SchedulerClient.deleteSchedule() with AssumeRole into scheduler account
    }
}
