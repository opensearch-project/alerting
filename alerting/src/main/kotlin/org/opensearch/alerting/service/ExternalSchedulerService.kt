/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.AlertingSettings.Companion.REMOTE_METADATA_REGION
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.util.ScheduleTranslator
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.scheduler.SchedulerClient
import software.amazon.awssdk.services.scheduler.model.ActionAfterCompletion
import software.amazon.awssdk.services.scheduler.model.FlexibleTimeWindow
import software.amazon.awssdk.services.scheduler.model.FlexibleTimeWindowMode
import software.amazon.awssdk.services.scheduler.model.ResourceNotFoundException
import software.amazon.awssdk.services.scheduler.model.ScheduleState
import software.amazon.awssdk.services.scheduler.model.Target
import java.util.concurrent.ConcurrentHashMap

/**
 * Manages external EventBridge schedules for monitor execution.
 *
 * Called from TransportIndexMonitorAction (create/update) and
 * TransportDeleteMonitorAction (delete) during monitor CRUD.
 *
 * [credentialsCache] must be set before any schedule operations are invoked.
 */
object ExternalSchedulerService {

    private val log = LogManager.getLogger(ExternalSchedulerService::class.java)

    const val SCHEDULE_NAME_PREFIX = "monitor-"

    /** EventBridge Scheduler context variable replaced at invocation time with the scheduled time. */
    const val EB_SCHEDULED_TIME_PLACEHOLDER = "<aws.scheduler.scheduled-time>"

    /**
     * Transient ThreadContext key used to override the default `account_id` plugin setting
     * on a per-request basis. Other routing fields (queue_name, role_arn) come from plugin
     * settings only and are not overridable via ThreadContext.
     */
    const val SCHEDULER_ACCOUNT_ID_KEY = "x-scheduler-account-id"

    @Volatile
    var credentialsCache: AssumeRoleCredentialsCache? = null

    private var region: String? = null
    private var messageGroupKeyName: String = ""

    private val schedulerClients = ConcurrentHashMap<String, SchedulerClient>()

    fun initialize(settings: Settings) {
        region = AlertingSettings.REMOTE_METADATA_REGION.get(settings)
        messageGroupKeyName = AlertingSettings.JOB_QUEUE_MESSAGE_GROUP_KEY_NAME.get(settings) ?: ""
    }

    fun scheduleName(monitorId: String): String = "$SCHEDULE_NAME_PREFIX$monitorId"

    /**
     * Creates an EventBridge schedule for a newly created monitor.
     */
    fun createSchedule(monitor: Monitor, routing: SchedulerRoutingResolver.Routing, targetInput: String) {
        val (scheduleExpression, timezone) = ScheduleTranslator.toEventBridgeExpression(monitor.schedule)
        val name = scheduleName(monitor.id)
        val queueUrl = buildQueueUrl(routing)
        log.info(
            "Creating EB schedule $name in account ${routing.accountId} " +
                "expr=$scheduleExpression tz=$timezone enabled=${monitor.enabled} " +
                "queue=$queueUrl role=${routing.roleArn} inputBytes=${targetInput.length}"
        )
        val client = getSchedulerClient(routing)
        client.createSchedule {
            it.name(name)
                .scheduleExpression(scheduleExpression)
                .scheduleExpressionTimezone(timezone?.toString() ?: "UTC")
                .state(if (monitor.enabled) ScheduleState.ENABLED else ScheduleState.DISABLED)
                .actionAfterCompletion(ActionAfterCompletion.NONE)
                .flexibleTimeWindow(FlexibleTimeWindow.builder().mode(FlexibleTimeWindowMode.OFF).build())
                .target(buildTarget(queueUrl, routing, targetInput, monitor))
        }
    }

    /**
     * Updates an EventBridge schedule. Always refreshes Target.Input with latest config.
     */
    fun updateSchedule(monitor: Monitor, routing: SchedulerRoutingResolver.Routing, targetInput: String) {
        val (scheduleExpression, timezone) = ScheduleTranslator.toEventBridgeExpression(monitor.schedule)
        val name = scheduleName(monitor.id)
        val queueUrl = buildQueueUrl(routing)
        log.info(
            "Updating EB schedule $name in account ${routing.accountId} " +
                "expr=$scheduleExpression tz=$timezone enabled=${monitor.enabled} " +
                "queue=$queueUrl role=${routing.roleArn} inputBytes=${targetInput.length}"
        )
        val client = getSchedulerClient(routing)
        client.updateSchedule {
            it.name(name)
                .scheduleExpression(scheduleExpression)
                .scheduleExpressionTimezone(timezone?.toString() ?: "UTC")
                .state(if (monitor.enabled) ScheduleState.ENABLED else ScheduleState.DISABLED)
                .actionAfterCompletion(ActionAfterCompletion.NONE)
                .flexibleTimeWindow(FlexibleTimeWindow.builder().mode(FlexibleTimeWindowMode.OFF).build())
                .target(buildTarget(queueUrl, routing, targetInput, monitor))
        }
    }

    /**
     * Deletes an EventBridge schedule. Idempotent — if not found, proceeds silently.
     */
    fun deleteSchedule(monitorId: String, routing: SchedulerRoutingResolver.Routing) {
        val name = scheduleName(monitorId)
        log.info("Deleting EB schedule $name from account ${routing.accountId} role=${routing.roleArn}")
        val client = getSchedulerClient(routing)
        try {
            client.deleteSchedule { it.name(name) }
        } catch (e: ResourceNotFoundException) {
            log.info("Schedule $name not found in account ${routing.accountId}, nothing to delete")
        }
    }

    /** Universal target ARN for SQS SendMessage via EventBridge Scheduler. */
    const val EB_SQS_UNIVERSAL_TARGET_ARN = "arn:aws:scheduler:::aws-sdk:sqs:sendMessage"

    private fun buildTarget(
        queueUrl: String,
        routing: SchedulerRoutingResolver.Routing,
        targetInput: String,
        monitor: Monitor
    ): Target {
        val universalInput = buildUniversalInput(queueUrl, targetInput, monitor)
        return Target.builder()
            .arn(EB_SQS_UNIVERSAL_TARGET_ARN)
            .roleArn(routing.roleArn)
            .input(universalInput)
            .build()
    }

    /**
     * Builds the Input JSON for the universal target `sqs:sendMessage`.
     * Includes MessageGroupId from monitor metadata when configured, enabling
     * SQS fair queuing on standard queues.
     */
    private fun buildUniversalInput(queueUrl: String, messageBody: String, monitor: Monitor): String {
        val escaped = messageBody.replace("\\", "\\\\").replace("\"", "\\\"")
        val sb = StringBuilder()
        sb.append("{\"QueueUrl\":\"").append(queueUrl).append("\",")
        sb.append("\"MessageBody\":\"").append(escaped).append("\"")
        val keyName = messageGroupKeyName
        if (keyName.isNotEmpty()) {
            val groupId = monitor.metadata?.get(keyName)
            if (!groupId.isNullOrEmpty()) {
                sb.append(",\"MessageGroupId\":\"").append(groupId).append("\"")
            }
        }
        sb.append("}")
        return sb.toString()
    }
    private fun buildQueueUrl(routing: SchedulerRoutingResolver.Routing): String {
        val resolvedRegion = requireNotNull(region) { "region must be set" }
        return "https://sqs.$resolvedRegion.amazonaws.com/${routing.accountId}/${routing.queueName}"
    }

    private fun getSchedulerClient(routing: SchedulerRoutingResolver.Routing): SchedulerClient {
        val cache = requireNotNull(credentialsCache) { "credentialsCache must be set before invoking schedule operations" }
        val resolvedRegion = requireNotNull(region) { "region must be set" }
        return schedulerClients.computeIfAbsent(routing.accountId) {
            SchedulerClient.builder()
                .region(Region.of(resolvedRegion))
                .credentialsProvider(cache.getCredentialsProvider(routing.accountId))
                .build()
        }
    }
}
