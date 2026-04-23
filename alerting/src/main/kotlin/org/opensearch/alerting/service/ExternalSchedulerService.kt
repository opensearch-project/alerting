/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import org.apache.logging.log4j.LogManager
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.util.ScheduleTranslator
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.scheduler.SchedulerClient
import software.amazon.awssdk.services.scheduler.model.ActionAfterCompletion
import software.amazon.awssdk.services.scheduler.model.FlexibleTimeWindow
import software.amazon.awssdk.services.scheduler.model.FlexibleTimeWindowMode
import software.amazon.awssdk.services.scheduler.model.ResourceNotFoundException
import software.amazon.awssdk.services.scheduler.model.ScheduleState
import software.amazon.awssdk.services.scheduler.model.Target
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

/**
 * Manages external EventBridge schedules for monitor execution.
 *
 * Called from TransportIndexMonitorAction (create/update) and
 * TransportDeleteMonitorAction (delete) during monitor CRUD.
 *
 * [stsClient] must be set before any schedule operations are invoked.
 * It is not created in the open-source plugin — the closed-source layer injects it.
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
    var stsClient: StsClient? = null

    @Volatile
    var region: String? = null

    var messageGroupKeyName: String = ""

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
        withSchedulerClient(routing) { client ->
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
        withSchedulerClient(routing) { client ->
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
    }

    /**
     * Deletes an EventBridge schedule. Idempotent — if not found, proceeds silently.
     */
    fun deleteSchedule(monitorId: String, routing: SchedulerRoutingResolver.Routing) {
        val name = scheduleName(monitorId)
        log.info("Deleting EB schedule $name from account ${routing.accountId} role=${routing.roleArn}")
        withSchedulerClient(routing) { client ->
            try {
                client.deleteSchedule { it.name(name) }
            } catch (e: ResourceNotFoundException) {
                log.info("Schedule $name not found in account ${routing.accountId}, nothing to delete")
            }
        }
    }

    /** Universal target ARN for SQS SendMessage via EventBridge Scheduler. */
    const val SQS_SEND_MESSAGE_ARN = "arn:aws:scheduler:::aws-sdk:sqs:sendMessage"

    private fun buildTarget(
        queueUrl: String,
        routing: SchedulerRoutingResolver.Routing,
        targetInput: String,
        monitor: Monitor
    ): Target {
        val universalInput = buildUniversalInput(queueUrl, targetInput, monitor)
        return Target.builder()
            .arn(SQS_SEND_MESSAGE_ARN)
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

    private fun <T> withSchedulerClient(routing: SchedulerRoutingResolver.Routing, block: (SchedulerClient) -> T): T {
        val sts = requireNotNull(stsClient) { "stsClient must be set before invoking schedule operations" }
        val resolvedRegion = requireNotNull(region) { "region must be set" }
        val assumeRoleResponse = sts.assumeRole(
            AssumeRoleRequest.builder()
                .roleArn(routing.roleArn)
                .roleSessionName("alerting-scheduler-${routing.accountId}")
                .build()
        )
        val credentials = assumeRoleResponse.credentials()
        val schedulerClient = SchedulerClient.builder()
            .region(Region.of(resolvedRegion))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(
                        credentials.accessKeyId(),
                        credentials.secretAccessKey(),
                        credentials.sessionToken()
                    )
                )
            )
            .build()
        return schedulerClient.use(block)
    }
}
