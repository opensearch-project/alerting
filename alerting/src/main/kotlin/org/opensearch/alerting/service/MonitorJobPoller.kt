/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.action.ExecuteMonitorAction
import org.opensearch.alerting.action.ExecuteMonitorRequest
import org.opensearch.alerting.action.ExecuteMonitorResponse
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.common.lifecycle.AbstractLifecycleComponent
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduleJobPayload
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.utils.scheduler.JobQueueAccountIdProvider
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.transport.client.Client
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest
import software.amazon.awssdk.services.sqs.model.Message
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/**
 * Polls SQS queues for monitor execution messages and dispatches them
 * to TransportExecuteMonitorAction. Runs a fixed number of coroutines
 * that round-robin across queue URLs constructed from account IDs, region,
 * and queue name.
 *
 * Coroutines are only launched when [enabled] is true (multi-tenant deployment mode).
 * [sqsClient] must be set before workers begin polling.
 */
class MonitorJobPoller(
    private val xContentRegistry: NamedXContentRegistry,
    private val client: Client,
    private val enabled: Boolean,
    private val accountIdProvider: JobQueueAccountIdProvider?,
    private val region: String,
    private val queueName: String
) : AbstractLifecycleComponent() {

    private val logger = LogManager.getLogger(MonitorJobPoller::class.java)
    private val supervisorJob = SupervisorJob()
    private val scope = CoroutineScope(Dispatchers.IO + supervisorJob)

    @Volatile
    var sqsClient: SqsClient? = null

    override fun doStart() {
        if (!enabled) {
            logger.info("MonitorJobPoller disabled, not starting poll workers")
            return
        }
        val provider = requireNotNull(accountIdProvider) { "accountIdProvider must be set before starting" }
        val sqs = requireNotNull(sqsClient) { "sqsClient must be set before starting" }

        logger.info("Starting MonitorJobPoller with $POLLER_THREAD_COUNT workers")
        repeat(POLLER_THREAD_COUNT) { scope.launch { pollLoop(provider, sqs, region, queueName) } }
    }

    override fun doStop() {
        logger.info("Stopping MonitorJobPoller")
        supervisorJob.cancel()
    }

    override fun doClose() {}

    private suspend fun pollLoop(
        provider: JobQueueAccountIdProvider,
        sqs: SqsClient,
        region: String,
        queueName: String
    ) {
        val queueIndex = AtomicInteger(0)
        var cachedQueueUrls: List<String> = emptyList()
        var cachedAccountIds: List<String> = emptyList()

        while (scope.isActive) {
            try {
                val accountIds = provider.getAccountIds()
                if (accountIds.isEmpty()) continue

                if (accountIds != cachedAccountIds) {
                    cachedAccountIds = accountIds
                    cachedQueueUrls = accountIds.map { "https://sqs.$region.amazonaws.com/$it/$queueName" }
                }

                val queueUrl = cachedQueueUrls[queueIndex.getAndIncrement() % cachedQueueUrls.size]

                val messages = receiveMessages(sqs, queueUrl)
                if (messages.isEmpty()) continue

                val message = messages[0]
                try {
                    logger.info(
                        "Received message {} from queue {}",
                        message.messageId(), queueUrl
                    )
                    val payload = parseMessage(message.body())
                    val monitor = payload.toMonitor(xContentRegistry)
                    val jobStartTime = Instant.parse(payload.jobStartTime)
                    logger.info(
                        "Parsed monitor [{}] type [{}] jobStartTime [{}]",
                        monitor.id, monitor.monitorType, jobStartTime
                    )
                    executeMonitor(monitor, jobStartTime)
                    deleteMessage(sqs, queueUrl, message)
                } catch (e: Exception) {
                    logger.error(
                        "Failed to process job queue message {} from queue {}",
                        message.messageId(), queueUrl, e
                    )
                    // Don't delete — visibility timeout expires, SQS redelivers
                }
            } catch (e: Exception) {
                logger.error("Error in MonitorJobPoller worker", e)
            }
        }
    }

    private suspend fun executeMonitor(monitor: Monitor, jobStartTime: Instant) {
        val request = ExecuteMonitorRequest(
            dryrun = false,
            requestEnd = TimeValue(jobStartTime.toEpochMilli()),
            monitorId = monitor.id,
            monitor = monitor,
            requestStart = null
        )
        try {
            client.suspendUntil<Client, ExecuteMonitorResponse> {
                client.execute(ExecuteMonitorAction.INSTANCE, request, it)
            }
        } catch (e: Exception) {
            throw AlertingException.wrap(e)
        }
    }

    internal fun receiveMessages(sqs: SqsClient, queueUrl: String): List<Message> {
        val request = ReceiveMessageRequest.builder()
            .queueUrl(queueUrl)
            .maxNumberOfMessages(1)
            .waitTimeSeconds(0)
            .build()
        return sqs.receiveMessage(request).messages()
    }

    internal fun deleteMessage(sqs: SqsClient, queueUrl: String, message: Message) {
        val request = DeleteMessageRequest.builder()
            .queueUrl(queueUrl)
            .receiptHandle(message.receiptHandle())
            .build()
        sqs.deleteMessage(request)
    }

    internal fun parseMessage(body: String): ScheduleJobPayload {
        try {
            return XContentType.JSON.xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, body)
                .use { parser ->
                    parser.nextToken()
                    ScheduleJobPayload.parse(parser)
                }
        } catch (e: Exception) {
            throw AlertingException.wrap(e)
        }
    }

    companion object {
        const val POLLER_THREAD_COUNT = 10
    }
}
