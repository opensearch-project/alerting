/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import com.carrotsearch.randomizedtesting.ThreadFilter
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters
import org.mockito.Mockito.mock
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.utils.scheduler.JobQueueAccountIdProvider
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.search.SearchModule
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.Client
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse
import software.amazon.awssdk.services.sqs.model.Message
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse
import software.amazon.awssdk.services.sqs.model.SqsException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

@ThreadLeakFilters(filters = [MonitorJobPollerTests.CoroutineThreadFilter::class])
class MonitorJobPollerTests : OpenSearchTestCase() {

    class CoroutineThreadFilter : ThreadFilter {
        override fun reject(t: Thread): Boolean =
            t.name.startsWith("DefaultDispatcher-worker") || t.name == "kotlinx.coroutines.DefaultExecutor"
    }

    private fun testXContentRegistry(): NamedXContentRegistry {
        return NamedXContentRegistry(
            mutableListOf(
                Monitor.XCONTENT_REGISTRY,
                SearchInput.XCONTENT_REGISTRY
            ) + SearchModule(Settings.EMPTY, emptyList()).namedXContents
        )
    }

    private fun mockClient(): Client = mock(Client::class.java)

    private fun testAccountIdProvider(accountId: String = "123"): JobQueueAccountIdProvider {
        return object : JobQueueAccountIdProvider {
            override fun getType() = "test"
            override fun initialize(settings: org.opensearch.common.settings.Settings) {}
            override fun getAccountIds() = listOf(accountId)
        }
    }

    private fun validMessageBody(): String {
        val monitorConfig = "{\"type\":\"monitor\",\"name\":\"test\"," +
            "\"monitor_type\":\"query_level_monitor\",\"enabled\":true," +
            "\"schedule\":{\"period\":{\"interval\":5,\"unit\":\"MINUTES\"}}," +
            "\"inputs\":[{\"search\":{\"indices\":[\"idx\"]," +
            "\"query\":{\"query\":{\"match_all\":{}}}}}]," +
            "\"triggers\":[],\"enabled_time\":1712750000000," +
            "\"last_update_time\":1712750000000}"
        val escaped = monitorConfig.replace("\"", "\\\"")
        return "{\"job_start_time\":\"2026-04-10T10:05:00Z\"," +
            "\"monitorId\":\"abc\",\"monitorConfig\":\"$escaped\"}"
    }

    private fun createPoller(
        sqsClient: FakeSqsClient = FakeSqsClient(),
        enabled: Boolean = true
    ): MonitorJobPoller {
        return MonitorJobPoller(
            testXContentRegistry(), mockClient(), enabled,
            testAccountIdProvider(), "us-west-2", "test-queue"
        ).also { it.sqsClient = sqsClient }
    }

    /**
     * Fake SqsClient for testing — avoids Mockito issues with Java 21 module system.
     */
    private class FakeSqsClient(
        private val onReceive: (ReceiveMessageRequest) -> ReceiveMessageResponse = {
            ReceiveMessageResponse.builder().messages(emptyList()).build()
        },
        private val onDelete: (DeleteMessageRequest) -> DeleteMessageResponse = {
            DeleteMessageResponse.builder().build()
        }
    ) : SqsClient {
        val closed = AtomicBoolean(false)
        override fun serviceName(): String = "sqs"
        override fun close() { closed.set(true) }
        override fun receiveMessage(req: ReceiveMessageRequest): ReceiveMessageResponse = onReceive(req)
        override fun deleteMessage(req: DeleteMessageRequest): DeleteMessageResponse = onDelete(req)
    }

    fun `test start creates threads when enabled and stop interrupts them`() {
        val sqsClient = FakeSqsClient()
        val poller = MonitorJobPoller(
            testXContentRegistry(), mockClient(), true,
            testAccountIdProvider(), "us-west-2", "test-queue"
        ).also { it.sqsClient = sqsClient }
        poller.start()
        Thread.sleep(100)
        poller.stop()
        poller.close()
    }

    fun `test start does not create threads when disabled`() {
        val latch = CountDownLatch(1)
        val sqsClient = FakeSqsClient(
            onReceive = {
                latch.countDown()
                ReceiveMessageResponse.builder().messages(emptyList()).build()
            }
        )
        val poller = MonitorJobPoller(
            testXContentRegistry(), mockClient(), false,
            null, "", ""
        )
        poller.start()
        // Should NOT poll since disabled
        assertFalse("Should not have polled", latch.await(500, TimeUnit.MILLISECONDS))
        poller.stop()
        poller.close()
    }

    fun `test start throws when provider not set`() {
        val poller = MonitorJobPoller(
            testXContentRegistry(), mockClient(), true,
            null, "us-west-2", "test-queue"
        )
        expectThrows(Exception::class.java) {
            poller.start()
        }
        poller.close()
    }

    fun `test worker polls when provider is set`() {
        val latch = CountDownLatch(1)
        val sqsClient = FakeSqsClient(
            onReceive = {
                latch.countDown()
                ReceiveMessageResponse.builder().messages(emptyList()).build()
            }
        )
        val poller = createPoller(sqsClient)
        poller.start()
        assertTrue("Should have polled", latch.await(5, TimeUnit.SECONDS))
        poller.stop()
        poller.close()
    }

    fun `test worker continues on provider exception`() {
        var callCount = 0
        val latch = CountDownLatch(2)
        val errorProvider = object : JobQueueAccountIdProvider {
            override fun getType() = "test"
            override fun initialize(settings: org.opensearch.common.settings.Settings) {}
            override fun getAccountIds(): List<String> {
                callCount++
                latch.countDown()
                if (callCount == 1) throw RuntimeException("test error")
                return listOf("123")
            }
        }
        val poller = MonitorJobPoller(
            testXContentRegistry(), mockClient(), true,
            errorProvider, "us-west-2", "test-queue"
        ).also { it.sqsClient = FakeSqsClient() }
        poller.start()
        assertTrue("Worker should have polled twice", latch.await(5, TimeUnit.SECONDS))
        poller.stop()
        poller.close()
    }

    fun `test worker handles empty account ids`() {
        val latch = CountDownLatch(3)
        val emptyProvider = object : JobQueueAccountIdProvider {
            override fun getType() = "test"
            override fun initialize(settings: org.opensearch.common.settings.Settings) {}
            override fun getAccountIds(): List<String> {
                latch.countDown()
                return emptyList()
            }
        }
        val poller = MonitorJobPoller(
            testXContentRegistry(), mockClient(), true,
            emptyProvider, "us-west-2", "test-queue"
        ).also { it.sqsClient = FakeSqsClient() }
        poller.start()
        assertTrue("Worker should have polled multiple times", latch.await(5, TimeUnit.SECONDS))
        poller.stop()
        poller.close()
    }

    fun `test receive message with correct parameters`() {
        val receivedRequest = AtomicReference<ReceiveMessageRequest>()
        val latch = CountDownLatch(1)
        val delivered = AtomicBoolean(false)
        val sqsClient = FakeSqsClient(
            onReceive = { req ->
                if (!delivered.getAndSet(true)) {
                    receivedRequest.set(req)
                    latch.countDown()
                    ReceiveMessageResponse.builder().messages(
                        Message.builder().messageId("msg-1")
                            .receiptHandle("r-1")
                            .body(validMessageBody()).build()
                    ).build()
                } else {
                    ReceiveMessageResponse.builder().messages(emptyList()).build()
                }
            }
        )
        val poller = createPoller(sqsClient)
        poller.start()
        assertTrue("Should have received", latch.await(5, TimeUnit.SECONDS))
        poller.stop()
        poller.close()

        val req = receivedRequest.get()
        assertEquals(1, req.maxNumberOfMessages())
        assertEquals(0, req.waitTimeSeconds())
        assertEquals(
            "https://sqs.us-west-2.amazonaws.com/123/test-queue",
            req.queueUrl()
        )
    }

    fun `test delete message uses receipt handle`() {
        val deletedRequest = AtomicReference<DeleteMessageRequest>()
        val sqsClient = FakeSqsClient(
            onDelete = { req ->
                deletedRequest.set(req)
                DeleteMessageResponse.builder().build()
            }
        )
        val poller = createPoller(sqsClient)
        val message = Message.builder()
            .messageId("msg-1")
            .receiptHandle("receipt-handle-abc")
            .body(validMessageBody())
            .build()

        poller.deleteMessage(
            sqsClient,
            "https://sqs.us-west-2.amazonaws.com/123/test-queue",
            message
        )

        assertEquals("receipt-handle-abc", deletedRequest.get().receiptHandle())
        assertEquals(
            "https://sqs.us-west-2.amazonaws.com/123/test-queue",
            deletedRequest.get().queueUrl()
        )
        poller.close()
    }

    fun `test sqs exception during receive does not kill worker`() {
        val callCount = AtomicInteger(0)
        val latch = CountDownLatch(2)
        val sqsClient = FakeSqsClient(
            onReceive = {
                val count = callCount.incrementAndGet()
                latch.countDown()
                if (count == 1) {
                    throw SqsException.builder().message("throttled").build()
                }
                ReceiveMessageResponse.builder().messages(emptyList()).build()
            }
        )
        val poller = createPoller(sqsClient)
        poller.start()
        assertTrue("Worker should survive SQS exception", latch.await(5, TimeUnit.SECONDS))
        poller.stop()
        poller.close()
    }

    fun `test empty receive does not call delete`() {
        val deleteCount = AtomicInteger(0)
        val receiveLatch = CountDownLatch(3)
        val sqsClient = FakeSqsClient(
            onReceive = {
                receiveLatch.countDown()
                ReceiveMessageResponse.builder().messages(emptyList()).build()
            },
            onDelete = {
                deleteCount.incrementAndGet()
                DeleteMessageResponse.builder().build()
            }
        )
        val poller = createPoller(sqsClient)
        poller.start()
        assertTrue("Should poll multiple times", receiveLatch.await(5, TimeUnit.SECONDS))
        poller.stop()
        poller.close()
        assertEquals("Delete should not be called", 0, deleteCount.get())
    }

    fun `test parseMessage extracts monitor and jobStartTime`() {
        val poller = createPoller()

        val monitorConfig = "{\"type\":\"monitor\",\"name\":\"test-monitor\"," +
            "\"monitor_type\":\"query_level_monitor\",\"enabled\":true," +
            "\"schedule\":{\"period\":{\"interval\":5,\"unit\":\"MINUTES\"}}," +
            "\"inputs\":[{\"search\":{\"indices\":[\"my-index\"]," +
            "\"query\":{\"query\":{\"match_all\":{}}}}}]," +
            "\"triggers\":[],\"enabled_time\":1712750000000," +
            "\"last_update_time\":1712750000000}"
        val escaped = monitorConfig.replace("\"", "\\\"")
        val body = "{\"job_start_time\":\"2026-04-10T10:05:00Z\"," +
            "\"monitorId\":\"abc123\",\"monitorConfig\":\"$escaped\"}"

        val payload = poller.parseMessage(body)
        val monitor = payload.toMonitor(testXContentRegistry())

        assertEquals("abc123", payload.monitorId)
        assertEquals("2026-04-10T10:05:00Z", payload.jobStartTime)
        assertEquals("test-monitor", monitor.name)
        assertEquals("query_level_monitor", monitor.monitorType)
        poller.close()
    }

    fun `test parseMessage throws on missing monitorConfig`() {
        val poller = createPoller()
        val body = "{\"job_start_time\":\"2026-04-10T10:05:00Z\"," +
            "\"monitorId\":\"abc123\"}"

        expectThrows(Exception::class.java) {
            poller.parseMessage(body)
        }
        poller.close()
    }
}
