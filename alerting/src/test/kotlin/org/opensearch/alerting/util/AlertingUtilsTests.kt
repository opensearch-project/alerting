/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.alerting.destination.message.BaseMessage
import org.opensearch.alerting.destination.message.CustomWebhookMessage
import org.opensearch.test.OpenSearchTestCase
import java.util.HashMap

class AlertingUtilsTests : OpenSearchTestCase() {

    private val HOST_DENY_LIST = listOf(
        "127.0.0.0/8",
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "0.0.0.0/8",
        "9.9.9.9" // ip
    )

    fun `test ips in denylist`() {
        val ips = listOf(
            "127.0.0.1", // 127.0.0.0/8
            "10.0.0.1", // 10.0.0.0/8
            "10.11.12.13", // 10.0.0.0/8
            "172.16.0.1", // "172.16.0.0/12"
            "192.168.0.1", // 192.168.0.0/16"
            "0.0.0.1", // 0.0.0.0/8
            "9.9.9.9"
        )
        for (ip in ips) {
            val bm = createMessageWithHost(ip)
            assertEquals(true, bm.isHostInDenylist(HOST_DENY_LIST))
        }
    }

    fun `test url in denylist`() {
        val urls = listOf("https://www.amazon.com", "https://mytest.com", "https://mytest.com")
        for (url in urls) {
            val bm = createMessageWithURl(url)
            assertEquals(false, bm.isHostInDenylist(HOST_DENY_LIST))
        }
    }

    private fun createMessageWithHost(host: String): BaseMessage {
        return CustomWebhookMessage.Builder("abc")
            .withHost(host)
            .withPath("incomingwebhooks/383c0e2b-d028-44f4-8d38-696754bc4574")
            .withMessage("{\"Content\":\"Message test\"}")
            .withMethod("POST")
            .withQueryParams(HashMap<String, String>()).build()
    }

    private fun createMessageWithURl(url: String): BaseMessage {
        return CustomWebhookMessage.Builder("abc")
            .withUrl(url)
            .withPath("incomingwebhooks/383c0e2b-d028-44f4-8d38-696754bc4574")
            .withMessage("{\"Content\":\"Message test\"}")
            .withMethod("POST")
            .withQueryParams(HashMap<String, String>()).build()
    }
}
