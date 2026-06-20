/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.opensearch.alerting.ALERTING_BASE_URI
import org.opensearch.alerting.ALERTING_FULL_ACCESS_ROLE
import org.opensearch.alerting.ALL_ACCESS_ROLE
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.alerting.randomQueryLevelTrigger
import org.opensearch.client.Request
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.core.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging

/**
 * Integration tests for Resource Sharing feature with Alerting plugin.
 * These tests only run when both security and resource_sharing are enabled.
 */
@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class SecureResourceSharingMonitorRestApiIT : AlertingRestTestCase() {

    companion object {
        @BeforeClass
        @JvmStatic
        fun setup() {
            org.junit.Assume.assumeTrue(System.getProperty("security", "false")!!.toBoolean())
            org.junit.Assume.assumeTrue(System.getProperty("resource_sharing.enabled", "false")!!.toBoolean())
        }
    }

    private val aliceUser = "rs_alice"
    private val bobUser = "rs_bob"
    private var aliceClient: RestClient? = null
    private var bobClient: RestClient? = null

    @Before
    fun setupUsers() {
        if (aliceClient != null) return

        createUser(aliceUser, arrayOf("engineering"))
        createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf(aliceUser))
        createUserRolesMapping(ALL_ACCESS_ROLE, arrayOf(aliceUser))
        aliceClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), aliceUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        createUser(bobUser, arrayOf("marketing"))
        createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf(bobUser))
        createUserRolesMapping(ALL_ACCESS_ROLE, arrayOf(bobUser))
        bobClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), bobUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()
    }

    @After
    fun cleanupClients() {
        aliceClient?.close()
        bobClient?.close()
        aliceClient = null
        bobClient = null
        deleteUser(aliceUser)
        deleteUser(bobUser)
    }

    fun `test monitor created by alice is not visible to bob`() {
        val monitor = randomQueryLevelMonitor(
            triggers = listOf(randomQueryLevelTrigger())
        )
        val createdMonitor = createMonitorWithClient(aliceClient!!, monitor)
        val monitorId = createdMonitor.id

        // Bob should NOT be able to get Alice's monitor
        val exception = expectThrows(ResponseException::class.java) {
            bobClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId")
        }
        assertTrue(
            exception.message!!.contains("no permissions") || exception.response.statusLine.statusCode == 403
        )
    }

    fun `test monitor created by alice is visible after sharing`() {
        val monitor = randomQueryLevelMonitor(
            triggers = listOf(randomQueryLevelTrigger())
        )
        val createdMonitor = createMonitorWithClient(aliceClient!!, monitor)
        val monitorId = createdMonitor.id

        // Share with bob via resource sharing API
        shareResource(aliceClient!!, monitorId, "monitor", "alerting_read_only", bobUser)

        // Wait for sharing to propagate
        Thread.sleep(2000)

        // Bob should now be able to get the monitor
        val response = bobClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId")
        assertEquals(RestStatus.OK.status, response.statusLine.statusCode)
    }

    fun `test bob cannot share alice monitor without full access`() {
        val monitor = randomQueryLevelMonitor(
            triggers = listOf(randomQueryLevelTrigger())
        )
        val createdMonitor = createMonitorWithClient(aliceClient!!, monitor)
        val monitorId = createdMonitor.id

        // Share read-only with bob
        shareResource(aliceClient!!, monitorId, "monitor", "alerting_read_only", bobUser)
        Thread.sleep(2000)

        // Bob (read-only) should NOT be able to share further
        val exception = expectThrows(ResponseException::class.java) {
            shareResource(bobClient!!, monitorId, "monitor", "alerting_read_only", "someone_else")
        }
        assertTrue(
            exception.message!!.contains("no permissions") || exception.response.statusLine.statusCode == 403
        )
    }

    private fun shareResource(client: RestClient, resourceId: String, resourceType: String, accessLevel: String, user: String) {
        val request = Request("PUT", "/_plugins/_security/api/resource/share")
        request.setJsonEntity(
            """
            {
              "resource_id": "$resourceId",
              "resource_type": "$resourceType",
              "share_with": {
                "$accessLevel": {
                    "users": ["$user"]
                }
              }
            }
            """.trimIndent()
        )
        val response = client.performRequest(request)
        assertEquals(200, response.statusLine.statusCode)
    }
}
