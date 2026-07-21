/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.hc.core5.http.io.entity.EntityUtils
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.opensearch.alerting.ALERTING_BASE_URI
import org.opensearch.alerting.ALERTING_FULL_ACCESS_ROLE
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.randomAlert
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.alerting.randomQueryLevelTrigger
import org.opensearch.client.Request
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.core.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging

/**
 * Integration tests that exercise the security plugin's transport-level interception on the resource-sharing framework.
 *
 * Each test drives an alerting transport action (via REST) as a non-admin user without a share entry on the target
 * monitor. The security plugin's ActionFilter (using DocRequest.id) is expected to reject those requests with 403.
 * When the resource is explicitly shared, the same requests should succeed.
 *
 * Runs only when both `security` and `resource_sharing.enabled` system properties are true.
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

        // Only ALERTING_FULL_ACCESS_ROLE — no all_access — so RSC is the sole gate.
        createRsUser(aliceUser, arrayOf("engineering"))
        aliceClient = buildClient(aliceUser)

        createRsUser(bobUser, arrayOf("marketing"))
        bobClient = buildClient(bobUser)
    }

    @After
    fun cleanupClients() {
        aliceClient?.close()
        bobClient?.close()
        aliceClient = null
        bobClient = null
        deleteRsUser(aliceUser)
        deleteRsUser(bobUser)
    }

    // ─── GET monitor ─────────────────────────────────────────────────────────────

    fun `test bob cannot get alice's monitor without share`() {
        val monitorId = aliceCreatesMonitor().id
        assertForbidden { bobClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId") }
    }

    fun `test bob can get alice's monitor after read-only share`() {
        val monitorId = aliceCreatesMonitor().id
        shareResource(aliceClient!!, monitorId, "alerting_read_only", bobUser)

        val response = bobClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId")
        assertEquals(RestStatus.OK.status, response.statusLine.statusCode)
    }

    // ─── UPDATE monitor ──────────────────────────────────────────────────────────

    fun `test bob cannot update alice's monitor with read-only share`() {
        val monitor = aliceCreatesMonitor()
        shareResource(aliceClient!!, monitor.id, "alerting_read_only", bobUser)

        assertForbidden {
            updateMonitorWithClient(bobClient!!, monitor.copy(name = "renamed-by-bob"))
        }
    }

    fun `test bob can update alice's monitor with read-write share`() {
        val monitor = aliceCreatesMonitor()
        shareResource(aliceClient!!, monitor.id, "alerting_read_write", bobUser)

        val updated = updateMonitorWithClient(bobClient!!, monitor.copy(name = "renamed-by-bob"))
        assertEquals("renamed-by-bob", updated.name)
    }

    fun `test alice sees bob's edits after read-write share`() {
        val monitor = aliceCreatesMonitor()
        shareResource(aliceClient!!, monitor.id, "alerting_read_write", bobUser)

        updateMonitorWithClient(bobClient!!, monitor.copy(name = "renamed-by-bob"))

        // Owner alice re-reads and sees bob's change
        val response = aliceClient!!.makeRequest("GET", "$ALERTING_BASE_URI/${monitor.id}")
        val body = EntityUtils.toString(response.entity)
        assertTrue("Owner should see edits made by shared user: $body", body.contains("renamed-by-bob"))
    }

    // ─── DELETE monitor ──────────────────────────────────────────────────────────

    fun `test bob cannot delete alice's monitor with read-only share`() {
        val monitor = aliceCreatesMonitor()
        shareResource(aliceClient!!, monitor.id, "alerting_read_only", bobUser)

        assertForbidden { deleteMonitorWithClient(bobClient!!, monitor) }
    }

    fun `test bob can delete alice's monitor with read-write share`() {
        val monitor = aliceCreatesMonitor()
        shareResource(aliceClient!!, monitor.id, "alerting_read_write", bobUser)

        val response = deleteMonitorWithClient(bobClient!!, monitor)
        assertEquals(RestStatus.OK.status, response.statusLine.statusCode)
    }

    fun `test bob can delete alice's monitor with full-access share`() {
        val monitor = aliceCreatesMonitor()
        shareResource(aliceClient!!, monitor.id, "alerting_full_access", bobUser)

        val response = deleteMonitorWithClient(bobClient!!, monitor)
        assertEquals(RestStatus.OK.status, response.statusLine.statusCode)
    }

    fun `test alice can no longer get her monitor after bob deletes it with full-access`() {
        val monitor = aliceCreatesMonitor()
        shareResource(aliceClient!!, monitor.id, "alerting_full_access", bobUser)

        deleteMonitorWithClient(bobClient!!, monitor)

        // Owner alice sees the delete propagated
        assertNotFound { aliceClient!!.makeRequest("GET", "$ALERTING_BASE_URI/${monitor.id}") }
    }

    // ─── SEARCH monitors ─────────────────────────────────────────────────────────

    fun `test bob's monitor search excludes alice's monitors`() {
        val aliceMonitorId = aliceCreatesMonitor().id
        val bobMonitorId = bobCreatesMonitor().id

        val body = bobSearchMonitors()
        assertTrue("Bob's own monitor missing: $body", body.contains(bobMonitorId))
        assertFalse("Alice's monitor leaked to bob: $body", body.contains(aliceMonitorId))
    }

    fun `test bob's monitor search includes shared monitor`() {
        val aliceMonitorId = aliceCreatesMonitor().id
        shareResource(aliceClient!!, aliceMonitorId, "alerting_read_only", bobUser)

        val body = bobSearchMonitors()
        assertTrue("Shared monitor missing from bob's search: $body", body.contains(aliceMonitorId))
    }

    // ─── GET alerts (subordinate resource) ───────────────────────────────────────

    fun `test bob cannot see alice's monitor alerts without share`() {
        val monitor = aliceCreatesMonitor()
        putAlertMappings()
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE, monitorId = monitor.id))

        val response = bobClient!!.makeRequest("GET", "$ALERTING_BASE_URI/alerts?monitorId=${monitor.id}")
        val body = EntityUtils.toString(response.entity)
        assertFalse("Alert leaked to bob without share: $body", body.contains(alert.id))
    }

    fun `test bob can see alice's monitor alerts after share`() {
        val monitor = aliceCreatesMonitor()
        putAlertMappings()
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE, monitorId = monitor.id))
        shareResource(aliceClient!!, monitor.id, "alerting_read_only", bobUser)

        val response = bobClient!!.makeRequest("GET", "$ALERTING_BASE_URI/alerts?monitorId=${monitor.id}")
        val body = EntityUtils.toString(response.entity)
        assertTrue("Shared alert missing: $body", body.contains(alert.id))
    }

    // ─── SHARE permission checks ─────────────────────────────────────────────────

    fun `test bob cannot re-share alice's monitor with only read-only access`() {
        val monitorId = aliceCreatesMonitor().id
        shareResource(aliceClient!!, monitorId, "alerting_read_only", bobUser)

        assertForbidden { shareResource(bobClient!!, monitorId, "alerting_read_only", "someone_else") }
    }

    fun `test bob cannot re-share alice's monitor with only read-write access`() {
        // read-write grants monitor CRUD but NOT the resource-share permission
        val monitorId = aliceCreatesMonitor().id
        shareResource(aliceClient!!, monitorId, "alerting_read_write", bobUser)

        assertForbidden { shareResource(bobClient!!, monitorId, "alerting_read_only", "someone_else") }
    }

    fun `test bob can re-share alice's monitor with full-access`() {
        val monitorId = aliceCreatesMonitor().id
        shareResource(aliceClient!!, monitorId, "alerting_full_access", bobUser)

        // Bob re-shares back to alice (must succeed with full_access)
        shareResource(bobClient!!, monitorId, "alerting_read_only", aliceUser)
    }

    // ─── REVOKE ──────────────────────────────────────────────────────────────────

    fun `test bob loses access after alice revokes share`() {
        val monitorId = aliceCreatesMonitor().id
        shareResource(aliceClient!!, monitorId, "alerting_read_only", bobUser)

        val ok = bobClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId")
        assertEquals(RestStatus.OK.status, ok.statusLine.statusCode)

        revokeResource(aliceClient!!, monitorId, bobUser)
        assertForbidden { bobClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId") }
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────────

    private fun aliceCreatesMonitor() = createMonitorWithClient(
        aliceClient!!,
        randomQueryLevelMonitor(triggers = listOf(randomQueryLevelTrigger()))
    )

    private fun bobCreatesMonitor() = createMonitorWithClient(
        bobClient!!,
        randomQueryLevelMonitor(triggers = listOf(randomQueryLevelTrigger()))
    )

    private fun bobSearchMonitors(): String {
        val body = """{"query":{"match_all":{}}}"""
        val entity = org.apache.hc.core5.http.io.entity.StringEntity(
            body,
            org.apache.hc.core5.http.ContentType.APPLICATION_JSON
        )
        val response = bobClient!!.makeRequest("POST", "$ALERTING_BASE_URI/_search", emptyMap(), entity)
        return EntityUtils.toString(response.entity)
    }

    private fun assertForbidden(block: () -> Any?) {
        val exception = expectThrows(ResponseException::class.java) { block() }
        val status = exception.response.statusLine.statusCode
        assertTrue(
            "Expected 403 but got $status: ${exception.message}",
            status == RestStatus.FORBIDDEN.status ||
                exception.message?.contains("no permissions") == true
        )
    }

    private fun assertNotFound(block: () -> Any?) {
        val exception = expectThrows(ResponseException::class.java) { block() }
        assertEquals(RestStatus.NOT_FOUND.status, exception.response.statusLine.statusCode)
    }

    private fun buildClient(user: String): RestClient =
        SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), user, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

    private fun createRsUser(name: String, backendRoles: Array<String>) {
        val broles = backendRoles.joinToString { "\"$it\"" }
        val userReq = Request("PUT", "/_plugins/_security/api/internalusers/$name")
        userReq.setJsonEntity(
            """{ "password": "$password", "backend_roles": [$broles], "attributes": {} }"""
        )
        adminClient().performRequest(userReq)

        val mappingReq = Request("PUT", "/_plugins/_security/api/rolesmapping/$ALERTING_FULL_ACCESS_ROLE")
        mappingReq.setJsonEntity(
            """{ "backend_roles": [], "hosts": [], "users": ["$name"] }"""
        )
        adminClient().performRequest(mappingReq)
    }

    private fun deleteRsUser(name: String) {
        try {
            adminClient().performRequest(
                Request("DELETE", "/_plugins/_security/api/internalusers/$name")
            )
        } catch (_: Exception) {
        }
    }

    private fun shareResource(client: RestClient, resourceId: String, accessLevel: String, user: String) {
        val request = Request("PUT", "/_plugins/_security/api/resource/share")
        request.setJsonEntity(
            """
            {
              "resource_id": "$resourceId",
              "resource_type": "monitor",
              "share_with": { "$accessLevel": { "users": ["$user"] } }
            }
            """.trimIndent()
        )
        val response = client.performRequest(request)
        assertEquals(200, response.statusLine.statusCode)
    }

    private fun revokeResource(client: RestClient, resourceId: String, user: String) {
        val request = Request("POST", "/_plugins/_security/api/resource/revoke")
        request.setJsonEntity(
            """
            {
              "resource_id": "$resourceId",
              "resource_type": "monitor",
              "revoke": { "users": ["$user"] }
            }
            """.trimIndent()
        )
        val response = client.performRequest(request)
        assertEquals(200, response.statusLine.statusCode)
    }
}
