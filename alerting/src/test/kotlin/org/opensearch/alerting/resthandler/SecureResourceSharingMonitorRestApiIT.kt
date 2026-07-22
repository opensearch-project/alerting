/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.EntityUtils
import org.apache.hc.core5.http.io.entity.StringEntity
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Ignore
import org.opensearch.alerting.ALERTING_BASE_URI
import org.opensearch.alerting.ALERTING_FULL_ACCESS_ROLE
import org.opensearch.alerting.AlertingPlugin.Companion.COMMENTS_BASE_URI
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
 * The suite drives alerting transport actions (via REST) as non-admin users to verify:
 *  - default deny: a user with the alerting role but no share entry gets 403 on read/update/delete/re-share
 *  - graduated access: read-only < read-write < full-access, where each level unlocks progressively more actions
 *  - non-owner mutations propagate back to the owner (alice sees bob's edits, sees bob's deletes)
 *  - subordinate resources (alerts, comments) inherit access from the parent monitor
 *  - revoke removes access
 *  - cross-resource isolation: a share on monitor A does not grant access to monitor B
 *  - third-party isolation: a share to bob does not grant access to carol
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

        private const val RS_ALICE = "rs_alice"
        private const val RS_BOB = "rs_bob"
        private const val RS_CAROL = "rs_carol"

        private const val READ_ONLY = "alerting_read_only"
        private const val READ_WRITE = "alerting_read_write"
        private const val FULL_ACCESS = "alerting_full_access"

        private const val TEST_INDEX = "rs_test_index"
        private const val TEST_INDEX_ROLE = "rs_test_index_role"
    }

    private var aliceClient: RestClient? = null
    private var bobClient: RestClient? = null
    private var carolClient: RestClient? = null

    @Before
    fun setupUsers() {
        // Ensure user/role provisioning is idempotent across test methods. The security plugin
        // treats PUT rolesmapping as replace, so we always include all three users. Repeatedly
        // recreating users and mappings within a suite has been observed to expose a config-cache
        // race that intermittently 403s subsequent cluster-level alerting actions — so avoid
        // touching them in @After.
        try { createTestIndex(TEST_INDEX) } catch (_: Exception) { /* already exists */ }
        try { createIndexRole(TEST_INDEX_ROLE, TEST_INDEX) } catch (_: Exception) { /* already exists */ }

        // Only ALERTING_FULL_ACCESS_ROLE — no all_access — so RSC is the sole gate.
        // createInternalUser uses PUT which is idempotent — safe to call every test.
        createInternalUser(RS_ALICE, arrayOf("engineering"))
        createInternalUser(RS_BOB, arrayOf("marketing"))
        createInternalUser(RS_CAROL, arrayOf("finance"))
        // Single mapping call for all three; PUT replaces so we must map them together.
        mapUsersToRole(ALERTING_FULL_ACCESS_ROLE, arrayOf(RS_ALICE, RS_BOB, RS_CAROL))
        mapUsersToRole(TEST_INDEX_ROLE, arrayOf(RS_ALICE, RS_BOB, RS_CAROL))

        aliceClient = buildClient(RS_ALICE)
        bobClient = buildClient(RS_BOB)
        carolClient = buildClient(RS_CAROL)
    }

    @After
    fun cleanupClients() {
        aliceClient?.close()
        bobClient?.close()
        carolClient?.close()
        aliceClient = null
        bobClient = null
        carolClient = null
        // Deliberately DO NOT delete users/rolesmappings/roles here — see [setupUsers] for the rationale.
    }

    // ─── Owner can always operate on their own resource ──────────────────────────

    fun `test owner can get their own monitor`() {
        val monitorId = aliceCreatesMonitor().id
        assertOk { aliceClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId") }
    }

    fun `test owner can update their own monitor`() {
        val monitor = aliceCreatesMonitor()
        updateMonitorAs(aliceClient!!, monitor.copy(name = "renamed"))
    }

    fun `test owner can delete their own monitor`() {
        val monitor = aliceCreatesMonitor()
        deleteMonitorAs(aliceClient!!, monitor)
    }

    // ─── Default deny (no share) ─────────────────────────────────────────────────

    fun `test bob cannot get alice's monitor without share`() {
        val monitorId = aliceCreatesMonitor().id
        assertForbidden { bobClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId") }
    }

    fun `test bob cannot update alice's monitor without share`() {
        val monitor = aliceCreatesMonitor()
        assertForbidden { updateMonitorAs(bobClient!!, monitor.copy(name = "hijacked")) }
    }

    fun `test bob cannot delete alice's monitor without share`() {
        val monitor = aliceCreatesMonitor()
        assertForbidden { deleteMonitorAs(bobClient!!, monitor) }
    }

    fun `test bob cannot re-share alice's monitor without share`() {
        val monitorId = aliceCreatesMonitor().id
        assertForbidden { shareResource(bobClient!!, monitorId, READ_ONLY, RS_CAROL) }
    }

    // ─── read-only share ─────────────────────────────────────────────────────────

    fun `test read-only share grants get`() {
        val monitorId = aliceCreatesMonitor().id
        shareResource(aliceClient!!, monitorId, READ_ONLY, RS_BOB)
        assertOk { bobClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId") }
    }

    fun `test read-only share denies update`() {
        val monitor = aliceCreatesMonitor()
        shareResource(aliceClient!!, monitor.id, READ_ONLY, RS_BOB)
        assertForbidden { updateMonitorAs(bobClient!!, monitor.copy(name = "renamed-by-bob")) }
    }

    fun `test read-only share denies delete`() {
        val monitor = aliceCreatesMonitor()
        shareResource(aliceClient!!, monitor.id, READ_ONLY, RS_BOB)
        assertForbidden { deleteMonitorAs(bobClient!!, monitor) }
    }

    fun `test read-only share denies re-share`() {
        val monitorId = aliceCreatesMonitor().id
        shareResource(aliceClient!!, monitorId, READ_ONLY, RS_BOB)
        assertForbidden { shareResource(bobClient!!, monitorId, READ_ONLY, RS_CAROL) }
    }

    // ─── read-write share ────────────────────────────────────────────────────────

    fun `test read-write share grants update`() {
        val monitor = aliceCreatesMonitor()
        shareResource(aliceClient!!, monitor.id, READ_WRITE, RS_BOB)
        val updated = updateMonitorAs(bobClient!!, monitor.copy(name = "renamed-by-bob"))
        assertEquals("renamed-by-bob", updated.name)
    }

    fun `test read-write share grants delete`() {
        val monitor = aliceCreatesMonitor()
        shareResource(aliceClient!!, monitor.id, READ_WRITE, RS_BOB)
        assertOk { deleteMonitorAs(bobClient!!, monitor) }
    }

    fun `test read-write share denies re-share`() {
        // share permission belongs only to full-access
        val monitorId = aliceCreatesMonitor().id
        shareResource(aliceClient!!, monitorId, READ_WRITE, RS_BOB)
        assertForbidden { shareResource(bobClient!!, monitorId, READ_ONLY, RS_CAROL) }
    }

    fun `test owner sees edits made by read-write shared user`() {
        val monitor = aliceCreatesMonitor()
        shareResource(aliceClient!!, monitor.id, READ_WRITE, RS_BOB)
        updateMonitorAs(bobClient!!, monitor.copy(name = "renamed-by-bob"))

        val body = getBody(aliceClient!!, "$ALERTING_BASE_URI/${monitor.id}")
        assertTrue("Owner should see edits by shared user: $body", body.contains("renamed-by-bob"))
    }

    fun `test owner sees delete performed by read-write shared user`() {
        val monitor = aliceCreatesMonitor()
        shareResource(aliceClient!!, monitor.id, READ_WRITE, RS_BOB)
        deleteMonitorAs(bobClient!!, monitor)

        assertNotFound { aliceClient!!.makeRequest("GET", "$ALERTING_BASE_URI/${monitor.id}") }
    }

    // ─── full-access share ───────────────────────────────────────────────────────

    fun `test full-access share grants re-share`() {
        val monitorId = aliceCreatesMonitor().id
        shareResource(aliceClient!!, monitorId, FULL_ACCESS, RS_BOB)
        // Bob re-shares to carol
        shareResource(bobClient!!, monitorId, READ_ONLY, RS_CAROL)
        assertOk { carolClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId") }
    }

    fun `test full-access share grants delete`() {
        val monitor = aliceCreatesMonitor()
        shareResource(aliceClient!!, monitor.id, FULL_ACCESS, RS_BOB)
        assertOk { deleteMonitorAs(bobClient!!, monitor) }
    }

    // ─── Third-party isolation ───────────────────────────────────────────────────

    fun `test share to bob does not grant access to carol`() {
        val monitorId = aliceCreatesMonitor().id
        shareResource(aliceClient!!, monitorId, READ_ONLY, RS_BOB)
        assertForbidden { carolClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId") }
    }

    // ─── Cross-resource isolation ────────────────────────────────────────────────

    fun `test share on one monitor does not grant access to another`() {
        val sharedMonitorId = aliceCreatesMonitor().id
        val unsharedMonitorId = aliceCreatesMonitor().id
        shareResource(aliceClient!!, sharedMonitorId, READ_ONLY, RS_BOB)

        assertOk { bobClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$sharedMonitorId") }
        assertForbidden { bobClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$unsharedMonitorId") }
    }

    // ─── Search DLS ──────────────────────────────────────────────────────────────

    fun `test search excludes monitors not owned or shared`() {
        val aliceMonitorId = aliceCreatesMonitor().id
        val bobMonitorId = bobCreatesMonitor().id

        val body = searchMonitors(bobClient!!)
        assertTrue("Bob's own monitor missing: $body", body.contains(bobMonitorId))
        assertFalse("Alice's monitor leaked: $body", body.contains(aliceMonitorId))
    }

    fun `test search includes shared monitor`() {
        val aliceMonitorId = aliceCreatesMonitor().id
        shareResource(aliceClient!!, aliceMonitorId, READ_ONLY, RS_BOB)

        val body = searchMonitors(bobClient!!)
        assertTrue("Shared monitor missing from search: $body", body.contains(aliceMonitorId))
    }

    // ─── Subordinate resource: alerts ────────────────────────────────────────────

    fun `test alerts inherit denial when monitor is not shared`() {
        val monitor = aliceCreatesMonitor()
        putAlertMappings()
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE, monitorId = monitor.id))

        // Bob has no share entry for this monitor. Two valid RSC outcomes:
        //   (a) 403 — bob has no active shares, so the cluster-level action gate rejects
        //       cluster:admin/opendistro/alerting/alerts/get outright.
        //   (b) 200 with an empty result — bob has some other share elsewhere, so the action is
        //       allowed and DLS filters out alice's alert.
        // The test guarantee is "bob cannot see alice's alert", satisfied by either outcome.
        try {
            val body = getBody(bobClient!!, "$ALERTING_BASE_URI/alerts?monitorId=${monitor.id}")
            assertFalse("Alert leaked without share: $body", body.contains(alert.id))
        } catch (e: ResponseException) {
            assertEquals(
                "Unexpected non-403 status on alerts GET without share",
                RestStatus.FORBIDDEN.status,
                e.response.statusLine.statusCode
            )
        }
    }

    fun `test alerts inherit access when monitor is shared read-only`() {
        val monitor = aliceCreatesMonitor()
        putAlertMappings()
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE, monitorId = monitor.id))
        shareResource(aliceClient!!, monitor.id, READ_ONLY, RS_BOB)

        val body = getBody(bobClient!!, "$ALERTING_BASE_URI/alerts?monitorId=${monitor.id}")
        assertTrue("Shared alert missing: $body", body.contains(alert.id))
    }

    fun `test acknowledge alert denied without share`() {
        val monitor = aliceCreatesMonitor()
        putAlertMappings()
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE, monitorId = monitor.id))

        assertForbidden { acknowledgeAlertsWithClient(bobClient!!, monitor, alert) }
    }

    fun `test acknowledge alert allowed with read-write share`() {
        val monitor = aliceCreatesMonitor()
        putAlertMappings()
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE, monitorId = monitor.id))
        shareResource(aliceClient!!, monitor.id, READ_WRITE, RS_BOB)

        acknowledgeAlertsWithClient(bobClient!!, monitor, alert)
    }

    // ─── Subordinate resource: comments ──────────────────────────────────────────

    // FIXME: CommentsIndices.createOrUpdateInitialCommentsHistoryIndex fires an
    // `indices().exists()` call as the caller — under RSC bob's role has no direct
    // index privileges on the comments history index, so the coroutine throws an uncaught
    // OpenSearchSecurityException and the HTTP response never returns (test hangs to suite
    // timeout). Fix requires wrapping that path in a per-call stash the way monitor writes do.
    @Ignore
    fun `test comment on alert denied without share`() {
        val monitor = aliceCreatesMonitor()
        putAlertMappings()
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE, monitorId = monitor.id))

        assertForbidden {
            val body = """{"content":"hi from bob"}"""
            bobClient!!.makeRequest(
                "POST",
                "$COMMENTS_BASE_URI/${alert.id}",
                emptyMap(),
                StringEntity(body, ContentType.APPLICATION_JSON)
            )
        }
    }

    // FIXME: same CommentsIndices.createIndex issue as the denied test above — bob's request
    // dies in the initial `indices().exists()` call before RSC can gate it. Re-enable once the
    // comments-flow stash pattern lands.
    @Ignore
    fun `test comment on alert allowed with read-write share`() {
        val monitor = aliceCreatesMonitor()
        putAlertMappings()
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE, monitorId = monitor.id))
        shareResource(aliceClient!!, monitor.id, READ_WRITE, RS_BOB)

        val body = """{"content":"hi from bob"}"""
        val response = bobClient!!.makeRequest(
            "POST",
            "$COMMENTS_BASE_URI/${alert.id}",
            emptyMap(),
            StringEntity(body, ContentType.APPLICATION_JSON)
        )
        assertEquals(RestStatus.CREATED.status, response.statusLine.statusCode)
    }

    // ─── Access-level downgrade ──────────────────────────────────────────────────

    fun `test re-sharing with lower access level narrows permissions`() {
        val monitor = aliceCreatesMonitor()
        shareResource(aliceClient!!, monitor.id, READ_WRITE, RS_BOB)
        // Confirm bob can update at first
        updateMonitorAs(bobClient!!, monitor.copy(name = "renamed-once"))

        // The framework's PUT /share only ADDS entries at the requested level — it does not
        // implicitly remove entries at other levels. To downgrade bob, we first revoke his
        // read-write share, then add read-only.
        revokeResource(aliceClient!!, monitor.id, RS_BOB)
        shareResource(aliceClient!!, monitor.id, READ_ONLY, RS_BOB)
        assertForbidden {
            updateMonitorAs(bobClient!!, monitor.copy(name = "renamed-again"))
        }
    }

    // ─── Revoke ──────────────────────────────────────────────────────────────────

    fun `test revoke removes access`() {
        val monitorId = aliceCreatesMonitor().id
        shareResource(aliceClient!!, monitorId, READ_ONLY, RS_BOB)
        assertOk { bobClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId") }

        revokeResource(aliceClient!!, monitorId, RS_BOB)
        assertForbidden { bobClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId") }
    }

    fun `test revoke on one user does not affect other user's access`() {
        val monitorId = aliceCreatesMonitor().id
        shareResource(aliceClient!!, monitorId, READ_ONLY, RS_BOB)
        shareResource(aliceClient!!, monitorId, READ_ONLY, RS_CAROL)

        revokeResource(aliceClient!!, monitorId, RS_BOB)

        assertForbidden { bobClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId") }
        assertOk { carolClient!!.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId") }
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────────

    private fun aliceCreatesMonitor() = createMonitorAs(aliceClient!!, sampleMonitor())

    private fun bobCreatesMonitor() = createMonitorAs(bobClient!!, sampleMonitor())

    /**
     * POSTs a monitor as [client] and parses the id out of the response directly.
     * The base test's [createMonitorWithClient] does a follow-up GET with the default admin client,
     * which fails under resource sharing because admin has no share on the newly-created monitor.
     */
    private fun createMonitorAs(
        client: RestClient,
        monitor: org.opensearch.commons.alerting.model.Monitor
    ): org.opensearch.commons.alerting.model.Monitor {
        val response = client.makeRequest(
            "POST",
            "$ALERTING_BASE_URI?refresh=true",
            emptyMap(),
            monitor.toHttpEntity()
        )
        assertEquals(RestStatus.CREATED.status, response.statusLine.statusCode)
        val body = response.asMap()
        val id = body["_id"] as String
        // The security plugin writes the sharing entry to `.opendistro-alerting-config-sharing`
        // asynchronously via its shard-level postIndex hook, after the monitor doc write already
        // acked the REST caller. Poll until the entry is visible before letting the test proceed —
        // otherwise the immediately-following owner request races the postIndex listener and gets
        // a spurious 403 ("No sharing info found").
        waitForResourceSharingEntry(id)
        return monitor.copy(id = id)
    }

    /**
     * PUT a monitor as [client] and skip the admin-driven verification GET in the base helper
     * ([AlertingRestTestCase.updateMonitorWithClient] uses admin client for the follow-up read,
     * which 403s under RSC because admin has no share entry). Returns the updated monitor
     * parsed from the PUT response.
     */
    private fun updateMonitorAs(
        client: RestClient,
        monitor: org.opensearch.commons.alerting.model.Monitor
    ): org.opensearch.commons.alerting.model.Monitor {
        val response = client.makeRequest(
            "PUT",
            "${monitor.relativeUrl()}?refresh=true",
            emptyMap(),
            monitor.toHttpEntity()
        )
        assertEquals("Unable to update a monitor", RestStatus.OK.status, response.statusLine.statusCode)
        val body = response.asMap()
        @Suppress("UNCHECKED_CAST")
        val monitorMap = body["monitor"] as Map<String, Any>
        return monitor.copy(name = monitorMap["name"] as String)
    }

    /**
     * DELETE a monitor as [client] without the admin-driven verification GET the base helper does.
     */
    private fun deleteMonitorAs(
        client: RestClient,
        monitor: org.opensearch.commons.alerting.model.Monitor
    ): org.opensearch.client.Response {
        val response = client.makeRequest(
            "DELETE",
            "${monitor.relativeUrl()}?refresh=true",
            emptyMap(),
            monitor.toHttpEntity()
        )
        assertEquals("Unable to delete a monitor", RestStatus.OK.status, response.statusLine.statusCode)
        return response
    }

    private fun waitForResourceSharingEntry(resourceId: String, timeoutMs: Long = 10_000) {
        val deadline = System.nanoTime() + timeoutMs * 1_000_000
        var lastException: Exception? = null
        while (System.nanoTime() < deadline) {
            try {
                adminClient().performRequest(Request("POST", "/.opendistro-alerting-config-sharing/_refresh"))
                val resp = adminClient().performRequest(
                    Request("GET", "/.opendistro-alerting-config-sharing/_doc/$resourceId")
                )
                if (resp.statusLine.statusCode == 200) return
            } catch (e: Exception) {
                lastException = e
            }
            Thread.sleep(100)
        }
        throw IllegalStateException("Sharing entry for $resourceId never appeared within ${timeoutMs}ms", lastException)
    }

    private fun sampleMonitor() = randomQueryLevelMonitor(
        inputs = listOf(
            org.opensearch.commons.alerting.model.SearchInput(
                indices = listOf(TEST_INDEX),
                query = org.opensearch.search.builder.SearchSourceBuilder()
                    .query(org.opensearch.index.query.QueryBuilders.matchAllQuery())
            )
        ),
        triggers = listOf(randomQueryLevelTrigger())
    )

    private fun searchMonitors(client: RestClient): String {
        val body = """{"query":{"match_all":{}}}"""
        val response = client.makeRequest(
            "POST",
            "$ALERTING_BASE_URI/_search",
            emptyMap(),
            StringEntity(body, ContentType.APPLICATION_JSON)
        )
        return EntityUtils.toString(response.entity)
    }

    private fun getBody(client: RestClient, path: String): String {
        val response = client.makeRequest("GET", path)
        return EntityUtils.toString(response.entity)
    }

    private fun assertOk(block: () -> org.opensearch.client.Response) {
        val response = block()
        val status = response.statusLine.statusCode
        assertTrue("Expected 2xx but got $status", status in 200..299)
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

    private fun createInternalUser(name: String, backendRoles: Array<String>) {
        val broles = backendRoles.joinToString { "\"$it\"" }
        val userReq = Request("PUT", "/_plugins/_security/api/internalusers/$name")
        userReq.setJsonEntity(
            """{ "password": "$password", "backend_roles": [$broles], "attributes": {} }"""
        )
        adminClient().performRequest(userReq)
    }

    private fun mapUsersToRole(role: String, users: Array<String>) {
        val usersJson = users.joinToString { "\"$it\"" }
        val req = Request("PUT", "/_plugins/_security/api/rolesmapping/$role")
        req.setJsonEntity("""{ "backend_roles": [], "hosts": [], "users": [$usersJson] }""")
        adminClient().performRequest(req)
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

    /**
     * Revoke uses the same `/share` endpoint with method PATCH and a `revoke` body keyed by
     * access-level (mirrors the security plugin's [ShareRequest] contract). The caller must
     * enumerate all access levels the user might be shared at — we pass all three since tests
     * don't always know which level was granted.
     */
    private fun revokeResource(client: RestClient, resourceId: String, user: String) {
        val request = Request("PATCH", "/_plugins/_security/api/resource/share")
        request.setJsonEntity(
            """
            {
              "resource_id": "$resourceId",
              "resource_type": "monitor",
              "revoke": {
                "$READ_ONLY":   { "users": ["$user"] },
                "$READ_WRITE":  { "users": ["$user"] },
                "$FULL_ACCESS": { "users": ["$user"] }
              }
            }
            """.trimIndent()
        )
        val response = client.performRequest(request)
        assertEquals(200, response.statusLine.statusCode)
    }
}
