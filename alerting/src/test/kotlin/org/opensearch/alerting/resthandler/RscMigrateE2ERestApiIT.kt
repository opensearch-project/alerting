/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.junit.BeforeClass
import org.opensearch.alerting.ALERTING_BASE_URI
import org.opensearch.alerting.ALERTING_FULL_ACCESS_ROLE
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.alerting.randomQueryLevelTrigger
import org.opensearch.client.Request
import org.opensearch.client.ResponseException
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.core.rest.RestStatus

/**
 * End-to-end migration lifecycle: pre-RSC → enable RSC → observe broken access →
 * run migrate endpoints → observe restored access.
 *
 * Requires the test cluster to be started with security enabled AND
 * `-Dresource_sharing.enabled=true` (which sets the static seed but leaves the flag
 * runtime-toggleable via cluster settings). The test flips the dynamic
 * `plugins.security.experimental.resource_sharing.enabled` cluster setting on and off
 * to simulate an upgrade from a pre-RSC cluster.
 *
 * The test explicitly ignores `plugins.security.experimental.resource_sharing.protected_types`
 * because that setting is also dynamic and its default (empty) list would leave the framework
 * inert even with the feature enabled — we set it during the "enable" phase.
 */
class RscMigrateE2ERestApiIT : AlertingRestTestCase() {

    companion object {
        @BeforeClass
        @JvmStatic
        fun requireSecurityAndRsc() {
            org.junit.Assume.assumeTrue(System.getProperty("security", "false")!!.toBoolean())
            org.junit.Assume.assumeTrue(System.getProperty("resource_sharing.enabled", "false")!!.toBoolean())
        }

        private const val RS_ALICE = "rs_alice_e2e"
        private const val TEST_INDEX = "rs_e2e_test_index"
        private const val TEST_INDEX_ROLE = "rs_e2e_test_index_role"
        private const val PASSWORD = "myStrongPassword123!"
    }

    fun `test end-to-end migrate from legacy to rsc`() {
        // ─── Phase 0: user setup (RSC-agnostic) ──────────────────────────────
        try { createTestIndex(TEST_INDEX) } catch (_: Exception) { /* already exists */ }
        try { createIndexRole(TEST_INDEX_ROLE, TEST_INDEX) } catch (_: Exception) { /* already exists */ }
        createUserE2E(RS_ALICE, arrayOf("engineering"))
        mapUsers(ALERTING_FULL_ACCESS_ROLE, arrayOf(RS_ALICE))
        mapUsers(TEST_INDEX_ROLE, arrayOf(RS_ALICE))
        val aliceClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), RS_ALICE, PASSWORD)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        try {
            // ─── Phase 1: RSC disabled — legacy backend-roles path ───────────────
            setClusterSetting("plugins.security.experimental.resource_sharing.enabled", false)
            setClusterSetting("plugins.security.experimental.resource_sharing.protected_types", emptyList<String>())

            val createResp = aliceClient.makeRequest(
                "POST",
                "$ALERTING_BASE_URI?refresh=true",
                emptyMap(),
                sampleMonitor().toHttpEntity(),
            )
            assertEquals(
                "Legacy monitor create must succeed before RSC is enabled",
                RestStatus.CREATED.status,
                createResp.statusLine.statusCode,
            )
            val monitorId = createResp.asMap()["_id"] as String

            // Alice can read her monitor via the legacy backend-roles path.
            val legacyGet = aliceClient.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId")
            assertEquals(
                "Legacy monitor GET must succeed before RSC is enabled",
                RestStatus.OK.status,
                legacyGet.statusLine.statusCode,
            )

            // Delete any auto-generated sharing entry that may have been created while RSC was
            // (momentarily) enabled — we want a truly-pre-RSC shape for the rest of the flow.
            deleteSharingEntry(monitorId)

            // ─── Phase 2: enable RSC — reads break because no sharing entry ──────
            setClusterSetting("plugins.security.experimental.resource_sharing.enabled", true)
            setClusterSetting(
                "plugins.security.experimental.resource_sharing.protected_types",
                listOf("monitor", "alerting-workflow"),
            )

            val brokenGet = try {
                aliceClient.performRequest(Request("GET", "$ALERTING_BASE_URI/$monitorId"))
                null
            } catch (e: ResponseException) {
                e
            }
            assertNotNull(
                "After enabling RSC without migration, legacy monitor GET must fail",
                brokenGet,
            )
            assertEquals(
                "Post-enable-without-migrate: expected 403, got ${brokenGet?.response?.statusLine?.statusCode}",
                RestStatus.FORBIDDEN.status,
                brokenGet!!.response.statusLine.statusCode,
            )

            // ─── Phase 3: run security-side migration (seeds sharing entries) ────
            //
            // The security plugin classifies each doc by trying each registered provider's
            // typeField (`monitor.type` / `workflow.type`) and reads owner metadata via each
            // provider's `ownerNamePath` / `ownerBackendRolesPath`. The request-level paths are
            // still required by the schema but only used as fallbacks — for alerting they aren't
            // referenced because every provider declares its own.
            val securityMigrate = adminClient().performRequest(
                Request("POST", "/_plugins/_security/api/resources/migrate").apply {
                    setJsonEntity(
                        """
                        {
                          "source_index": ".opendistro-alerting-config",
                          "username_path": "/monitor/user/name",
                          "backend_roles_path": "/monitor/user/backend_roles",
                          "default_owner": "$RS_ALICE",
                          "default_access_level": {
                            "monitor": "alerting_full_access",
                            "alerting-workflow": "alerting_full_access"
                          }
                        }
                        """.trimIndent(),
                    )
                    val opts = org.opensearch.client.RequestOptions.DEFAULT.toBuilder()
                    opts.setWarningsHandler(org.opensearch.client.WarningsHandler.PERMISSIVE)
                    setOptions(opts.build())
                },
            )
            assertEquals(RestStatus.OK.status, securityMigrate.statusLine.statusCode)

            // Wait for the sharing shard to acknowledge — mirrors the same race we work around
            // in [SecureResourceSharingMonitorRestApiIT.createMonitorAs].
            adminClient().performRequest(Request("POST", "/.opendistro-alerting-config-sharing/_refresh"))

            // ─── Phase 5: alice reads her monitor again — RSC now lets her through ─
            val postMigrateGet = aliceClient.makeRequest("GET", "$ALERTING_BASE_URI/$monitorId")
            assertEquals(
                "After migration, alice should recover access to her own legacy monitor",
                RestStatus.OK.status,
                postMigrateGet.statusLine.statusCode,
            )
        } finally {
            aliceClient.close()
            // Reset the flags so subsequent tests don't inherit a modified cluster state.
            setClusterSetting("plugins.security.experimental.resource_sharing.enabled", true)
            setClusterSetting(
                "plugins.security.experimental.resource_sharing.protected_types",
                listOf("monitor", "alerting-workflow"),
            )
        }
    }

    private fun sampleMonitor() = randomQueryLevelMonitor(
        inputs = listOf(
            org.opensearch.commons.alerting.model.SearchInput(
                indices = listOf(TEST_INDEX),
                query = org.opensearch.search.builder.SearchSourceBuilder()
                    .query(org.opensearch.index.query.QueryBuilders.matchAllQuery()),
            ),
        ),
        triggers = listOf(randomQueryLevelTrigger()),
    )

    /**
     * Delete the auto-generated sharing entry so the doc looks like it was written by a pre-RSC
     * alerting build (no sharing record exists). Harmless if none exists.
     */
    private fun deleteSharingEntry(monitorId: String) {
        try {
            val opts = org.opensearch.client.RequestOptions.DEFAULT.toBuilder()
            opts.setWarningsHandler(org.opensearch.client.WarningsHandler.PERMISSIVE)
            val delRequest = Request(
                "DELETE",
                "/.opendistro-alerting-config-sharing/_doc/$monitorId?refresh=true",
            )
            delRequest.setOptions(opts.build())
            adminClient().performRequest(delRequest)
        } catch (_: Exception) {
            // No sharing entry existed — expected on a truly-legacy doc simulation.
        }
    }

    private fun setClusterSetting(key: String, value: Any) {
        val jsonValue = when (value) {
            is Boolean -> value.toString()
            is List<*> -> value.joinToString(prefix = "[", postfix = "]") { "\"$it\"" }
            else -> "\"$value\""
        }
        val body = """{ "persistent": { "$key": $jsonValue } }"""
        val request = Request("PUT", "/_cluster/settings")
        request.setJsonEntity(body)
        val opts = org.opensearch.client.RequestOptions.DEFAULT.toBuilder()
        opts.setWarningsHandler(org.opensearch.client.WarningsHandler.PERMISSIVE)
        request.setOptions(opts.build())
        adminClient().performRequest(request)
    }

    private fun createUserE2E(name: String, backendRoles: Array<String>) {
        val broles = backendRoles.joinToString { "\"$it\"" }
        val req = Request("PUT", "/_plugins/_security/api/internalusers/$name")
        req.setJsonEntity(
            """{ "password": "$PASSWORD", "backend_roles": [$broles], "attributes": {} }""",
        )
        adminClient().performRequest(req)
    }

    private fun mapUsers(role: String, users: Array<String>) {
        val usersJson = users.joinToString { "\"$it\"" }
        val req = Request("PUT", "/_plugins/_security/api/rolesmapping/$role")
        req.setJsonEntity("""{ "backend_roles": [], "hosts": [], "users": [$usersJson] }""")
        adminClient().performRequest(req)
    }
}
