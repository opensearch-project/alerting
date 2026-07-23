/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.makeRequest
import org.opensearch.client.Request
import org.opensearch.core.rest.RestStatus

/**
 * Exercises `POST /_plugins/_alerting/_migrate_to_rsc`. Runs unconditionally (does not depend on
 * `security` / `resource_sharing.enabled`) — the migration endpoint is a plain UBQ operation and
 * has value even when the resource-sharing feature isn't enabled yet.
 *
 * Test strategy: bypass the alerting REST layer and write legacy-shape docs (no owner scratch
 * fields) directly to `.opendistro-alerting-config` via admin. Then call the migrate endpoint and
 * query the docs back to confirm they gained `_migration_user_name` / `_migration_backend_roles`.
 * The security plugin identifies monitor vs workflow at index-op time via type-specific typeField
 * paths (`monitor.type` / `workflow.type`), so no top-level discriminator field is written.
 */
class MigrateToRscRestApiIT : AlertingRestTestCase() {

    private val configIndex = ".opendistro-alerting-config"

    fun `test migrate backfills owner scratch fields on a legacy monitor doc`() {
        val docId = "legacy-monitor-1"
        val legacyMonitor = """
            {
              "monitor": {
                "type": "monitor",
                "schema_version": 0,
                "name": "legacy-name",
                "monitor_type": "query_level_monitor",
                "user": { "name": "alice", "backend_roles": ["engineering", "ops"] },
                "enabled": false,
                "enabled_time": null,
                "schedule": { "period": { "interval": 5, "unit": "MINUTES" } },
                "inputs": [],
                "triggers": []
              }
            }
        """.trimIndent()
        indexRawDoc(docId, legacyMonitor)

        val response = adminClient().makeRequest("POST", "/_plugins/_alerting/_migrate_to_rsc")
        assertEquals(RestStatus.OK.status, response.statusLine.statusCode)
        val body = response.asMap()
        val updated = (body["updated"] as Number).toLong()
        assertTrue("Expected at least one doc updated, got $updated", updated >= 1L)

        val migrated = readRawDoc(docId)
        assertEquals("alice", migrated["_migration_user_name"])
        assertEquals(listOf("engineering", "ops"), migrated["_migration_backend_roles"])
    }

    fun `test migrate handles a legacy workflow doc`() {
        val docId = "legacy-workflow-1"
        val legacyWorkflow = """
            {
              "workflow": {
                "type": "workflow",
                "schema_version": 0,
                "name": "legacy-wf",
                "workflow_type": "composite",
                "user": { "name": "bob", "backend_roles": ["ml"] },
                "enabled": false,
                "enabled_time": null,
                "schedule": { "period": { "interval": 5, "unit": "MINUTES" } },
                "inputs": [],
                "triggers": [],
                "owner": "alerting"
              }
            }
        """.trimIndent()
        indexRawDoc(docId, legacyWorkflow)

        adminClient().makeRequest("POST", "/_plugins/_alerting/_migrate_to_rsc")

        val migrated = readRawDoc(docId)
        assertEquals("bob", migrated["_migration_user_name"])
        assertEquals(listOf("ml"), migrated["_migration_backend_roles"])
    }

    fun `test migrate is idempotent and noops on already-migrated docs`() {
        val docId = "already-migrated"
        val alreadyMigrated = """
            {
              "_migration_user_name": "carol",
              "_migration_backend_roles": ["sec"],
              "monitor": {
                "type": "monitor",
                "schema_version": 0,
                "name": "already",
                "monitor_type": "query_level_monitor",
                "user": { "name": "carol", "backend_roles": ["sec"] },
                "enabled": false,
                "enabled_time": null,
                "schedule": { "period": { "interval": 5, "unit": "MINUTES" } },
                "inputs": [],
                "triggers": []
              }
            }
        """.trimIndent()
        indexRawDoc(docId, alreadyMigrated)

        // First run — may pick up other legacy docs from prior tests in this class, so we don't
        // assert on the counts here. What we do assert: a follow-up run reports zero updated.
        adminClient().makeRequest("POST", "/_plugins/_alerting/_migrate_to_rsc")

        val secondRun = adminClient().makeRequest("POST", "/_plugins/_alerting/_migrate_to_rsc")
        assertEquals(RestStatus.OK.status, secondRun.statusLine.statusCode)
        val body = secondRun.asMap()
        val updated = (body["updated"] as Number).toLong()
        assertEquals("Second run must be a full noop", 0L, updated)

        val migrated = readRawDoc(docId)
        assertEquals("carol", migrated["_migration_user_name"])
    }

    fun `test migrate deletes non-shareable metadata docs`() {
        val docId = "some-monitor-id-metadata"
        val metadata = """
            {
              "metadata": { "monitor_id": "some-monitor-id", "last_run_context": {} }
            }
        """.trimIndent()
        indexRawDoc(docId, metadata)

        adminClient().makeRequest("POST", "/_plugins/_alerting/_migrate_to_rsc")

        // Metadata docs are deleted because the downstream security plugin's `resources/migrate`
        // fails if it encounters any doc it can't classify by type. Metadata is regenerated on the
        // next monitor execution, so deletion is safe.
        try {
            readRawDoc(docId)
            fail("Expected metadata doc to be deleted after migrate, but it still exists")
        } catch (e: org.opensearch.client.ResponseException) {
            assertEquals(
                "Expected 404 for deleted metadata doc",
                RestStatus.NOT_FOUND.status,
                e.response.statusLine.statusCode,
            )
        }
    }

    private fun indexRawDoc(id: String, source: String) {
        val request = Request("PUT", "/$configIndex/_doc/$id?refresh=true")
        request.setJsonEntity(source)
        // Direct writes to the system index emit a deprecation warning; ignore so the test
        // doesn't fail on `WarningFailureException`.
        val optionsBuilder = org.opensearch.client.RequestOptions.DEFAULT.toBuilder()
        optionsBuilder.setWarningsHandler(org.opensearch.client.WarningsHandler.PERMISSIVE)
        request.setOptions(optionsBuilder.build())
        adminClient().performRequest(request)
    }

    @Suppress("UNCHECKED_CAST")
    private fun readRawDoc(id: String): Map<String, Any> {
        val request = Request("GET", "/$configIndex/_doc/$id")
        val optionsBuilder = org.opensearch.client.RequestOptions.DEFAULT.toBuilder()
        optionsBuilder.setWarningsHandler(org.opensearch.client.WarningsHandler.PERMISSIVE)
        request.setOptions(optionsBuilder.build())
        val response = adminClient().performRequest(request)
        assertEquals(RestStatus.OK.status, response.statusLine.statusCode)
        val bodyStr = org.apache.hc.core5.http.io.entity.EntityUtils.toString(response.entity)
        val parser = org.opensearch.common.xcontent.json.JsonXContent.jsonXContent
            .createParser(xContentRegistry(), org.opensearch.common.xcontent.LoggingDeprecationHandler.INSTANCE, bodyStr)
        return parser.map()["_source"] as Map<String, Any>
    }
}
