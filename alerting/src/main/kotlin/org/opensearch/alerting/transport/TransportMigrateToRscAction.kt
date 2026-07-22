/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.MigrateToRscAction
import org.opensearch.alerting.action.MigrateToRscRequest
import org.opensearch.alerting.action.MigrateToRscResponse
import org.opensearch.common.inject.Inject
import org.opensearch.commons.alerting.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.core.action.ActionListener
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.DeleteByQueryAction
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder
import org.opensearch.index.reindex.UpdateByQueryAction
import org.opensearch.index.reindex.UpdateByQueryRequestBuilder
import org.opensearch.script.Script
import org.opensearch.script.ScriptType
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

private val log = LogManager.getLogger(TransportMigrateToRscAction::class.java)

/**
 * Runs an update-by-query on `.opendistro-alerting-config` that:
 *
 *  1. Adds the top-level `resource_type` discriminator ("monitor" or "workflow") to every
 *     shareable resource doc, so the security plugin's [ResourceProvider.typeField] contract
 *     can classify them post-upgrade.
 *  2. Copies `<wrapper>.user.name` / `<wrapper>.user.backend_roles` up to top-level
 *     `_migration_user_name` / `_migration_backend_roles` fields so the security plugin's
 *     `POST /_plugins/_security/api/resources/migrate` — which takes a single JSON pointer
 *     for the owner path — can address monitors and workflows in one call.
 *
 * The script `noop`s docs that already have `resource_type` (idempotent re-run) and docs that
 * lack both a `monitor` and `workflow` wrapper (metadata records that aren't shareable).
 * The query pre-filters to docs missing `resource_type` so the noop branch only runs on rare
 * concurrent-write windows.
 *
 * This is a one-shot admin operation. Downstream flow:
 *   POST /_plugins/_alerting/_migrate_to_rsc
 *   POST /_plugins/_security/api/resources/migrate  { ... username_path: "/_migration_user_name" ... }
 */
class TransportMigrateToRscAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
) : HandledTransportAction<MigrateToRscRequest, MigrateToRscResponse>(
    MigrateToRscAction.NAME, transportService, actionFilters, ::MigrateToRscRequest,
) {

    override fun doExecute(task: Task, request: MigrateToRscRequest, actionListener: ActionListener<MigrateToRscResponse>) {
        // Step 1: purge metadata (`<monitorId>-metadata`) docs. They're not shareable resources and
        // the security plugin's `resources/migrate` endpoint scans the entire source index — if any
        // doc's `resource_type` is null it fails the whole call. Metadata is regenerated on next
        // monitor execution, so deletion is safe.
        deleteMetadataDocs(
            onSuccess = { runResourceBackfill(actionListener) },
            onFailure = { e ->
                log.error("Migrate-to-RSC failed while purging metadata docs", e)
                actionListener.onFailure(e)
            },
        )
    }

    private fun deleteMetadataDocs(onSuccess: () -> Unit, onFailure: (Exception) -> Unit) {
        DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
            .source(SCHEDULED_JOBS_INDEX)
            .filter(QueryBuilders.existsQuery("metadata"))
            .refresh(true)
            .abortOnVersionConflict(false)
            .execute(
                object : ActionListener<BulkByScrollResponse> {
                    override fun onResponse(response: BulkByScrollResponse) {
                        log.info("Migrate-to-RSC purged {} metadata docs before backfill", response.deleted)
                        onSuccess()
                    }

                    override fun onFailure(e: Exception) = onFailure(e)
                },
            )
    }

    private fun runResourceBackfill(actionListener: ActionListener<MigrateToRscResponse>) {
        val script = Script(ScriptType.INLINE, "painless", MIGRATION_SCRIPT, emptyMap())

        UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE)
            .source(SCHEDULED_JOBS_INDEX)
            .filter(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("resource_type")))
            .refresh(true)
            .abortOnVersionConflict(false)
            .script(script)
            .execute(
                object : ActionListener<BulkByScrollResponse> {
                    override fun onResponse(response: BulkByScrollResponse) {
                        val failures = (response.bulkFailures?.size ?: 0).toLong() +
                            (response.searchFailures?.size ?: 0).toLong()
                        log.info(
                            "Migrate-to-RSC completed: updated={}, noops={}, failures={}, took={}ms",
                            response.updated,
                            response.noops,
                            failures,
                            response.took.millis,
                        )
                        actionListener.onResponse(
                            MigrateToRscResponse(
                                updated = response.updated,
                                noops = response.noops,
                                failures = failures,
                                tookMillis = response.took.millis,
                            ),
                        )
                    }

                    override fun onFailure(e: Exception) {
                        log.error("Migrate-to-RSC update-by-query failed", e)
                        actionListener.onFailure(e)
                    }
                },
            )
    }

    companion object {
        /**
         * Painless script executed per doc. Reads the wrapper key, sets `resource_type` and copies
         * scratch owner fields; skips docs already migrated or without a shareable wrapper.
         */
        private val MIGRATION_SCRIPT = """
            if (ctx._source.containsKey('resource_type')) { ctx.op = 'noop'; return; }
            def wrapperKey = null;
            if (ctx._source.containsKey('monitor')) { wrapperKey = 'monitor'; }
            else if (ctx._source.containsKey('workflow')) { wrapperKey = 'workflow'; }
            else { ctx.op = 'noop'; return; }
            ctx._source.resource_type = wrapperKey;
            def wrapper = ctx._source[wrapperKey];
            if (wrapper != null && wrapper.user != null) {
                if (wrapper.user.name != null) {
                    ctx._source._migration_user_name = wrapper.user.name;
                }
                if (wrapper.user.backend_roles != null) {
                    ctx._source._migration_backend_roles = wrapper.user.backend_roles;
                }
            }
        """.trimIndent()
    }
}
