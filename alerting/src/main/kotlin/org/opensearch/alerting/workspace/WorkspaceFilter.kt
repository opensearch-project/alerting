/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.workspace

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder

/**
 * Implements workspace isolation patterns for alerting CRUD and search operations.
 *
 * Four tenancy patterns:
 * 1. Stamp — inject workspace_id into a document at create time
 * 2. Filter — inject workspace_id term filter into search queries
 * 3. Ownership Check — verify a fetched document belongs to the requesting workspace
 * 4. App-Scoped — filter by app_arn for shared resources (destinations, channels)
 *
 * All methods are no-ops when WorkspaceContext is null (backward compatibility).
 */
object WorkspaceFilter {

    const val WORKSPACE_ID_FIELD = "workspace_id"

    private val log = LogManager.getLogger(WorkspaceFilter::class.java)

    /**
     * Filter Pattern: Inject a workspace_id term filter into a search query.
     * The original query semantics are preserved — moved into a bool.must clause,
     * with the workspace filter in bool.filter (does not affect scoring).
     *
     * No-op when context is null.
     */
    fun applySearchFilter(
        searchSourceBuilder: SearchSourceBuilder,
        context: WorkspaceContext?
    ): SearchSourceBuilder {
        if (context == null) return searchSourceBuilder

        val workspaceTermFilter = QueryBuilders.termQuery(WORKSPACE_ID_FIELD, context.workspaceId)
        val existingQuery = searchSourceBuilder.query()

        val filteredQuery = if (existingQuery != null) {
            QueryBuilders.boolQuery()
                .must(existingQuery)
                .filter(workspaceTermFilter)
        } else {
            QueryBuilders.boolQuery()
                .filter(workspaceTermFilter)
        }

        searchSourceBuilder.query(filteredQuery)
        log.debug("Applied workspace filter for workspaceId=${context.workspaceId}")
        return searchSourceBuilder
    }

    /**
     * Ownership Check: Verify that a fetched document's workspace_id matches
     * the requesting workspace. Returns true if they match.
     *
     * Returns false if the document has no workspace_id (legacy document).
     * Always returns true when context is null (isolation disabled).
     */
    fun verifyOwnership(documentWorkspaceId: String?, context: WorkspaceContext?): Boolean {
        if (context == null) return true
        if (documentWorkspaceId == null) return false
        return documentWorkspaceId == context.workspaceId
    }

    /**
     * Throws NOT_FOUND if ownership check fails. Uses NOT_FOUND instead of
     * FORBIDDEN to prevent information leakage about resource existence.
     */
    fun requireOwnership(documentWorkspaceId: String?, context: WorkspaceContext?, resourceType: String, resourceId: String) {
        if (!verifyOwnership(documentWorkspaceId, context)) {
            throw AlertingException.wrap(
                OpenSearchStatusException(
                    "$resourceType $resourceId not found",
                    RestStatus.NOT_FOUND
                )
            )
        }
    }

    /**
     * Validates that a workspace_id is not being changed on update.
     * workspace_id is immutable once stamped at creation.
     *
     * No-op when context is null.
     */
    fun validateWorkspaceIdImmutable(existingWorkspaceId: String?, context: WorkspaceContext?) {
        if (context == null) return
        if (existingWorkspaceId != null && existingWorkspaceId != context.workspaceId) {
            throw AlertingException.wrap(
                OpenSearchStatusException(
                    "Cannot change workspace_id of existing resource",
                    RestStatus.FORBIDDEN
                )
            )
        }
    }
}
