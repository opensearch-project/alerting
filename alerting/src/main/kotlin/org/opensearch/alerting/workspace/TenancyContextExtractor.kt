/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.workspace

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.rest.RestStatus
import org.opensearch.threadpool.ThreadPool

/**
 * Extracts workspace/tenancy information from the OpenSearch ThreadContext
 * headers propagated by the hosting runtime.
 *
 * Returns null when workspace isolation is disabled (backward compatibility).
 * Throws BAD_REQUEST when isolation is enabled but required headers are missing
 * (missing headers indicate an infrastructure/wiring problem, not a permissions issue).
 */
class TenancyContextExtractor(
    private val threadPool: ThreadPool,
    private val isWorkspaceIsolationEnabled: () -> Boolean
) {

    companion object {
        private val log = LogManager.getLogger(TenancyContextExtractor::class.java)
    }

    /**
     * Extract WorkspaceContext from the current thread context.
     * @return WorkspaceContext if workspace isolation is enabled and headers are present, null otherwise.
     * @throws AlertingException with BAD_REQUEST status if isolation is enabled but required headers are missing.
     */
    fun extract(): WorkspaceContext? {
        if (!isWorkspaceIsolationEnabled()) {
            return null
        }

        val threadContext = threadPool.threadContext

        val workspaceId = threadContext.getTransient<String>(WorkspaceContext.HEADER_WORKSPACE_ID)
        val tenantId = threadContext.getTransient<String>(WorkspaceContext.HEADER_TENANT_ID)

        if (workspaceId.isNullOrBlank() || tenantId.isNullOrBlank()) {
            log.error(
                "Workspace isolation is enabled but required headers are missing. " +
                    "workspace_id=${workspaceId.isNullOrBlank()}, tenant_id=${tenantId.isNullOrBlank()}"
            )
            throw AlertingException.wrap(
                OpenSearchStatusException(
                    "Missing required tenancy context. Ensure workspace_id and tenant_id are propagated.",
                    RestStatus.BAD_REQUEST
                )
            )
        }

        log.debug("Extracted workspace context: workspaceId=$workspaceId, tenantId=$tenantId")
        return WorkspaceContext(
            workspaceId = workspaceId,
            tenantId = tenantId
        )
    }
}
