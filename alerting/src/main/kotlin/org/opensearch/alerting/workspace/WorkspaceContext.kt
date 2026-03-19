/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.workspace

/**
 * Immutable value object carrying tenancy information extracted from the
 * request context. Propagated through the entire request lifecycle.
 *
 * @property workspaceId Workspace identifier for document-level isolation within a tenant.
 *                       Used by the plugin to filter reads and stamp writes.
 * @property tenantId Tenant identifier for storage-level isolation.
 *                    Passed to the Remote Metadata SDK on every persistence operation.
 */
data class WorkspaceContext(
    val workspaceId: String,
    val tenantId: String
) {
    companion object {
        const val HEADER_WORKSPACE_ID = "_workspace_id"
        const val HEADER_TENANT_ID = "_tenant_id"
    }
}
