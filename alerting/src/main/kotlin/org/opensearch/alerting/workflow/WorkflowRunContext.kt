/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.workflow

data class WorkflowRunContext(
    // In case of dry run it's random generated id, while in other cases it's workflowId
    val workflowId: String,
    val workflowMetadataId: String,
    val chainedMonitorId: String?,
    val executionId: String,
    val matchingDocIdsPerIndex: Map<String, List<String>>
)
