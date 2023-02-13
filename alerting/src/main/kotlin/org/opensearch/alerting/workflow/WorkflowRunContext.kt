/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.workflow

data class WorkflowRunContext(
    val chainedMonitorId: String?,
    val workflowExecutionId: String,
    val indexToDocIds: Map<String, List<String>>
)
