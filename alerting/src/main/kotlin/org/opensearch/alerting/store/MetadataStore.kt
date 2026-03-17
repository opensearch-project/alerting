/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.store

import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.search.builder.SearchSourceBuilder

/**
 * Abstraction that decouples the alerting plugin from its storage backend.
 *
 * In plugin mode, backed by local system indices.
 * In remote mode, backed by saved-object storage.
 */
interface MetadataStore {

    // --- Monitor operations ---

    suspend fun indexMonitor(
        workspaceId: String,
        monitor: Monitor,
        id: String? = null,
        seqNo: Long? = null,
        primaryTerm: Long? = null,
        refreshPolicy: RefreshPolicy = RefreshPolicy.IMMEDIATE,
        timeout: TimeValue? = null
    ): IndexMonitorResponse

    suspend fun getMonitor(workspaceId: String, monitorId: String): GetMonitorResponse?

    suspend fun deleteMonitor(
        workspaceId: String,
        monitorId: String,
        refreshPolicy: RefreshPolicy = RefreshPolicy.IMMEDIATE
    ): DeleteMonitorResponse

    suspend fun searchMonitors(workspaceId: String, searchSource: SearchSourceBuilder): SearchMonitorsResponse

    // --- Alert operations ---

    suspend fun indexAlert(
        workspaceId: String,
        alert: Alert,
        id: String? = null,
        routingId: String? = null,
        refreshPolicy: RefreshPolicy = RefreshPolicy.IMMEDIATE
    ): IndexAlertResponse

    suspend fun searchAlerts(workspaceId: String, searchSource: SearchSourceBuilder): SearchAlertsResponse

    suspend fun bulkIndexAlerts(
        workspaceId: String,
        alerts: List<Alert>,
        routingId: String? = null,
        refreshPolicy: RefreshPolicy = RefreshPolicy.IMMEDIATE
    ): BulkIndexAlertsResponse

    suspend fun deleteAlerts(workspaceId: String, alertIds: List<String>): DeleteAlertsResponse

    suspend fun acknowledgeAlerts(workspaceId: String, alertIds: List<String>): AcknowledgeAlertsResponse
}

// --- Response types ---

data class IndexMonitorResponse(
    val id: String,
    val version: Long,
    val seqNo: Long,
    val primaryTerm: Long,
    val monitor: Monitor
)

data class GetMonitorResponse(
    val id: String,
    val version: Long,
    val seqNo: Long,
    val primaryTerm: Long,
    val monitor: Monitor
)

data class DeleteMonitorResponse(val id: String)

data class SearchMonitorsResponse(val hits: List<MonitorHit>, val totalHits: Long) {
    data class MonitorHit(val id: String, val version: Long, val monitor: Monitor)
}

data class IndexAlertResponse(val id: String, val version: Long)

data class SearchAlertsResponse(val hits: List<AlertHit>, val totalHits: Long) {
    data class AlertHit(val id: String, val version: Long, val alert: Alert)
}

data class BulkIndexAlertsResponse(val successCount: Int, val failureCount: Int)

data class DeleteAlertsResponse(val deleted: Int, val failed: Int)

data class AcknowledgeAlertsResponse(val acknowledged: List<String>, val failed: List<String>)
