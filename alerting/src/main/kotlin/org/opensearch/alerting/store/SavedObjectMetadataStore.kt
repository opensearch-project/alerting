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
 * Remote-mode implementation of MetadataStore.
 * Delegates persistence to the saved-object storage backend.
 * Workspace isolation is enforced by the storage layer.
 */
class SavedObjectMetadataStore : MetadataStore {

    override suspend fun indexMonitor(
        workspaceId: String,
        monitor: Monitor,
        id: String?,
        seqNo: Long?,
        primaryTerm: Long?,
        refreshPolicy: RefreshPolicy,
        timeout: TimeValue?
    ): IndexMonitorResponse {
        TODO("Remote mode implementation")
    }

    override suspend fun getMonitor(workspaceId: String, monitorId: String): GetMonitorResponse? {
        TODO("Remote mode implementation")
    }

    override suspend fun deleteMonitor(
        workspaceId: String,
        monitorId: String,
        refreshPolicy: RefreshPolicy
    ): DeleteMonitorResponse {
        TODO("Remote mode implementation")
    }

    override suspend fun searchMonitors(workspaceId: String, searchSource: SearchSourceBuilder): SearchMonitorsResponse {
        TODO("Remote mode implementation")
    }

    override suspend fun indexAlert(
        workspaceId: String,
        alert: Alert,
        id: String?,
        routingId: String?,
        refreshPolicy: RefreshPolicy
    ): IndexAlertResponse {
        TODO("Remote mode implementation")
    }

    override suspend fun searchAlerts(workspaceId: String, searchSource: SearchSourceBuilder): SearchAlertsResponse {
        TODO("Remote mode implementation")
    }

    override suspend fun bulkIndexAlerts(
        workspaceId: String,
        alerts: List<Alert>,
        routingId: String?,
        refreshPolicy: RefreshPolicy
    ): BulkIndexAlertsResponse {
        TODO("Remote mode implementation")
    }

    override suspend fun deleteAlerts(workspaceId: String, alertIds: List<String>): DeleteAlertsResponse {
        TODO("Remote mode implementation")
    }

    override suspend fun acknowledgeAlerts(workspaceId: String, alertIds: List<String>): AcknowledgeAlertsResponse {
        TODO("Remote mode implementation")
    }
}
