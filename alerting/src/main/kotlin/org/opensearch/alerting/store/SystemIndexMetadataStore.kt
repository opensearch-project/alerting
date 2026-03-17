/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.store

import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.transport.client.Client
import java.time.Instant

/**
 * Plugin-mode implementation of MetadataStore.
 * Delegates to local system indices, preserving existing behavior.
 * The workspaceId parameter is not used in this implementation.
 */
class SystemIndexMetadataStore(
    private val client: Client,
    private val xContentRegistry: NamedXContentRegistry
) : MetadataStore {

    companion object {
        private const val SCHEDULED_JOBS_INDEX = ScheduledJob.SCHEDULED_JOBS_INDEX
    }

    // --- Monitor operations ---

    override suspend fun indexMonitor(
        workspaceId: String,
        monitor: Monitor,
        id: String?,
        seqNo: Long?,
        primaryTerm: Long?,
        refreshPolicy: RefreshPolicy,
        timeout: TimeValue?
    ): IndexMonitorResponse {
        val builder = monitor.toXContentWithUser(
            XContentFactory.jsonBuilder(),
            ToXContent.MapParams(mapOf("with_type" to "true"))
        )
        val indexRequest = IndexRequest(SCHEDULED_JOBS_INDEX)
            .source(builder)
            .setRefreshPolicy(refreshPolicy)
        if (id != null) indexRequest.id(id)
        if (seqNo != null && primaryTerm != null) {
            indexRequest.setIfSeqNo(seqNo).setIfPrimaryTerm(primaryTerm)
        }
        if (timeout != null) indexRequest.timeout(timeout)

        val response: IndexResponse = client.suspendUntil { index(indexRequest, it) }
        return IndexMonitorResponse(response.id, response.version, response.seqNo, response.primaryTerm, monitor)
    }

    override suspend fun getMonitor(workspaceId: String, monitorId: String): GetMonitorResponse? {
        val getRequest = GetRequest(SCHEDULED_JOBS_INDEX, monitorId)
        val response: GetResponse = client.suspendUntil { get(getRequest, it) }

        if (!response.isExists || response.isSourceEmpty) return null

        val monitor = contentParser(response.sourceAsBytesRef).use { xcp ->
            ScheduledJob.parse(xcp, response.id, response.version) as Monitor
        }

        return GetMonitorResponse(response.id, response.version, response.seqNo, response.primaryTerm, monitor)
    }

    override suspend fun deleteMonitor(
        workspaceId: String,
        monitorId: String,
        refreshPolicy: RefreshPolicy
    ): DeleteMonitorResponse {
        val deleteRequest = DeleteRequest(SCHEDULED_JOBS_INDEX, monitorId)
            .setRefreshPolicy(refreshPolicy)
        val response: DeleteResponse = client.suspendUntil { delete(deleteRequest, it) }
        return DeleteMonitorResponse(response.id)
    }

    override suspend fun searchMonitors(workspaceId: String, searchSource: SearchSourceBuilder): SearchMonitorsResponse {
        val searchRequest = SearchRequest(SCHEDULED_JOBS_INDEX).source(searchSource)
        val response: SearchResponse = client.suspendUntil { search(searchRequest, it) }

        val hits = response.hits.hits.mapNotNull { hit ->
            contentParser(hit.sourceRef).use { xcp ->
                val monitor = ScheduledJob.parse(xcp, hit.id, hit.version) as? Monitor ?: return@mapNotNull null
                SearchMonitorsResponse.MonitorHit(hit.id, hit.version, monitor)
            }
        }

        return SearchMonitorsResponse(hits, response.hits.totalHits?.value ?: 0)
    }

    // --- Alert operations ---

    override suspend fun indexAlert(
        workspaceId: String,
        alert: Alert,
        id: String?,
        routingId: String?,
        refreshPolicy: RefreshPolicy
    ): IndexAlertResponse {
        val builder = alert.toXContentWithUser(XContentFactory.jsonBuilder())
        val indexRequest = IndexRequest(AlertIndices.ALERT_INDEX)
            .source(builder)
            .setRefreshPolicy(refreshPolicy)
        if (id != null) indexRequest.id(id)
        if (routingId != null) indexRequest.routing(routingId)

        val response: IndexResponse = client.suspendUntil { index(indexRequest, it) }
        return IndexAlertResponse(response.id, response.version)
    }

    override suspend fun searchAlerts(workspaceId: String, searchSource: SearchSourceBuilder): SearchAlertsResponse {
        val searchRequest = SearchRequest(AlertIndices.ALERT_INDEX).source(searchSource)
        val response: SearchResponse = client.suspendUntil { search(searchRequest, it) }

        val hits = response.hits.hits.map { hit ->
            val alert = Alert.parse(contentParser(hit.sourceRef), hit.id, hit.version)
            SearchAlertsResponse.AlertHit(hit.id, hit.version, alert)
        }

        return SearchAlertsResponse(hits, response.hits.totalHits?.value ?: 0)
    }

    override suspend fun bulkIndexAlerts(
        workspaceId: String,
        alerts: List<Alert>,
        routingId: String?,
        refreshPolicy: RefreshPolicy
    ): BulkIndexAlertsResponse {
        if (alerts.isEmpty()) return BulkIndexAlertsResponse(0, 0)

        val bulkRequest = BulkRequest().setRefreshPolicy(refreshPolicy)
        for (alert in alerts) {
            val builder = alert.toXContentWithUser(XContentFactory.jsonBuilder())
            val indexRequest = IndexRequest(AlertIndices.ALERT_INDEX).source(builder)
            if (alert.id != Alert.NO_ID) indexRequest.id(alert.id)
            if (routingId != null) indexRequest.routing(routingId)
            bulkRequest.add(indexRequest)
        }

        val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
        val failureCount = bulkResponse.items.count { it.isFailed }
        return BulkIndexAlertsResponse(bulkResponse.items.size - failureCount, failureCount)
    }

    override suspend fun deleteAlerts(workspaceId: String, alertIds: List<String>): DeleteAlertsResponse {
        if (alertIds.isEmpty()) return DeleteAlertsResponse(0, 0)

        val bulkRequest = BulkRequest().setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        for (alertId in alertIds) {
            bulkRequest.add(DeleteRequest(AlertIndices.ALERT_INDEX, alertId))
        }

        val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
        val failureCount = bulkResponse.items.count { it.isFailed }
        return DeleteAlertsResponse(bulkResponse.items.size - failureCount, failureCount)
    }

    override suspend fun acknowledgeAlerts(workspaceId: String, alertIds: List<String>): AcknowledgeAlertsResponse {
        val acknowledged = mutableListOf<String>()
        val failed = mutableListOf<String>()
        val currentTime = Instant.now()

        val bulkRequest = BulkRequest().setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        for (alertId in alertIds) {
            try {
                val getResponse: GetResponse = client.suspendUntil { get(GetRequest(AlertIndices.ALERT_INDEX, alertId), it) }
                if (!getResponse.isExists) {
                    failed.add(alertId)
                    continue
                }
                val alert = Alert.parse(contentParser(getResponse.sourceAsBytesRef), getResponse.id, getResponse.version)
                if (alert.state != Alert.State.ACTIVE) {
                    failed.add(alertId)
                    continue
                }
                val acknowledgedAlert = alert.copy(state = Alert.State.ACKNOWLEDGED, acknowledgedTime = currentTime)
                bulkRequest.add(
                    IndexRequest(AlertIndices.ALERT_INDEX)
                        .id(alertId)
                        .source(acknowledgedAlert.toXContentWithUser(XContentFactory.jsonBuilder()))
                )
                acknowledged.add(alertId)
            } catch (e: Exception) {
                failed.add(alertId)
            }
        }

        if (bulkRequest.numberOfActions() > 0) {
            val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
            bulkResponse.items.forEach { item ->
                if (item.isFailed) {
                    acknowledged.remove(item.id)
                    failed.add(item.id)
                }
            }
        }

        return AcknowledgeAlertsResponse(acknowledged, failed)
    }

    // --- Helpers ---

    private fun contentParser(bytesReference: BytesReference): XContentParser {
        val xcp = XContentHelper.createParser(
            xContentRegistry, LoggingDeprecationHandler.INSTANCE,
            bytesReference, XContentType.JSON
        )
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
        return xcp
    }
}
