/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.alerts

import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALERT_HISTORY_WRITE_INDEX
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALERT_INDEX
import org.opensearch.alerting.elasticapi.suspendUntil
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.Monitor
import org.opensearch.client.Client
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.VersionType
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder

/**
 * Moves defunct active alerts to the alert history index when the corresponding monitor or trigger is deleted.
 *
 * The logic for moving alerts consists of:
 * 1. Find active alerts:
 *      a. matching monitorId if no monitor is provided (postDelete)
 *      b. matching monitorId and no triggerIds if monitor is provided (postIndex)
 * 2. Move alerts over to [ALERT_HISTORY_WRITE_INDEX] as DELETED
 * 3. Delete alerts from [ALERT_INDEX]
 * 4. Schedule a retry if there were any failures
 */
suspend fun moveAlerts(client: Client, monitorId: String, monitor: Monitor? = null) {
    val boolQuery = QueryBuilders.boolQuery()
        .filter(QueryBuilders.termQuery(Alert.MONITOR_ID_FIELD, monitorId))

    if (monitor != null) {
        boolQuery.mustNot(QueryBuilders.termsQuery(Alert.TRIGGER_ID_FIELD, monitor.triggers.map { it.id }))
    }

    val activeAlertsQuery = SearchSourceBuilder.searchSource()
        .query(boolQuery)
        .version(true)

    val activeAlertsRequest = SearchRequest(AlertIndices.ALERT_INDEX)
        .routing(monitorId)
        .source(activeAlertsQuery)
    val response: SearchResponse = client.suspendUntil { search(activeAlertsRequest, it) }

    // If no alerts are found, simply return
    if (response.hits.totalHits?.value == 0L) return
    val indexRequests = response.hits.map { hit ->
        IndexRequest(AlertIndices.ALERT_HISTORY_WRITE_INDEX)
            .routing(monitorId)
            .source(
                Alert.parse(alertContentParser(hit.sourceRef), hit.id, hit.version)
                    .copy(state = Alert.State.DELETED)
                    .toXContentWithUser(XContentFactory.jsonBuilder())
            )
            .version(hit.version)
            .versionType(VersionType.EXTERNAL_GTE)
            .id(hit.id)
    }
    val copyRequest = BulkRequest().add(indexRequests)
    val copyResponse: BulkResponse = client.suspendUntil { bulk(copyRequest, it) }

    val deleteRequests = copyResponse.items.filterNot { it.isFailed }.map {
        DeleteRequest(AlertIndices.ALERT_INDEX, it.id)
            .routing(monitorId)
            .version(it.version)
            .versionType(VersionType.EXTERNAL_GTE)
    }
    val deleteResponse: BulkResponse = client.suspendUntil { bulk(BulkRequest().add(deleteRequests), it) }

    if (copyResponse.hasFailures()) {
        val retryCause = copyResponse.items.filter { it.isFailed }
            .firstOrNull { it.status() == RestStatus.TOO_MANY_REQUESTS }
            ?.failure?.cause
        throw RuntimeException(
            "Failed to copy alerts for [$monitorId, ${monitor?.triggers?.map { it.id }}]: " +
                copyResponse.buildFailureMessage(),
            retryCause
        )
    }
    if (deleteResponse.hasFailures()) {
        val retryCause = deleteResponse.items.filter { it.isFailed }
            .firstOrNull { it.status() == RestStatus.TOO_MANY_REQUESTS }
            ?.failure?.cause
        throw RuntimeException(
            "Failed to delete alerts for [$monitorId, ${monitor?.triggers?.map { it.id }}]: " +
                deleteResponse.buildFailureMessage(),
            retryCause
        )
    }
}

private fun alertContentParser(bytesReference: BytesReference): XContentParser {
    val xcp = XContentHelper.createParser(
        NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
        bytesReference, XContentType.JSON
    )
    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
    return xcp
}
