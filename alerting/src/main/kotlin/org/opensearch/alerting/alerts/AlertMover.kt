/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.alerts

import org.apache.logging.log4j.LogManager
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.MonitorRunnerExecutionContext
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALERT_HISTORY_WRITE_INDEX
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALERT_INDEX
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.util.ScheduledJobUtils
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.CompositeInput
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.VersionType
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.transport.client.Client

private val log = LogManager.getLogger(AlertMover::class.java)

class AlertMover {
    companion object {
        /**
         * Moves defunct active alerts to the alert history index when the corresponding monitor or trigger is deleted.
         *
         * The logic for moving alerts consists of:
         * 1. Find active alerts:
         *      a. matching monitorId if no monitor is provided (postDelete)
         *      b. matching monitorId and no triggerIds if monitor is provided (postIndex)
         * 2. Move alerts over to DataSources.alertsHistoryIndex as DELETED
         * 3. Delete alerts from monitor's DataSources.alertsIndex
         * 4. Schedule a retry if there were any failures
         */
        suspend fun moveAlerts(client: Client, monitorId: String, monitor: Monitor?) {
            var alertIndex = monitor?.dataSources?.alertsIndex ?: ALERT_INDEX
            var alertHistoryIndex = monitor?.dataSources?.alertsHistoryIndex ?: ALERT_HISTORY_WRITE_INDEX

            val boolQuery = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(Alert.MONITOR_ID_FIELD, monitorId))

            if (monitor != null) {
                boolQuery.mustNot(QueryBuilders.termsQuery(Alert.TRIGGER_ID_FIELD, monitor.triggers.map { it.id }))
            }

            val activeAlertsQuery = SearchSourceBuilder.searchSource()
                .query(boolQuery)
                .version(true)

            val activeAlertsRequest = SearchRequest(alertIndex)
                .routing(monitorId)
                .source(activeAlertsQuery)
            val response: SearchResponse = client.suspendUntil { search(activeAlertsRequest, it) }

            // If no alerts are found, simply return
            if (response.hits.totalHits?.value == 0L) return
            val indexRequests = response.hits.map { hit ->
                IndexRequest(alertHistoryIndex)
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
                DeleteRequest(alertIndex, it.id)
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

        /**
         * Moves defunct active alerts to the alert history index when the corresponding workflow or trigger is deleted.
         *
         * The logic for moving alerts consists of:
         * 1. Find active alerts:
         *      a. matching workflowId if no workflow is provided (postDelete)
         *      b. matching workflowid and chained alert triggerIds if monitor is provided (postIndex)
         * 2. Move alerts over to DataSources.alertsHistoryIndex as DELETED
         * 3. Delete alerts from monitor's DataSources.alertsIndex
         * 4. Schedule a retry if there were any failures
         */
        suspend fun moveAlerts(client: Client, workflowId: String, workflow: Workflow?, monitorCtx: MonitorRunnerExecutionContext) {
            var alertIndex = ALERT_INDEX
            var alertHistoryIndex = ALERT_HISTORY_WRITE_INDEX
            if (workflow != null) {
                if (
                    workflow.inputs.isNotEmpty() && workflow.inputs[0] is CompositeInput &&
                    (workflow.inputs[0] as CompositeInput).sequence.delegates.isNotEmpty()
                ) {
                    var i = 0
                    val delegates = (workflow.inputs[0] as CompositeInput).sequence.delegates
                    try {
                        var getResponse: GetResponse? = null
                        while (i < delegates.size && (getResponse == null || getResponse.isExists == false)) {
                            getResponse =
                                client.suspendUntil {
                                    client.get(
                                        GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, delegates[i].monitorId),
                                        it
                                    )
                                }
                            if (getResponse!!.isExists) {
                                val monitor =
                                    ScheduledJobUtils.parseMonitorFromScheduledJobDocSource(
                                        monitorCtx.xContentRegistry!!,
                                        response = getResponse
                                    )

                                alertIndex = monitor.dataSources.alertsIndex
                                alertHistoryIndex =
                                    if (monitor.dataSources.alertsHistoryIndex == null) alertHistoryIndex
                                    else monitor.dataSources.alertsHistoryIndex!!
                            }
                            i++
                        }
                    } catch (e: Exception) {
                        log.error("Failed to get delegate monitor for workflow $workflowId. Assuming default alert indices", e)
                    }
                }
            }
            val dataSources = DataSources().copy(alertsHistoryIndex = alertHistoryIndex, alertsIndex = alertIndex)
            /** check if alert index is initialized **/
            if (monitorCtx.alertIndices!!.isAlertInitialized(dataSources) == false)
                return
            val boolQuery = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery(Alert.WORKFLOW_ID_FIELD, workflowId))

            if (workflow != null) {
                boolQuery.mustNot(QueryBuilders.termsQuery(Alert.TRIGGER_ID_FIELD, workflow.triggers.map { it.id }))
            }

            val activeAlertsQuery = SearchSourceBuilder.searchSource()
                .query(boolQuery)
                .version(true)

            val activeAlertsRequest = SearchRequest(alertIndex)
                .routing(workflowId)
                .source(activeAlertsQuery)
            val response: SearchResponse = client.suspendUntil { search(activeAlertsRequest, it) }

            // If no alerts are found, simply return
            if (response.hits.totalHits?.value == 0L) return
            val indexRequests = response.hits.map { hit ->
                IndexRequest(alertHistoryIndex)
                    .routing(workflowId)
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
                DeleteRequest(alertIndex, it.id)
                    .routing(workflowId)
                    .version(it.version)
                    .versionType(VersionType.EXTERNAL_GTE)
            }
            val deleteResponse: BulkResponse = client.suspendUntil { bulk(BulkRequest().add(deleteRequests), it) }

            if (copyResponse.hasFailures()) {
                val retryCause = copyResponse.items.filter { it.isFailed }
                    .firstOrNull { it.status() == RestStatus.TOO_MANY_REQUESTS }
                    ?.failure?.cause
                throw RuntimeException(
                    "Failed to copy alerts for [$workflowId, ${workflow?.triggers?.map { it.id }}]: " +
                        copyResponse.buildFailureMessage(),
                    retryCause
                )
            }
            if (deleteResponse.hasFailures()) {
                val retryCause = deleteResponse.items.filter { it.isFailed }
                    .firstOrNull { it.status() == RestStatus.TOO_MANY_REQUESTS }
                    ?.failure?.cause
                throw RuntimeException(
                    "Failed to delete alerts for [$workflowId, ${workflow?.triggers?.map { it.id }}]: " +
                        deleteResponse.buildFailureMessage(),
                    retryCause
                )
            }
        }
    }
}
