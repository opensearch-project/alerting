/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.ActionRequest
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.update.UpdateRequest
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.use
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AcknowledgeAlertRequest
import org.opensearch.commons.alerting.action.AcknowledgeAlertResponse
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.GetMonitorRequest
import org.opensearch.commons.alerting.action.GetMonitorResponse
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.util.optionalTimeField
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestRequest
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.time.Instant
import java.util.Locale

private val log = LogManager.getLogger(TransportAcknowledgeAlertAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportAcknowledgeAlertAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    clusterService: ClusterService,
    actionFilters: ActionFilters,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
    val transportGetMonitorAction: TransportGetMonitorAction
) : HandledTransportAction<ActionRequest, AcknowledgeAlertResponse>(
    AlertingActions.ACKNOWLEDGE_ALERTS_ACTION_NAME, transportService, actionFilters, ::AcknowledgeAlertRequest
) {

    @Volatile
    private var isAlertHistoryEnabled = AlertingSettings.ALERT_HISTORY_ENABLED.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.ALERT_HISTORY_ENABLED) { isAlertHistoryEnabled = it }
    }

    override fun doExecute(
        task: Task,
        acknowledgeAlertRequest: ActionRequest,
        actionListener: ActionListener<AcknowledgeAlertResponse>
    ) {
        val request = acknowledgeAlertRequest as? AcknowledgeAlertRequest
            ?: recreateObject(acknowledgeAlertRequest) { AcknowledgeAlertRequest(it) }
        client.threadPool().threadContext.stashContext().use {
            scope.launch {
                val getMonitorResponse: GetMonitorResponse =
                    transportGetMonitorAction.client.suspendUntil {
                        val getMonitorRequest = GetMonitorRequest(
                            monitorId = request.monitorId,
                            -3L,
                            RestRequest.Method.GET,
                            FetchSourceContext.FETCH_SOURCE
                        )
                        execute(AlertingActions.GET_MONITOR_ACTION_TYPE, getMonitorRequest, it)
                    }
                if (getMonitorResponse.monitor == null) {
                    actionListener.onFailure(
                        AlertingException.wrap(
                            ResourceNotFoundException(
                                String.format(
                                    Locale.ROOT,
                                    "No monitor found with id [%s]",
                                    request.monitorId
                                )
                            )
                        )
                    )
                } else {
                    AcknowledgeHandler(client, actionListener, request).start(getMonitorResponse.monitor!!)
                }
            }
        }
    }

    inner class AcknowledgeHandler(
        private val client: Client,
        private val actionListener: ActionListener<AcknowledgeAlertResponse>,
        private val request: AcknowledgeAlertRequest
    ) {
        val alerts = mutableMapOf<String, Alert>()

        suspend fun start(monitor: Monitor) = findActiveAlerts(monitor)

        private suspend fun findActiveAlerts(monitor: Monitor) {
            val queryBuilder = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(Alert.MONITOR_ID_FIELD, request.monitorId))
                .filter(QueryBuilders.termsQuery("_id", request.alertIds))
            val searchRequest = SearchRequest()
                .indices(monitor.dataSources.alertsIndex)
                .routing(request.monitorId)
                .source(
                    SearchSourceBuilder()
                        .query(queryBuilder)
                        .version(true)
                        .seqNoAndPrimaryTerm(true)
                        .size(request.alertIds.size)
                )
            try {
                val searchResponse: SearchResponse = client.suspendUntil { client.search(searchRequest, it) }
                onSearchResponse(searchResponse, monitor)
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun onSearchResponse(response: SearchResponse, monitor: Monitor) {
            val alertsHistoryIndex = monitor.dataSources.alertsHistoryIndex
            val updateRequests = mutableListOf<UpdateRequest>()
            val copyRequests = mutableListOf<IndexRequest>()
            response.hits.forEach { hit ->
                val xcp = XContentHelper.createParser(
                    xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    hit.sourceRef, XContentType.JSON
                )
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                val alert = Alert.parse(xcp, hit.id, hit.version)
                alerts[alert.id] = alert

                if (alert.state == Alert.State.ACTIVE) {
                    if (
                        alert.findingIds.isEmpty() ||
                        !isAlertHistoryEnabled
                    ) {
                        val updateRequest = UpdateRequest(monitor.dataSources.alertsIndex, alert.id)
                            .routing(request.monitorId)
                            .setIfSeqNo(hit.seqNo)
                            .setIfPrimaryTerm(hit.primaryTerm)
                            .doc(
                                XContentFactory.jsonBuilder().startObject()
                                    .field(Alert.STATE_FIELD, Alert.State.ACKNOWLEDGED.toString())
                                    .optionalTimeField(Alert.ACKNOWLEDGED_TIME_FIELD, Instant.now())
                                    .endObject()
                            )
                        updateRequests.add(updateRequest)
                    } else {
                        val copyRequest = IndexRequest(alertsHistoryIndex)
                            .routing(request.monitorId)
                            .id(alert.id)
                            .source(
                                alert.copy(state = Alert.State.ACKNOWLEDGED, acknowledgedTime = Instant.now())
                                    .toXContentWithUser(XContentFactory.jsonBuilder())
                            )
                        copyRequests.add(copyRequest)
                    }
                }
            }

            try {
                val updateResponse: BulkResponse? = if (updateRequests.isNotEmpty())
                    client.suspendUntil {
                        client.bulk(BulkRequest().add(updateRequests).setRefreshPolicy(request.refreshPolicy), it)
                    }
                else null
                val copyResponse: BulkResponse? = if (copyRequests.isNotEmpty())
                    client.suspendUntil {
                        client.bulk(BulkRequest().add(copyRequests).setRefreshPolicy(request.refreshPolicy), it)
                    }
                else null
                onBulkResponse(updateResponse, copyResponse, monitor)
            } catch (t: Exception) {
                log.error("ack error: ${t.message}")
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun onBulkResponse(updateResponse: BulkResponse?, copyResponse: BulkResponse?, monitor: Monitor) {
            val deleteRequests = mutableListOf<DeleteRequest>()
            val missing = request.alertIds.toMutableSet()
            val acknowledged = mutableListOf<Alert>()
            val failed = mutableListOf<Alert>()

            alerts.values.forEach {
                if (it.state != Alert.State.ACTIVE) {
                    missing.remove(it.id)
                    failed.add(it)
                }
            }

            updateResponse?.items?.forEach { item ->
                missing.remove(item.id)
                if (item.isFailed) {
                    failed.add(alerts[item.id]!!)
                } else {
                    acknowledged.add(alerts[item.id]!!)
                }
            }

            copyResponse?.items?.forEach { item ->
                log.info("got a copyResponse: $item")
                missing.remove(item.id)
                if (item.isFailed) {
                    log.info("got a failureResponse: ${item.failureMessage}")
                    failed.add(alerts[item.id]!!)
                } else {
                    val deleteRequest = DeleteRequest(monitor.dataSources.alertsIndex, item.id)
                        .routing(request.monitorId)
                    deleteRequests.add(deleteRequest)
                }
            }

            if (deleteRequests.isNotEmpty()) {
                try {
                    val deleteResponse: BulkResponse = client.suspendUntil {
                        client.bulk(BulkRequest().add(deleteRequests).setRefreshPolicy(request.refreshPolicy), it)
                    }
                    deleteResponse.items.forEach { item ->
                        missing.remove(item.id)
                        if (item.isFailed) {
                            failed.add(alerts[item.id]!!)
                        } else {
                            acknowledged.add(alerts[item.id]!!)
                        }
                    }
                } catch (t: Exception) {
                    actionListener.onFailure(AlertingException.wrap(t))
                    return
                }
            }
            actionListener.onResponse(AcknowledgeAlertResponse(acknowledged.toList(), failed.toList(), missing.toList()))
        }
    }
}
