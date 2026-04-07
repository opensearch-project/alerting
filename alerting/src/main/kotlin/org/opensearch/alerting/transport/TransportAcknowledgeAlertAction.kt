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
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.await
import org.opensearch.alerting.util.use
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AcknowledgeAlertRequest
import org.opensearch.commons.alerting.action.AcknowledgeAlertResponse
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.GetMonitorRequest
import org.opensearch.commons.alerting.action.GetMonitorResponse
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.alerting.util.optionalTimeField
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.QueryBuilders
import org.opensearch.remote.metadata.client.BulkDataObjectRequest
import org.opensearch.remote.metadata.client.BulkDataObjectResponse
import org.opensearch.remote.metadata.client.DeleteDataObjectRequest
import org.opensearch.remote.metadata.client.PutDataObjectRequest
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.remote.metadata.client.SearchDataObjectRequest
import org.opensearch.remote.metadata.client.UpdateDataObjectRequest
import org.opensearch.rest.RestRequest
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
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
    val transportGetMonitorAction: TransportGetMonitorAction,
    val sdkClient: SdkClient
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
            val searchSourceBuilder = SearchSourceBuilder()
                .query(queryBuilder)
                .version(true)
                .seqNoAndPrimaryTerm(true)
                .size(request.alertIds.size)
            val sdkSearchRequest = SearchDataObjectRequest.builder()
                .indices(monitor.dataSources.alertsIndex)
                .routing(request.monitorId)
                .searchSourceBuilder(searchSourceBuilder)
                .build()
            try {
                val searchResponse = sdkClient.searchDataObjectAsync(sdkSearchRequest).await()
                    .searchResponse() ?: throw RuntimeException("Unknown error loading alerts")
                onSearchResponse(searchResponse, monitor)
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun onSearchResponse(response: SearchResponse, monitor: Monitor) {
            val alertsHistoryIndex = monitor.dataSources.alertsHistoryIndex
            val updateRequests = mutableListOf<UpdateDataObjectRequest>()
            val copyRequests = mutableListOf<PutDataObjectRequest>()
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
                        updateRequests.add(
                            UpdateDataObjectRequest.builder()
                                .index(monitor.dataSources.alertsIndex)
                                .id(alert.id)
                                .routing(request.monitorId)
                                .ifSeqNo(hit.seqNo)
                                .ifPrimaryTerm(hit.primaryTerm)
                                .dataObject(
                                    ToXContentObject { builder, _ ->
                                        builder.startObject()
                                            .field(Alert.STATE_FIELD, Alert.State.ACKNOWLEDGED.toString())
                                            .optionalTimeField(Alert.ACKNOWLEDGED_TIME_FIELD, Instant.now())
                                            .endObject()
                                    }
                                )
                                .build()
                        )
                    } else {
                        val ackedAlert = alert.copy(state = Alert.State.ACKNOWLEDGED, acknowledgedTime = Instant.now())
                        copyRequests.add(
                            PutDataObjectRequest.builder()
                                .index(alertsHistoryIndex)
                                .id(alert.id)
                                .routing(request.monitorId)
                                .overwriteIfExists(true)
                                .dataObject(ToXContentObject { builder, _ -> ackedAlert.toXContentWithUser(builder) })
                                .build()
                        )
                    }
                }
            }

            try {
                val updateResponse = if (updateRequests.isNotEmpty()) {
                    val bulkRequest = BulkDataObjectRequest(null)
                    updateRequests.forEach { bulkRequest.add(it) }
                    sdkClient.bulkDataObjectAsync(bulkRequest).await()
                } else null
                val copyResponse = if (copyRequests.isNotEmpty()) {
                    val bulkRequest = BulkDataObjectRequest(null)
                    copyRequests.forEach { bulkRequest.add(it) }
                    sdkClient.bulkDataObjectAsync(bulkRequest).await()
                } else null
                onBulkResponse(updateResponse, copyResponse, monitor)
            } catch (t: Exception) {
                log.error("ack error: ${t.message}")
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun onBulkResponse(
            updateResponse: BulkDataObjectResponse?,
            copyResponse: BulkDataObjectResponse?,
            monitor: Monitor
        ) {
            val deleteRequests = mutableListOf<DeleteDataObjectRequest>()
            val missing = request.alertIds.toMutableSet()
            val acknowledged = mutableListOf<Alert>()
            val failed = mutableListOf<Alert>()

            alerts.values.forEach {
                if (it.state != Alert.State.ACTIVE) {
                    missing.remove(it.id)
                    failed.add(it)
                }
            }

            updateResponse?.responses?.forEach { item ->
                missing.remove(item.id())
                if (item.isFailed) {
                    failed.add(alerts[item.id()]!!)
                } else {
                    acknowledged.add(alerts[item.id()]!!)
                }
            }

            copyResponse?.responses?.forEach { item ->
                log.info("got a copyResponse: $item")
                missing.remove(item.id())
                if (item.isFailed) {
                    log.info("got a failureResponse: ${item.cause()?.message}")
                    failed.add(alerts[item.id()]!!)
                } else {
                    deleteRequests.add(
                        DeleteDataObjectRequest.builder()
                            .index(monitor.dataSources.alertsIndex)
                            .id(item.id())
                            .routing(request.monitorId)
                            .build()
                    )
                }
            }

            if (deleteRequests.isNotEmpty()) {
                try {
                    val bulkRequest = BulkDataObjectRequest(null)
                    deleteRequests.forEach { bulkRequest.add(it) }
                    val deleteResponse = sdkClient.bulkDataObjectAsync(bulkRequest).await()
                    deleteResponse.responses.forEach { item ->
                        missing.remove(item.id())
                        if (item.isFailed) {
                            failed.add(alerts[item.id()]!!)
                        } else {
                            acknowledged.add(alerts[item.id()]!!)
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
