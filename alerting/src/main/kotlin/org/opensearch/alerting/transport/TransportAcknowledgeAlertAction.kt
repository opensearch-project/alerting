/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.update.UpdateRequest
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AcknowledgeAlertRequest
import org.opensearch.commons.alerting.action.AcknowledgeAlertResponse
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.util.optionalTimeField
import org.opensearch.commons.utils.recreateObject
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.time.Instant

private val log = LogManager.getLogger(TransportAcknowledgeAlertAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportAcknowledgeAlertAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    clusterService: ClusterService,
    actionFilters: ActionFilters,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<AcknowledgeAlertRequest, AcknowledgeAlertResponse>(
    AlertingActions.ACKNOWLEDGE_ALERTS_ACTION_NAME, transportService, actionFilters, ::AcknowledgeAlertRequest
) {

    @Volatile private var isAlertHistoryEnabled = AlertingSettings.ALERT_HISTORY_ENABLED.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.ALERT_HISTORY_ENABLED) { isAlertHistoryEnabled = it }
    }

    override fun doExecute(
        task: Task,
        acknowledgeAlertRequest: AcknowledgeAlertRequest,
        actionListener: ActionListener<AcknowledgeAlertResponse>
    ) {
        val request = acknowledgeAlertRequest as? AcknowledgeAlertRequest
            ?: recreateObject(acknowledgeAlertRequest) { AcknowledgeAlertRequest(it) }
        client.threadPool().threadContext.stashContext().use {
            scope.launch {
                AcknowledgeHandler(client, actionListener, request).start()
            }
        }
    }

    inner class AcknowledgeHandler(
        private val client: Client,
        private val actionListener: ActionListener<AcknowledgeAlertResponse>,
        private val request: AcknowledgeAlertRequest
    ) {
        val alerts = mutableMapOf<String, Alert>()

        suspend fun start() = findActiveAlerts()

        private suspend fun findActiveAlerts() {
            val queryBuilder = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(Alert.MONITOR_ID_FIELD, request.monitorId))
                .filter(QueryBuilders.termsQuery("_id", request.alertIds))
            val searchRequest = SearchRequest()
                .indices(AlertIndices.ALERT_INDEX)
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
                onSearchResponse(searchResponse)
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun onSearchResponse(response: SearchResponse) {
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
                    if (alert.findingIds.isEmpty() || !isAlertHistoryEnabled) {
                        val updateRequest = UpdateRequest(AlertIndices.ALERT_INDEX, alert.id)
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
                        val copyRequest = IndexRequest(AlertIndices.ALERT_HISTORY_WRITE_INDEX)
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
                onBulkResponse(updateResponse, copyResponse)
            } catch (t: Exception) {
                log.error("ack error: ${t.message}")
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun onBulkResponse(updateResponse: BulkResponse?, copyResponse: BulkResponse?) {
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
                    val deleteRequest = DeleteRequest(AlertIndices.ALERT_INDEX, item.id)
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
