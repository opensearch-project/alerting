/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.update.UpdateRequest
import org.opensearch.alerting.action.AcknowledgeAlertAction
import org.opensearch.alerting.action.AcknowledgeAlertRequest
import org.opensearch.alerting.action.AcknowledgeAlertResponse
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.elasticapi.optionalTimeField
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.time.Instant

private val log = LogManager.getLogger(TransportAcknowledgeAlertAction::class.java)

class TransportAcknowledgeAlertAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<AcknowledgeAlertRequest, AcknowledgeAlertResponse>(
    AcknowledgeAlertAction.NAME, transportService, actionFilters, ::AcknowledgeAlertRequest
) {

    override fun doExecute(task: Task, request: AcknowledgeAlertRequest, actionListener: ActionListener<AcknowledgeAlertResponse>) {
        client.threadPool().threadContext.stashContext().use {
            AcknowledgeHandler(client, actionListener, request).start()
        }
    }

    inner class AcknowledgeHandler(
        private val client: Client,
        private val actionListener: ActionListener<AcknowledgeAlertResponse>,
        private val request: AcknowledgeAlertRequest
    ) {
        val alerts = mutableMapOf<String, Alert>()

        fun start() = findActiveAlerts()

        private fun findActiveAlerts() {
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

            client.search(
                searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(response: SearchResponse) {
                        onSearchResponse(response)
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        }

        private fun onSearchResponse(response: SearchResponse) {
            val updateRequests = response.hits.flatMap { hit ->
                val xcp = XContentHelper.createParser(
                    xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    hit.sourceRef, XContentType.JSON
                )
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                val alert = Alert.parse(xcp, hit.id, hit.version)
                alerts[alert.id] = alert
                if (alert.state == Alert.State.ACTIVE) {
                    listOf(
                        UpdateRequest(AlertIndices.ALERT_INDEX, hit.id)
                            .routing(request.monitorId)
                            .setIfSeqNo(hit.seqNo)
                            .setIfPrimaryTerm(hit.primaryTerm)
                            .doc(
                                XContentFactory.jsonBuilder().startObject()
                                    .field(Alert.STATE_FIELD, Alert.State.ACKNOWLEDGED.toString())
                                    .optionalTimeField(Alert.ACKNOWLEDGED_TIME_FIELD, Instant.now())
                                    .endObject()
                            )
                    )
                } else {
                    emptyList()
                }
            }

            log.info("Acknowledging monitor: $request.monitorId, alerts: ${updateRequests.map { it.id() }}")
            val bulkRequest = BulkRequest().add(updateRequests).setRefreshPolicy(request.refreshPolicy)
            client.bulk(
                bulkRequest,
                object : ActionListener<BulkResponse> {
                    override fun onResponse(response: BulkResponse) {
                        onBulkResponse(response)
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        }

        private fun onBulkResponse(response: BulkResponse) {
            val missing = request.alertIds.toMutableSet()
            val acknowledged = mutableListOf<Alert>()
            val failed = mutableListOf<Alert>()
            // First handle all alerts that aren't currently ACTIVE. These can't be acknowledged.
            alerts.values.forEach {
                if (it.state != Alert.State.ACTIVE) {
                    missing.remove(it.id)
                    failed.add(it)
                }
            }
            // Now handle all alerts we tried to acknowledge...
            response.items.forEach { item ->
                missing.remove(item.id)
                if (item.isFailed) {
                    failed.add(alerts[item.id]!!)
                } else {
                    acknowledged.add(alerts[item.id]!!)
                }
            }
            actionListener.onResponse(AcknowledgeAlertResponse(acknowledged.toList(), failed.toList(), missing.toList()))
        }
    }
}
