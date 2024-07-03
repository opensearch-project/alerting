/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.ActionRequest
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest
import org.opensearch.action.update.UpdateRequest
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.ScheduledJobUtils
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AcknowledgeAlertResponse
import org.opensearch.commons.alerting.action.AcknowledgeChainedAlertRequest
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.CompositeInput
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.alerting.util.optionalTimeField
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.time.Instant
import java.util.Locale

private val log = LogManager.getLogger(TransportAcknowledgeChainedAlertAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportAcknowledgeChainedAlertAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    clusterService: ClusterService,
    actionFilters: ActionFilters,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
) : HandledTransportAction<ActionRequest, AcknowledgeAlertResponse>(
    AlertingActions.ACKNOWLEDGE_CHAINED_ALERTS_ACTION_NAME,
    transportService,
    actionFilters,
    ::AcknowledgeChainedAlertRequest
) {
    @Volatile
    private var isAlertHistoryEnabled = AlertingSettings.ALERT_HISTORY_ENABLED.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.ALERT_HISTORY_ENABLED) { isAlertHistoryEnabled = it }
    }

    override fun doExecute(
        task: Task,
        AcknowledgeChainedAlertRequest: ActionRequest,
        actionListener: ActionListener<AcknowledgeAlertResponse>,
    ) {
        val request = AcknowledgeChainedAlertRequest as? AcknowledgeChainedAlertRequest
            ?: recreateObject(AcknowledgeChainedAlertRequest) { AcknowledgeChainedAlertRequest(it) }
        client.threadPool().threadContext.stashContext().use {
            scope.launch {
                try {
                    val getResponse = getWorkflow(request.workflowId)
                    if (getResponse.isExists == false) {
                        actionListener.onFailure(
                            AlertingException.wrap(
                                ResourceNotFoundException(
                                    String.format(
                                        Locale.ROOT,
                                        "No workflow found with id [%s]",
                                        request.workflowId
                                    )
                                )
                            )
                        )
                    } else {
                        val workflow = ScheduledJobUtils.parseWorkflowFromScheduledJobDocSource(xContentRegistry, getResponse)
                        AcknowledgeHandler(client, actionListener, request).start(workflow = workflow)
                    }
                } catch (e: Exception) {
                    log.error("Failed to acknowledge chained alerts from request $request", e)
                    actionListener.onFailure(AlertingException.wrap(e))
                }
            }
        }
    }

    private suspend fun getWorkflow(workflowId: String): GetResponse {
        return client.suspendUntil { client.get(GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, workflowId), it) }
    }

    inner class AcknowledgeHandler(
        private val client: Client,
        private val actionListener: ActionListener<AcknowledgeAlertResponse>,
        private val request: AcknowledgeChainedAlertRequest,
    ) {
        val alerts = mutableMapOf<String, Alert>()

        suspend fun start(workflow: Workflow) = findActiveAlerts(workflow)

        private suspend fun findActiveAlerts(workflow: Workflow) {
            try {
                val queryBuilder = QueryBuilders.boolQuery()
                    .must(
                        QueryBuilders.wildcardQuery("workflow_id", request.workflowId)
                    )
                    .must(QueryBuilders.termsQuery("_id", request.alertIds))
                if (workflow.inputs.isEmpty() || (workflow.inputs[0] is CompositeInput) == false) {
                    actionListener.onFailure(
                        OpenSearchStatusException("Workflow ${workflow.id} is invalid", RestStatus.INTERNAL_SERVER_ERROR)
                    )
                    return
                }
                val compositeInput = workflow.inputs[0] as CompositeInput
                val workflowId = compositeInput.sequence.delegates[0].monitorId
                val dataSources: DataSources = getDataSources(workflowId)
                val searchRequest = SearchRequest()
                    .indices(dataSources.alertsIndex)
                    .routing(request.workflowId)
                    .source(
                        SearchSourceBuilder()
                            .query(queryBuilder)
                            .version(true)
                            .seqNoAndPrimaryTerm(true)
                            .size(request.alertIds.size)
                    )

                val searchResponse: SearchResponse = client.suspendUntil { client.search(searchRequest, it) }
                onSearchResponse(searchResponse, workflow, dataSources)
            } catch (t: Exception) {
                log.error("Failed to acknowledge chained alert ${request.alertIds} for workflow ${request.workflowId}", t)
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun getDataSources(monitorId: String): DataSources {
            val getResponse: GetResponse = client.suspendUntil { client.get(GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, monitorId), it) }
            return ScheduledJobUtils.parseMonitorFromScheduledJobDocSource(xContentRegistry, getResponse).dataSources
        }

        private suspend fun onSearchResponse(response: SearchResponse, workflow: Workflow, dataSources: DataSources) {
            val alertsHistoryIndex = dataSources.alertsHistoryIndex
            val updateRequests = mutableListOf<UpdateRequest>()
            val copyRequests = mutableListOf<IndexRequest>()
            response.hits.forEach { hit ->
                val xcp = XContentHelper.createParser(
                    xContentRegistry,
                    LoggingDeprecationHandler.INSTANCE,
                    hit.sourceRef,
                    XContentType.JSON
                )
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                val alert = Alert.parse(xcp, hit.id, hit.version)
                alerts[alert.id] = alert

                if (alert.state == Alert.State.ACTIVE) {
                    if (
                        alert.findingIds.isEmpty() ||
                        !isAlertHistoryEnabled
                    ) {
                        val updateRequest = UpdateRequest(dataSources.alertsIndex, alert.id)
                            .routing(request.workflowId)
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
                            .routing(request.workflowId)
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
                val updateResponse: BulkResponse? = if (updateRequests.isNotEmpty()) {
                    client.suspendUntil {
                        client.bulk(BulkRequest().add(updateRequests).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), it)
                    }
                } else null
                val copyResponse: BulkResponse? = if (copyRequests.isNotEmpty()) {
                    client.suspendUntil {
                        client.bulk(BulkRequest().add(copyRequests).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), it)
                    }
                } else null
                onBulkResponse(updateResponse, copyResponse, dataSources)
            } catch (t: Exception) {
                log.error("Failed to acknowledge chained alert ${request.alertIds} for workflow ${request.workflowId}", t)
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun onBulkResponse(updateResponse: BulkResponse?, copyResponse: BulkResponse?, dataSources: DataSources) {
            val deleteRequests = mutableListOf<DeleteRequest>()
            val acknowledged = mutableListOf<Alert>()
            val missing = request.alertIds.toMutableSet()
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
                    val deleteRequest = DeleteRequest(dataSources.alertsIndex, item.id)
                        .routing(request.workflowId)
                    deleteRequests.add(deleteRequest)
                }
            }

            if (deleteRequests.isNotEmpty()) {
                try {
                    val deleteResponse: BulkResponse = client.suspendUntil {
                        client.bulk(BulkRequest().add(deleteRequests).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), it)
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
            actionListener.onResponse(
                AcknowledgeAlertResponse(
                    acknowledged.toList(),
                    failed.toList(),
                    missing.toList()
                )
            )
        }
    }
}
