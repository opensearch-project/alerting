/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.opensearchapi.firstFailureOrNull
import org.opensearch.alerting.opensearchapi.retry
import org.opensearch.alerting.script.ChainedAlertTriggerExecutionContext
import org.opensearch.alerting.script.DocumentLevelTriggerExecutionContext
import org.opensearch.alerting.script.QueryLevelTriggerExecutionContext
import org.opensearch.alerting.util.CommentsUtils
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.MAX_SEARCH_SIZE
import org.opensearch.alerting.util.await
import org.opensearch.alerting.util.getBucketKeysHash
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.alerts.AlertError
import org.opensearch.commons.alerting.model.ActionExecutionResult
import org.opensearch.commons.alerting.model.ActionRunResult
import org.opensearch.commons.alerting.model.AggregationResultBucket
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.BucketLevelTrigger
import org.opensearch.commons.alerting.model.ChainedAlertTriggerRunResult
import org.opensearch.commons.alerting.model.ClusterMetricsTriggerRunResult
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.NoOpTrigger
import org.opensearch.commons.alerting.model.QueryLevelTriggerRunResult
import org.opensearch.commons.alerting.model.Trigger
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.model.WorkflowRunContext
import org.opensearch.commons.alerting.model.action.AlertCategory
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.DeleteByQueryAction
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder
import org.opensearch.remote.metadata.client.BulkDataObjectRequest
import org.opensearch.remote.metadata.client.DeleteDataObjectRequest
import org.opensearch.remote.metadata.client.PutDataObjectRequest
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.remote.metadata.client.SearchDataObjectRequest
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder
import org.opensearch.transport.client.Client
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
/** Service that handles CRUD operations for alerts */
class AlertService(
    val client: Client,
    val xContentRegistry: NamedXContentRegistry,
    val alertIndices: AlertIndices,
    val sdkClient: SdkClient
) {

    companion object {
        const val MAX_BUCKET_LEVEL_MONITOR_ALERT_SEARCH_COUNT = 500
        const val ERROR_ALERT_ID_PREFIX = "error-alert"

        val ALERTS_SEARCH_TIMEOUT = TimeValue(5, TimeUnit.MINUTES)
    }

    private val logger = LogManager.getLogger(AlertService::class.java)

    suspend fun loadCurrentAlertsForWorkflow(workflow: Workflow, dataSources: DataSources): Map<Trigger, Alert?> {
        val searchAlertsResponse: SearchResponse = searchAlerts(
            workflow = workflow,
            size = workflow.triggers.size * 2, // We expect there to be only a single in-progress alert so fetch 2 to check
            dataSources = dataSources
        )

        val foundAlerts = searchAlertsResponse.hits.map { Alert.parse(contentParser(it.sourceRef), it.id, it.version) }
            .groupBy { it.triggerId }
        foundAlerts.values.forEach { alerts ->
            if (alerts.size > 1) {
                logger.warn("Found multiple alerts for same trigger: $alerts")
            }
        }

        return workflow.triggers.associateWith { trigger ->
            foundAlerts[trigger.id]?.firstOrNull()
        }
    }

    suspend fun loadCurrentAlertsForQueryLevelMonitor(monitor: Monitor, workflowRunContext: WorkflowRunContext?): Map<Trigger, Alert?> {
        val searchAlertsResponse: SearchResponse = searchAlerts(
            monitor = monitor,
            size = monitor.triggers.size * 2, // We expect there to be only a single in-progress alert so fetch 2 to check
            workflowRunContext
        )

        val foundAlerts = searchAlertsResponse.hits.map { Alert.parse(contentParser(it.sourceRef), it.id, it.version) }
            .groupBy { it.triggerId }
        foundAlerts.values.forEach { alerts ->
            if (alerts.size > 1) {
                logger.warn("Found multiple alerts for same trigger: $alerts")
            }
        }

        return monitor.triggers.associateWith { trigger ->
            foundAlerts[trigger.id]?.firstOrNull()
        }
    }

    suspend fun loadCurrentAlertsForBucketLevelMonitor(
        monitor: Monitor,
        workflowRunContext: WorkflowRunContext?,
    ): Map<Trigger, MutableMap<String, Alert>> {
        val searchAlertsResponse: SearchResponse = searchAlerts(
            monitor = monitor,
            // TODO: This should be limited based on a circuit breaker that limits Alerts
            size = MAX_BUCKET_LEVEL_MONITOR_ALERT_SEARCH_COUNT,
            workflowRunContext = workflowRunContext
        )

        val foundAlerts = searchAlertsResponse.hits.map { Alert.parse(contentParser(it.sourceRef), it.id, it.version) }
            .groupBy { it.triggerId }

        return monitor.triggers.associateWith { trigger ->
            // Default to an empty map if there are no Alerts found for a Trigger to make Alert categorization logic easier
            (
                foundAlerts[trigger.id]?.mapNotNull { alert ->
                    alert.aggregationResultBucket?.let { it.getBucketKeysHash() to alert }
                }?.toMap()?.toMutableMap() ?: mutableMapOf()
                )
        }
    }

    fun composeQueryLevelAlert(
        ctx: QueryLevelTriggerExecutionContext,
        result: QueryLevelTriggerRunResult,
        alertError: AlertError?,
        executionId: String,
        workflorwRunContext: WorkflowRunContext?
    ): Alert? {
        val currentTime = Instant.now()
        val currentAlert = ctx.alert?.alert

        val updatedActionExecutionResults = mutableListOf<ActionExecutionResult>()
        val currentActionIds = mutableSetOf<String>()
        if (currentAlert != null) {
            // update current alert's action execution results
            for (actionExecutionResult in currentAlert.actionExecutionResults) {
                val actionId = actionExecutionResult.actionId
                currentActionIds.add(actionId)
                val actionRunResult = result.actionResults[actionId]
                when {
                    actionRunResult == null -> updatedActionExecutionResults.add(actionExecutionResult)
                    actionRunResult.throttled ->
                        updatedActionExecutionResults.add(
                            actionExecutionResult.copy(
                                throttledCount = actionExecutionResult.throttledCount + 1
                            )
                        )
                    else -> updatedActionExecutionResults.add(actionExecutionResult.copy(lastExecutionTime = actionRunResult.executionTime))
                }
            }
            // add action execution results which not exist in current alert
            updatedActionExecutionResults.addAll(
                result.actionResults.filter { !currentActionIds.contains(it.key) }
                    .map { ActionExecutionResult(it.key, it.value.executionTime, if (it.value.throttled) 1 else 0) }
            )
        } else {
            updatedActionExecutionResults.addAll(
                result.actionResults.map {
                    ActionExecutionResult(it.key, it.value.executionTime, if (it.value.throttled) 1 else 0)
                }
            )
        }

        // Including a list of triggered clusters for cluster metrics monitors
        var triggeredClusters: MutableList<String>? = null
        if (result is ClusterMetricsTriggerRunResult)
            result.clusterTriggerResults.forEach {
                if (it.triggered) {
                    // Add an empty list if one isn't already present
                    if (triggeredClusters.isNullOrEmpty()) triggeredClusters = mutableListOf()

                    // Add the cluster to the list of triggered clusters
                    triggeredClusters!!.add(it.cluster)
                }
            }

        // Merge the alert's error message to the current alert's history
        val updatedHistory = currentAlert?.errorHistory.update(alertError)
        return if (alertError == null && !result.triggered) {
            currentAlert?.copy(
                state = Alert.State.COMPLETED,
                endTime = currentTime,
                errorMessage = null,
                errorHistory = updatedHistory,
                actionExecutionResults = updatedActionExecutionResults,
                schemaVersion = IndexUtils.alertIndexSchemaVersion,
                clusters = triggeredClusters
            )
        } else if (alertError == null && currentAlert?.isAcknowledged() == true) {
            null
        } else if (currentAlert != null) {
            val alertState = if (alertError == null) Alert.State.ACTIVE else Alert.State.ERROR
            currentAlert.copy(
                state = alertState,
                lastNotificationTime = currentTime,
                errorMessage = alertError?.message,
                errorHistory = updatedHistory,
                actionExecutionResults = updatedActionExecutionResults,
                schemaVersion = IndexUtils.alertIndexSchemaVersion,
                clusters = triggeredClusters
            )
        } else {
            val alertState = if (workflorwRunContext?.auditDelegateMonitorAlerts == true) {
                Alert.State.AUDIT
            } else if (alertError == null) Alert.State.ACTIVE
            else Alert.State.ERROR
            Alert(
                monitor = ctx.monitor, trigger = ctx.trigger, startTime = currentTime,
                lastNotificationTime = currentTime, state = alertState, errorMessage = alertError?.message,
                errorHistory = updatedHistory, actionExecutionResults = updatedActionExecutionResults,
                schemaVersion = IndexUtils.alertIndexSchemaVersion, executionId = executionId,
                workflowId = workflorwRunContext?.workflowId ?: "",
                clusters = triggeredClusters
            )
        }
    }

    // TODO: clean this up so it follows the proper alert management for doc monitors
    fun composeDocLevelAlert(
        findings: List<String>,
        relatedDocIds: List<String>,
        ctx: DocumentLevelTriggerExecutionContext,
        alertError: AlertError?,
        executionId: String,
        workflorwRunContext: WorkflowRunContext?
    ): Alert {
        val currentTime = Instant.now()

        val alertState = if (workflorwRunContext?.auditDelegateMonitorAlerts == true) {
            Alert.State.AUDIT
        } else if (alertError == null) {
            Alert.State.ACTIVE
        } else {
            Alert.State.ERROR
        }
        return Alert(
            id = UUID.randomUUID().toString(), monitor = ctx.monitor, trigger = ctx.trigger, startTime = currentTime,
            lastNotificationTime = currentTime, state = alertState, errorMessage = alertError?.message,
            schemaVersion = IndexUtils.alertIndexSchemaVersion, findingIds = findings, relatedDocIds = relatedDocIds,
            executionId = executionId, workflowId = workflorwRunContext?.workflowId ?: ""
        )
    }

    fun composeMonitorErrorAlert(
        id: String,
        monitor: Monitor,
        alertError: AlertError,
        executionId: String?,
        workflowRunContext: WorkflowRunContext?
    ): Alert {
        val currentTime = Instant.now()
        val alertState = if (workflowRunContext?.auditDelegateMonitorAlerts == true) {
            Alert.State.AUDIT
        } else {
            Alert.State.ERROR
        }
        return Alert(
            id = id, monitor = monitor, trigger = NoOpTrigger(), startTime = currentTime,
            lastNotificationTime = currentTime, state = alertState, errorMessage = alertError.message,
            schemaVersion = IndexUtils.alertIndexSchemaVersion, executionId = executionId, workflowId = workflowRunContext?.workflowId ?: ""
        )
    }

    fun composeChainedAlert(
        ctx: ChainedAlertTriggerExecutionContext,
        executionId: String,
        workflow: Workflow,
        associatedAlertIds: List<String>,
        result: ChainedAlertTriggerRunResult,
        alertError: AlertError? = null,
    ): Alert? {

        val currentTime = Instant.now()
        val currentAlert = ctx.alert

        val updatedActionExecutionResults = mutableListOf<ActionExecutionResult>()
        val currentActionIds = mutableSetOf<String>()
        if (currentAlert != null) {
            // update current alert's action execution results
            for (actionExecutionResult in currentAlert.actionExecutionResults) {
                val actionId = actionExecutionResult.actionId
                currentActionIds.add(actionId)
                val actionRunResult = result.actionResults[actionId]
                when {
                    actionRunResult == null -> updatedActionExecutionResults.add(actionExecutionResult)
                    actionRunResult.throttled ->
                        updatedActionExecutionResults.add(
                            actionExecutionResult.copy(
                                throttledCount = actionExecutionResult.throttledCount + 1
                            )
                        )

                    else -> updatedActionExecutionResults.add(actionExecutionResult.copy(lastExecutionTime = actionRunResult.executionTime))
                }
            }
            // add action execution results which not exist in current alert
            updatedActionExecutionResults.addAll(
                result.actionResults.filter { !currentActionIds.contains(it.key) }
                    .map { ActionExecutionResult(it.key, it.value.executionTime, if (it.value.throttled) 1 else 0) }
            )
        } else {
            updatedActionExecutionResults.addAll(
                result.actionResults.map {
                    ActionExecutionResult(it.key, it.value.executionTime, if (it.value.throttled) 1 else 0)
                }
            )
        }

        // Merge the alert's error message to the current alert's history
        val updatedHistory = currentAlert?.errorHistory.update(alertError)
        return if (alertError == null && !result.triggered) {
            currentAlert?.copy(
                state = Alert.State.COMPLETED,
                endTime = currentTime,
                errorMessage = null,
                errorHistory = updatedHistory,
                actionExecutionResults = updatedActionExecutionResults,
                schemaVersion = IndexUtils.alertIndexSchemaVersion
            )
        } else if (alertError == null && currentAlert?.isAcknowledged() == true) {
            null
        } else if (currentAlert != null) {
            val alertState = Alert.State.ACTIVE
            currentAlert.copy(
                state = alertState,
                lastNotificationTime = currentTime,
                errorMessage = alertError?.message,
                errorHistory = updatedHistory,
                actionExecutionResults = updatedActionExecutionResults,
                schemaVersion = IndexUtils.alertIndexSchemaVersion,
            )
        } else {
            if (alertError == null) Alert.State.ACTIVE
            else Alert.State.ERROR
            Alert(
                startTime = Instant.now(),
                lastNotificationTime = currentTime,
                state = Alert.State.ACTIVE,
                errorMessage = null, schemaVersion = IndexUtils.alertIndexSchemaVersion,
                chainedAlertTrigger = ctx.trigger,
                executionId = executionId,
                workflow = workflow,
                associatedAlertIds = associatedAlertIds
            )
        }
    }

    fun updateActionResultsForBucketLevelAlert(
        currentAlert: Alert,
        actionResults: Map<String, ActionRunResult>,
        alertError: AlertError?
    ): Alert {
        val updatedActionExecutionResults = mutableListOf<ActionExecutionResult>()
        val currentActionIds = mutableSetOf<String>()
        // Update alert's existing action execution results
        for (actionExecutionResult in currentAlert.actionExecutionResults) {
            val actionId = actionExecutionResult.actionId
            currentActionIds.add(actionId)
            val actionRunResult = actionResults[actionId]
            when {
                actionRunResult == null -> updatedActionExecutionResults.add(actionExecutionResult)
                actionRunResult.throttled ->
                    updatedActionExecutionResults.add(
                        actionExecutionResult.copy(
                            throttledCount = actionExecutionResult.throttledCount + 1
                        )
                    )
                else -> updatedActionExecutionResults.add(actionExecutionResult.copy(lastExecutionTime = actionRunResult.executionTime))
            }
        }

        // Add action execution results not currently present in the alert
        updatedActionExecutionResults.addAll(
            actionResults.filter { !currentActionIds.contains(it.key) }
                .map { ActionExecutionResult(it.key, it.value.executionTime, if (it.value.throttled) 1 else 0) }
        )

        val updatedErrorHistory = currentAlert.errorHistory.update(alertError)
        return if (alertError == null) {
            currentAlert.copy(errorHistory = updatedErrorHistory, actionExecutionResults = updatedActionExecutionResults)
        } else {
            currentAlert.copy(
                state = Alert.State.ERROR,
                errorMessage = alertError.message,
                errorHistory = updatedErrorHistory,
                actionExecutionResults = updatedActionExecutionResults
            )
        }
    }

    // TODO: Can change the parameters to use ctx: BucketLevelTriggerExecutionContext instead of monitor/trigger and
    //  result: AggTriggerRunResult for aggResultBuckets
    // TODO: Can refactor this method to use Sets instead which can cleanup some of the categorization logic (like getting completed alerts)
    fun getCategorizedAlertsForBucketLevelMonitor(
        monitor: Monitor,
        trigger: BucketLevelTrigger,
        currentAlerts: MutableMap<String, Alert>,
        aggResultBuckets: List<AggregationResultBucket>,
        findings: List<String>,
        executionId: String,
        workflorwRunContext: WorkflowRunContext?
    ): Map<AlertCategory, List<Alert>> {
        val dedupedAlerts = mutableListOf<Alert>()
        val newAlerts = mutableListOf<Alert>()
        val currentTime = Instant.now()

        aggResultBuckets.forEach { aggAlertBucket ->
            val currentAlert = currentAlerts[aggAlertBucket.getBucketKeysHash()]
            if (currentAlert != null) {
                // De-duped Alert
                dedupedAlerts.add(currentAlert.copy(aggregationResultBucket = aggAlertBucket))

                // Remove de-duped Alert from currentAlerts since it is no longer a candidate for a potentially completed Alert
                currentAlerts.remove(aggAlertBucket.getBucketKeysHash())
            } else {
                // New Alert
                val alertState = if (workflorwRunContext?.auditDelegateMonitorAlerts == true) {
                    Alert.State.AUDIT
                } else Alert.State.ACTIVE
                val newAlert = Alert(
                    monitor = monitor, trigger = trigger, startTime = currentTime,
                    lastNotificationTime = currentTime, state = alertState, errorMessage = null,
                    errorHistory = mutableListOf(), actionExecutionResults = mutableListOf(),
                    schemaVersion = IndexUtils.alertIndexSchemaVersion, aggregationResultBucket = aggAlertBucket,
                    findingIds = findings, executionId = executionId, workflowId = workflorwRunContext?.workflowId ?: ""
                )
                newAlerts.add(newAlert)
            }
        }

        return mapOf(
            AlertCategory.DEDUPED to dedupedAlerts,
            AlertCategory.NEW to newAlerts
        )
    }

    fun convertToCompletedAlerts(currentAlerts: Map<String, Alert>?): List<Alert> {
        val currentTime = Instant.now()
        return currentAlerts?.map {
            it.value.copy(
                state = Alert.State.COMPLETED, endTime = currentTime, errorMessage = null,
                schemaVersion = IndexUtils.alertIndexSchemaVersion
            )
        } ?: listOf()
    }

    suspend fun upsertMonitorErrorAlert(
        monitor: Monitor,
        errorMessage: String,
        executionId: String?,
        workflowRunContext: WorkflowRunContext?,
    ) {
        val newErrorAlertId = "$ERROR_ALERT_ID_PREFIX-${monitor.id}-${UUID.randomUUID()}"

        val searchRequest = SearchDataObjectRequest.builder()
            .indices(monitor.dataSources.alertsIndex)
            .routing(monitor.id)
            .searchSourceBuilder(
                SearchSourceBuilder()
                    .sort(Alert.START_TIME_FIELD, SortOrder.DESC)
                    .query(
                        QueryBuilders.boolQuery()
                            .must(QueryBuilders.termQuery(Alert.MONITOR_ID_FIELD, monitor.id))
                            .must(QueryBuilders.termQuery(Alert.STATE_FIELD, Alert.State.ERROR.name))
                    )
            )
            .build()
        val searchResponse: SearchResponse = sdkClient.searchDataObjectAsync(searchRequest).await()
            .searchResponse() ?: throw RuntimeException("Unknown error searching for error alerts")

        var alert =
            composeMonitorErrorAlert(newErrorAlertId, monitor, AlertError(Instant.now(), errorMessage), executionId, workflowRunContext)

        if (searchResponse.hits.totalHits.value > 0L) {
            if (searchResponse.hits.totalHits.value > 1L) {
                logger.warn("There are [${searchResponse.hits.totalHits.value}] error alerts for monitor [${monitor.id}]")
            }
            // Deserialize first/latest Alert
            val hit = searchResponse.hits.hits[0]
            val xcp = contentParser(hit.sourceRef)
            val existingErrorAlert = Alert.parse(xcp, hit.id, hit.version)

            val currentTime = Instant.now()
            alert = if (alert.errorMessage != existingErrorAlert.errorMessage) {
                var newErrorHistory = existingErrorAlert.errorHistory.update(
                    AlertError(existingErrorAlert.startTime, existingErrorAlert.errorMessage!!)
                )
                alert.copy(
                    id = existingErrorAlert.id,
                    errorHistory = newErrorHistory,
                    startTime = currentTime,
                    lastNotificationTime = currentTime
                )
            } else {
                existingErrorAlert.copy(lastNotificationTime = currentTime)
            }
        }

        val putRequest = PutDataObjectRequest.builder()
            .index(monitor.dataSources.alertsIndex)
            .id(alert.id)
            .routing(alert.monitorId)
            .overwriteIfExists(true)
            .dataObject(ToXContentObject { builder, _ -> alert.toXContentWithUser(builder) })
            .build()
        val putResponse = sdkClient.putDataObjectAsync(putRequest).await()
        if (putResponse.isFailed) {
            throw ExceptionsHelper.convertToOpenSearchException(
                putResponse.cause() ?: RuntimeException("Failed to upsert monitor error alert")
            )
        }
        logger.debug("Monitor error Alert successfully upserted. Op result: ${putResponse.indexResponse()?.result}")
    }

    suspend fun clearMonitorErrorAlert(monitor: Monitor) {
        val currentTime = Instant.now()
        try {
            val searchRequest = SearchDataObjectRequest.builder()
                .indices(monitor.dataSources.alertsIndex)
                .routing(monitor.id)
                .searchSourceBuilder(
                    SearchSourceBuilder()
                        .size(MAX_SEARCH_SIZE)
                        .sort(Alert.START_TIME_FIELD, SortOrder.DESC)
                        .query(
                            QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery(Alert.MONITOR_ID_FIELD, monitor.id))
                                .must(QueryBuilders.termQuery(Alert.STATE_FIELD, Alert.State.ERROR.name))
                        )
                )
                .build()
            val searchResponse: SearchResponse = sdkClient.searchDataObjectAsync(searchRequest).await()
                .searchResponse() ?: throw RuntimeException("Unknown error searching for error alerts")
            // If there's no error alert present, there's nothing to clear. We can stop here.
            if (searchResponse.hits.totalHits.value == 0L) {
                return
            }

            val bulkRequest = BulkDataObjectRequest(monitor.dataSources.alertsIndex)
            searchResponse.hits.hits.forEach { hit ->
                if (searchResponse.hits.totalHits.value > 1L) {
                    logger.warn("Found [${searchResponse.hits.totalHits.value}] error alerts for monitor [${monitor.id}] while clearing")
                }
                val xcp = contentParser(hit.sourceRef)
                val existingErrorAlert = Alert.parse(xcp, hit.id, hit.version)
                val updatedAlert = existingErrorAlert.copy(endTime = currentTime)

                bulkRequest.add(
                    PutDataObjectRequest.builder()
                        .index(monitor.dataSources.alertsIndex)
                        .id(updatedAlert.id)
                        .routing(monitor.id)
                        .overwriteIfExists(true)
                        .dataObject(ToXContentObject { builder, _ -> updatedAlert.toXContentWithUser(builder) })
                        .build()
                )
            }

            val bulkResponse = sdkClient.bulkDataObjectAsync(bulkRequest).await()
            if (bulkResponse.hasFailures()) {
                bulkResponse.responses.filter { it.isFailed }.forEach { item ->
                    logger.debug("Failed clearing error alert ${item.id()} of monitor [${monitor.id}]")
                }
            } else {
                logger.debug("[${searchResponse.hits.totalHits.value}] Error Alerts successfully cleared. End time set to: $currentTime")
            }
        } catch (e: Exception) {
            logger.error("Error clearing monitor error alerts for monitor [${monitor.id}]: ${ExceptionsHelper.detailedMessage(e)}")
        }
    }

    /**
     * Moves already cleared "error alerts" to history index.
     * Error Alert is cleared when endTime timestamp is set, on first successful run after failed run
     * */
    suspend fun moveClearedErrorAlertsToHistory(monitorId: String, alertIndex: String, alertHistoryIndex: String) {
        try {
            val searchRequest = SearchDataObjectRequest.builder()
                .indices(alertIndex)
                .routing(monitorId)
                .searchSourceBuilder(
                    SearchSourceBuilder()
                        .size(MAX_SEARCH_SIZE)
                        .query(
                            QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery(Alert.MONITOR_ID_FIELD, monitorId))
                                .must(QueryBuilders.termQuery(Alert.STATE_FIELD, Alert.State.ERROR.name))
                                .must(QueryBuilders.existsQuery(Alert.END_TIME_FIELD))
                        )
                        .version(true)
                )
                .build()
            val searchResponse: SearchResponse = sdkClient.searchDataObjectAsync(searchRequest).await()
                .searchResponse() ?: throw RuntimeException("Unknown error searching for cleared error alerts")

            if (searchResponse.hits.totalHits.value == 0L) {
                return
            }

            // Copy to history index
            val bulkRequest = BulkDataObjectRequest(null)
            searchResponse.hits.hits.forEach { hit ->
                val xcp = contentParser(hit.sourceRef)
                val alert = Alert.parse(xcp, hit.id, hit.version)

                bulkRequest.add(
                    PutDataObjectRequest.builder()
                        .index(alertHistoryIndex)
                        .id(hit.id)
                        .routing(alert.monitorId)
                        .overwriteIfExists(true)
                        .dataObject(ToXContentObject { builder, _ -> alert.toXContentWithUser(builder) })
                        .build()
                )
            }

            val bulkResponse = sdkClient.bulkDataObjectAsync(bulkRequest).await()
            if (bulkResponse.hasFailures()) {
                bulkResponse.responses.filter { it.isFailed }.forEach { item ->
                    logger.error("Failed copying error alert [${item.id()}] to history index [$alertHistoryIndex]")
                }
                return
            }

            // Delete from alertIndex

            val alertIds = searchResponse.hits.hits.map { it.id }

            val deleteResponse: BulkByScrollResponse = suspendCoroutine { cont ->
                DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                    .source(alertIndex)
                    .filter(QueryBuilders.termsQuery("_id", alertIds))
                    .refresh(true)
                    .timeout(ALERTS_SEARCH_TIMEOUT)
                    .execute(
                        object : ActionListener<BulkByScrollResponse> {
                            override fun onResponse(response: BulkByScrollResponse) = cont.resume(response)
                            override fun onFailure(t: Exception) = cont.resumeWithException(t)
                        }
                    )
            }
            deleteResponse.bulkFailures.forEach {
                logger.error("Failed deleting alert while moving cleared alerts: [${it.id}] cause: [${it.cause}] ")
            }
        } catch (e: Exception) {
            logger.error("Failed moving cleared error alerts to history index: ${ExceptionsHelper.detailedMessage(e)}")
        }
    }

    suspend fun saveAlerts(
        dataSources: DataSources,
        alerts: List<Alert>,
        retryPolicy: BackoffPolicy,
        allowUpdatingAcknowledgedAlert: Boolean = false,
        routingId: String // routing is mandatory and set as monitor id. for workflow chained alerts we pass workflow id as routing
    ) {
        val alertsIndex = dataSources.alertsIndex
        val alertsHistoryIndex = dataSources.alertsHistoryIndex

        val commentIdsToDelete = mutableListOf<String>()

        val putRequests = mutableListOf<PutDataObjectRequest>()
        val deleteRequests = mutableListOf<DeleteDataObjectRequest>()

        alerts.forEach { alert ->
            when (alert.state) {
                Alert.State.ACTIVE, Alert.State.ERROR -> {
                    putRequests.add(
                        PutDataObjectRequest.builder()
                            .index(alertsIndex)
                            .id(if (alert.id != Alert.NO_ID) alert.id else null)
                            .routing(routingId)
                            .overwriteIfExists(true)
                            .dataObject(ToXContentObject { builder, _ -> alert.toXContentWithUser(builder) })
                            .build()
                    )
                }
                Alert.State.ACKNOWLEDGED -> {
                    if (allowUpdatingAcknowledgedAlert) {
                        putRequests.add(
                            PutDataObjectRequest.builder()
                                .index(alertsIndex)
                                .id(if (alert.id != Alert.NO_ID) alert.id else null)
                                .routing(routingId)
                                .overwriteIfExists(true)
                                .dataObject(ToXContentObject { builder, _ -> alert.toXContentWithUser(builder) })
                                .build()
                        )
                    } else {
                        throw IllegalStateException("Unexpected attempt to save ${alert.state} alert: $alert")
                    }
                }
                Alert.State.AUDIT -> {
                    val index = if (alertIndices.isAlertHistoryEnabled()) {
                        dataSources.alertsHistoryIndex
                    } else dataSources.alertsIndex
                    putRequests.add(
                        PutDataObjectRequest.builder()
                            .index(index)
                            .id(if (alert.id != Alert.NO_ID) alert.id else null)
                            .routing(routingId)
                            .overwriteIfExists(true)
                            .dataObject(ToXContentObject { builder, _ -> alert.toXContentWithUser(builder) })
                            .build()
                    )
                }
                Alert.State.DELETED -> {
                    throw IllegalStateException("Unexpected attempt to save ${alert.state} alert: $alert")
                }
                Alert.State.COMPLETED -> {
                    deleteRequests.add(
                        DeleteDataObjectRequest.builder()
                            .index(alertsIndex)
                            .id(alert.id)
                            .routing(routingId)
                            .build()
                    )
                    if (alertIndices.isAlertHistoryEnabled()) {
                        putRequests.add(
                            PutDataObjectRequest.builder()
                                .index(alertsHistoryIndex)
                                .id(alert.id)
                                .routing(routingId)
                                .overwriteIfExists(true)
                                .dataObject(ToXContentObject { builder, _ -> alert.toXContentWithUser(builder) })
                                .build()
                        )
                    } else {
                        commentIdsToDelete.addAll(CommentsUtils.getCommentIDsByAlertIDs(client, listOf(alert.id)))
                    }
                }
            }
        }

        if (putRequests.isEmpty() && deleteRequests.isEmpty()) return
        // Retry Bulk requests if there was any 429 response
        retryPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
            val bulkRequest = BulkDataObjectRequest(null)
            putRequests.forEach { bulkRequest.add(it) }
            deleteRequests.forEach { bulkRequest.add(it) }
            val bulkResponse = sdkClient.bulkDataObjectAsync(bulkRequest).await()
            val failedResponses = bulkResponse.responses.filter { it.isFailed }
            val retryableFailures = failedResponses.filter { it.status() == RestStatus.TOO_MANY_REQUESTS }

            if (retryableFailures.isNotEmpty()) {
                val retryCause = retryableFailures.first().cause()
                throw ExceptionsHelper.convertToOpenSearchException(retryCause)
            }
        }

        // delete all the comments of any Alerts that were deleted
        CommentsUtils.deleteComments(client, commentIdsToDelete)
    }

    /**
     * This is a separate method created specifically for saving new Alerts during the Bucket-Level Monitor run.
     * Alerts are saved in two batches during the execution of an Bucket-Level Monitor, once before the Actions are executed
     * and once afterwards. This method saves Alerts to the monitor's alertIndex but returns the same Alerts with their document IDs.
     *
     * The Alerts are required with their indexed ID so that when the new Alerts are updated after the Action execution,
     * the ID is available for the index request so that the existing Alert can be updated, instead of creating a duplicate Alert document.
     */
    suspend fun saveNewAlerts(dataSources: DataSources, alerts: List<Alert>, retryPolicy: BackoffPolicy): List<Alert> {
        val savedAlerts = mutableListOf<Alert>()
        var alertsBeingIndexed = alerts
        var requestsToRetry: MutableList<PutDataObjectRequest> = alerts.map { alert ->
            if (alert.state != Alert.State.ACTIVE && alert.state != Alert.State.AUDIT) {
                throw IllegalStateException("Unexpected attempt to save new alert [$alert] with state [${alert.state}]")
            }
            if (alert.id != Alert.NO_ID) {
                throw IllegalStateException("Unexpected attempt to save new alert [$alert] with an existing alert ID [${alert.id}]")
            }
            val alertIndex = if (alert.state == Alert.State.AUDIT && alertIndices.isAlertHistoryEnabled()) {
                dataSources.alertsHistoryIndex
            } else dataSources.alertsIndex
            PutDataObjectRequest.builder()
                .index(alertIndex)
                .routing(alert.monitorId)
                .overwriteIfExists(false)
                .dataObject(ToXContentObject { builder, _ -> alert.toXContentWithUser(builder) })
                .build()
        }.toMutableList()

        if (requestsToRetry.isEmpty()) return listOf()

        retryPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
            val bulkRequest = BulkDataObjectRequest(null)
            requestsToRetry.forEach { bulkRequest.add(it) }
            val bulkResponse = sdkClient.bulkDataObjectAsync(bulkRequest).await()
            val responses = bulkResponse.responses

            requestsToRetry = mutableListOf()
            val alertsBeingRetried = mutableListOf<Alert>()
            responses.forEachIndexed { index, item ->
                if (item.isFailed) {
                    if (item.status() == RestStatus.TOO_MANY_REQUESTS) {
                        requestsToRetry.add(
                            PutDataObjectRequest.builder()
                                .index(requestsToRetry.getOrNull(index)?.index() ?: dataSources.alertsIndex)
                                .routing(alertsBeingIndexed[index].monitorId)
                                .overwriteIfExists(false)
                                .dataObject(
                                    ToXContentObject { builder, _ -> alertsBeingIndexed[index].toXContentWithUser(builder) }
                                )
                                .build()
                        )
                        alertsBeingRetried.add(alertsBeingIndexed[index])
                    }
                } else {
                    savedAlerts.add(alertsBeingIndexed[index].copy(id = item.id()))
                }
            }

            alertsBeingIndexed = alertsBeingRetried

            if (requestsToRetry.isNotEmpty()) {
                val retryCause = responses.first { it.isFailed && it.status() == RestStatus.TOO_MANY_REQUESTS }.cause()
                throw ExceptionsHelper.convertToOpenSearchException(retryCause)
            }
        }

        return savedAlerts
    }

    private fun contentParser(bytesReference: BytesReference): XContentParser {
        val xcp = XContentHelper.createParser(
            xContentRegistry, LoggingDeprecationHandler.INSTANCE,
            bytesReference, XContentType.JSON
        )
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
        return xcp
    }

    /**
     * Searches for Alerts in the monitor's alertIndex.
     *
     * @param monitorId The Monitor to get Alerts for
     * @param size The number of search hits (Alerts) to return
     */
    private suspend fun searchAlerts(monitor: Monitor, size: Int, workflowRunContext: WorkflowRunContext?): SearchResponse {
        val monitorId = monitor.id
        val alertIndex = monitor.dataSources.alertsIndex

        val queryBuilder = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery(Alert.MONITOR_ID_FIELD, monitorId))
        if (workflowRunContext != null) {
            queryBuilder.must(QueryBuilders.termQuery(Alert.WORKFLOW_ID_FIELD, workflowRunContext.workflowId))
        }
        val searchSourceBuilder = SearchSourceBuilder()
            .size(size)
            .query(queryBuilder)

        val searchRequest = SearchDataObjectRequest.builder()
            .indices(alertIndex)
            .routing(monitorId)
            .searchSourceBuilder(searchSourceBuilder)
            .build()
        val searchResponse: SearchResponse = sdkClient.searchDataObjectAsync(searchRequest).await()
            .searchResponse() ?: throw RuntimeException("Unknown error loading alerts")
        if (searchResponse.status() != RestStatus.OK) {
            throw (searchResponse.firstFailureOrNull()?.cause ?: RuntimeException("Unknown error loading alerts"))
        }

        return searchResponse
    }

    /**
     * Searches for ACTIVE/ACKNOWLEDGED chained alerts in the workflow's alertIndex.
     *
     * @param monitorId The Monitor to get Alerts for
     * @param size The number of search hits (Alerts) to return
     */
    private suspend fun searchAlerts(
        workflow: Workflow,
        size: Int,
        dataSources: DataSources,
    ): SearchResponse {
        val workflowId = workflow.id
        val alertIndex = dataSources.alertsIndex

        val queryBuilder = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery(Alert.WORKFLOW_ID_FIELD, workflowId))
            .must(QueryBuilders.termQuery(Alert.MONITOR_ID_FIELD, ""))
        val searchSourceBuilder = SearchSourceBuilder()
            .size(size)
            .query(queryBuilder)

        val searchRequest = SearchDataObjectRequest.builder()
            .indices(alertIndex)
            .routing(workflowId)
            .searchSourceBuilder(searchSourceBuilder)
            .build()
        val searchResponse: SearchResponse = sdkClient.searchDataObjectAsync(searchRequest).await()
            .searchResponse() ?: throw RuntimeException("Unknown error loading alerts")
        if (searchResponse.status() != RestStatus.OK) {
            throw (searchResponse.firstFailureOrNull()?.cause ?: RuntimeException("Unknown error loading alerts"))
        }
        return searchResponse
    }

    private fun List<AlertError>?.update(alertError: AlertError?): List<AlertError> {
        return when {
            this == null && alertError == null -> emptyList()
            this != null && alertError == null -> this
            this == null && alertError != null -> listOf(alertError)
            this != null && alertError != null -> (listOf(alertError) + this).take(10)
            else -> throw IllegalStateException("Unreachable code reached!")
        }
    }
}
