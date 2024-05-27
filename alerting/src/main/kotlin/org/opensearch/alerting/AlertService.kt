/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.model.ActionRunResult
import org.opensearch.alerting.model.ChainedAlertTriggerRunResult
import org.opensearch.alerting.model.ClusterMetricsTriggerRunResult
import org.opensearch.alerting.model.QueryLevelTriggerRunResult
import org.opensearch.alerting.opensearchapi.firstFailureOrNull
import org.opensearch.alerting.opensearchapi.retry
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.script.ChainedAlertTriggerExecutionContext
import org.opensearch.alerting.script.DocumentLevelTriggerExecutionContext
import org.opensearch.alerting.script.QueryLevelTriggerExecutionContext
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.MAX_SEARCH_SIZE
import org.opensearch.alerting.util.NotesUtils
import org.opensearch.alerting.util.getBucketKeysHash
import org.opensearch.alerting.workflow.WorkflowRunContext
import org.opensearch.client.Client
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.alerts.AlertError
import org.opensearch.commons.alerting.model.ActionExecutionResult
import org.opensearch.commons.alerting.model.AggregationResultBucket
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.BucketLevelTrigger
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.NoOpTrigger
import org.opensearch.commons.alerting.model.Note
import org.opensearch.commons.alerting.model.Trigger
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.model.action.AlertCategory
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.VersionType
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.DeleteByQueryAction
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder
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
    val alertIndices: AlertIndices
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
        val currentAlert = ctx.alertContext?.alert

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

    /**
     * Performs a Search request to retrieve the Notes associated with the
     * Alert with the given ID.
     *
     * Searches for the n most recently created Notes based on maxNotes
     */
    suspend fun getNotesForAlertNotification(alertId: String, maxNotes: Int): List<Note> {
        val allNotes = NotesUtils.getNotesByAlertIDs(client, listOf(alertId))
        val sortedNotes = allNotes.sortedByDescending { it.time }
        if (sortedNotes.size <= maxNotes) {
            return sortedNotes
        }
        return sortedNotes.slice(0 until maxNotes)
    }

    suspend fun upsertMonitorErrorAlert(
        monitor: Monitor,
        errorMessage: String,
        executionId: String?,
        workflowRunContext: WorkflowRunContext?,
    ) {
        val newErrorAlertId = "$ERROR_ALERT_ID_PREFIX-${monitor.id}-${UUID.randomUUID()}"

        val searchRequest = SearchRequest(monitor.dataSources.alertsIndex)
            .source(
                SearchSourceBuilder()
                    .sort(Alert.START_TIME_FIELD, SortOrder.DESC)
                    .query(
                        QueryBuilders.boolQuery()
                            .must(QueryBuilders.termQuery(Alert.MONITOR_ID_FIELD, monitor.id))
                            .must(QueryBuilders.termQuery(Alert.STATE_FIELD, Alert.State.ERROR.name))
                    )
            )
        val searchResponse: SearchResponse = client.suspendUntil { search(searchRequest, it) }

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

        val alertIndexRequest = IndexRequest(monitor.dataSources.alertsIndex)
            .routing(alert.monitorId)
            .source(alert.toXContentWithUser(XContentFactory.jsonBuilder()))
            .opType(DocWriteRequest.OpType.INDEX)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .id(alert.id)

        val indexResponse: IndexResponse = client.suspendUntil { index(alertIndexRequest, it) }
        logger.debug("Monitor error Alert successfully upserted. Op result: ${indexResponse.result}")
    }

    suspend fun clearMonitorErrorAlert(monitor: Monitor) {
        val currentTime = Instant.now()
        try {
            val searchRequest = SearchRequest("${monitor.dataSources.alertsIndex}")
                .source(
                    SearchSourceBuilder()
                        .size(MAX_SEARCH_SIZE)
                        .sort(Alert.START_TIME_FIELD, SortOrder.DESC)
                        .query(
                            QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery(Alert.MONITOR_ID_FIELD, monitor.id))
                                .must(QueryBuilders.termQuery(Alert.STATE_FIELD, Alert.State.ERROR.name))
                        )

                )
            searchRequest.cancelAfterTimeInterval = ALERTS_SEARCH_TIMEOUT
            val searchResponse: SearchResponse = client.suspendUntil { search(searchRequest, it) }
            // If there's no error alert present, there's nothing to clear. We can stop here.
            if (searchResponse.hits.totalHits.value == 0L) {
                return
            }

            val indexRequests = mutableListOf<IndexRequest>()
            searchResponse.hits.hits.forEach { hit ->
                if (searchResponse.hits.totalHits.value > 1L) {
                    logger.warn("Found [${searchResponse.hits.totalHits.value}] error alerts for monitor [${monitor.id}] while clearing")
                }
                // Deserialize first/latest Alert
                val xcp = contentParser(hit.sourceRef)
                val existingErrorAlert = Alert.parse(xcp, hit.id, hit.version)

                val updatedAlert = existingErrorAlert.copy(
                    endTime = currentTime
                )

                indexRequests += IndexRequest(monitor.dataSources.alertsIndex)
                    .routing(monitor.id)
                    .id(updatedAlert.id)
                    .source(updatedAlert.toXContentWithUser(XContentFactory.jsonBuilder()))
                    .opType(DocWriteRequest.OpType.INDEX)
            }

            val bulkResponse: BulkResponse = client.suspendUntil {
                bulk(BulkRequest().add(indexRequests).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), it)
            }
            if (bulkResponse.hasFailures()) {
                bulkResponse.items.forEach { item ->
                    if (item.isFailed) {
                        logger.debug("Failed clearing error alert ${item.id} of monitor [${monitor.id}]")
                    }
                }
            } else {
                logger.debug("[${bulkResponse.items.size}] Error Alerts successfully cleared. End time set to: $currentTime")
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
            val searchRequest = SearchRequest(alertIndex)
                .source(
                    SearchSourceBuilder()
                        .size(MAX_SEARCH_SIZE)
                        .query(
                            QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery(Alert.MONITOR_ID_FIELD, monitorId))
                                .must(QueryBuilders.termQuery(Alert.STATE_FIELD, Alert.State.ERROR.name))
                                .must(QueryBuilders.existsQuery(Alert.END_TIME_FIELD))
                        )
                        .version(true) // Do we need this?
                )
            searchRequest.cancelAfterTimeInterval = ALERTS_SEARCH_TIMEOUT
            val searchResponse: SearchResponse = client.suspendUntil { search(searchRequest, it) }

            if (searchResponse.hits.totalHits.value == 0L) {
                return
            }

            // Copy to history index

            val copyRequests = mutableListOf<IndexRequest>()

            searchResponse.hits.hits.forEach { hit ->

                val xcp = contentParser(hit.sourceRef)
                val alert = Alert.parse(xcp, hit.id, hit.version)

                copyRequests.add(
                    IndexRequest(alertHistoryIndex)
                        .routing(alert.monitorId)
                        .source(hit.sourceRef, XContentType.JSON)
                        .version(hit.version)
                        .versionType(VersionType.EXTERNAL_GTE)
                        .id(hit.id)
                        .timeout(MonitorRunnerService.monitorCtx.indexTimeout)
                )
            }

            val bulkResponse: BulkResponse = client.suspendUntil {
                bulk(BulkRequest().add(copyRequests).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), it)
            }
            if (bulkResponse.hasFailures()) {
                bulkResponse.items.forEach { item ->
                    if (item.isFailed) {
                        logger.error("Failed copying error alert [${item.id}] to history index [$alertHistoryIndex]")
                    }
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

        val notesToDeleteIDs = mutableListOf<String>()

        var requestsToRetry = alerts.flatMap { alert ->
            // We don't want to set the version when saving alerts because the MonitorRunner has first priority when writing alerts.
            // In the rare event that a user acknowledges an alert between when it's read and when it's written
            // back we're ok if that acknowledgement is lost. It's easier to get the user to retry than for the runner to
            // spend time reloading the alert and writing it back.
            when (alert.state) {
                Alert.State.ACTIVE, Alert.State.ERROR -> {
                    listOf<DocWriteRequest<*>>(
                        IndexRequest(alertsIndex)
                            .routing(routingId)
                            .source(alert.toXContentWithUser(XContentFactory.jsonBuilder()))
                            .id(if (alert.id != Alert.NO_ID) alert.id else null)
                    )
                }
                Alert.State.ACKNOWLEDGED -> {
                    // Allow ACKNOWLEDGED Alerts to be updated for Bucket-Level Monitors since de-duped Alerts can be ACKNOWLEDGED
                    // and updated by the MonitorRunner
                    if (allowUpdatingAcknowledgedAlert) {
                        listOf<DocWriteRequest<*>>(
                            IndexRequest(alertsIndex)
                                .routing(routingId)
                                .source(alert.toXContentWithUser(XContentFactory.jsonBuilder()))
                                .id(if (alert.id != Alert.NO_ID) alert.id else null)
                        )
                    } else {
                        throw IllegalStateException("Unexpected attempt to save ${alert.state} alert: $alert")
                    }
                }
                Alert.State.AUDIT -> {
                    val index = if (alertIndices.isAlertHistoryEnabled()) {
                        dataSources.alertsHistoryIndex
                    } else dataSources.alertsIndex
                    listOf<DocWriteRequest<*>>(
                        IndexRequest(index)
                            .routing(routingId)
                            .source(alert.toXContentWithUser(XContentFactory.jsonBuilder()))
                            .id(if (alert.id != Alert.NO_ID) alert.id else null)
                    )
                }
                Alert.State.DELETED -> {
                    throw IllegalStateException("Unexpected attempt to save ${alert.state} alert: $alert")
                }
                Alert.State.COMPLETED -> {
//                    val requestsList = mutableListOf<DocWriteRequest<*>>(
//                        DeleteRequest(alertsIndex, alert.id)
//                            .routing(routingId)
//                    )
//                    // Only add completed alert to respective history indices if history is enabled
//                    if (alertIndices.isAlertHistoryEnabled()) {
//                        requestsList.add(
//                            IndexRequest(alertsHistoryIndex)
//                                .routing(routingId)
//                                .source(alert.toXContentWithUser(XContentFactory.jsonBuilder()))
//                                .id(alert.id)
//                        )
//                    } else {
//                        // Prepare Alert's Notes for deletion as well
//                        val notes = NotesUtils.searchNotesByAlertID(client, listOf(alert.id))
//                        notes.forEach { notesToDeleteIDs.add(it.id) }
//                    }
//                    Collections.unmodifiableList(requestsList)
                    listOfNotNull<DocWriteRequest<*>>(
                        DeleteRequest(alertsIndex, alert.id)
                            .routing(routingId),
                        if (alertIndices.isAlertHistoryEnabled()) {
                            // Only add completed alert to history index if history is enabled
                            IndexRequest(alertsHistoryIndex)
                                .routing(routingId)
                                .source(alert.toXContentWithUser(XContentFactory.jsonBuilder()))
                                .id(alert.id)
                        } else {
                            // Otherwise, prepare the Alert's Notes for deletion, and don't include
                            // a request to index the Alert to an Alert history index
                            notesToDeleteIDs.addAll(NotesUtils.getNoteIDsByAlertIDs(client, listOf(alert.id)))
                            null
                        }
                    )
                }
            }
        }

        if (requestsToRetry.isEmpty()) return
        // Retry Bulk requests if there was any 429 response
        retryPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
            val bulkRequest = BulkRequest().add(requestsToRetry).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            val bulkResponse: BulkResponse = client.suspendUntil { client.bulk(bulkRequest, it) }
            val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
            requestsToRetry = failedResponses.filter { it.status() == RestStatus.TOO_MANY_REQUESTS }
                .map { bulkRequest.requests()[it.itemId] as IndexRequest }

            if (requestsToRetry.isNotEmpty()) {
                val retryCause = failedResponses.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
                throw ExceptionsHelper.convertToOpenSearchException(retryCause)
            }
        }

        // delete all the Notes of any Alerts that were deleted
        NotesUtils.deleteNotes(client, notesToDeleteIDs)
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
        var requestsToRetry: MutableList<IndexRequest> = alerts.map { alert ->
            if (alert.state != Alert.State.ACTIVE && alert.state != Alert.State.AUDIT) {
                throw IllegalStateException("Unexpected attempt to save new alert [$alert] with state [${alert.state}]")
            }
            if (alert.id != Alert.NO_ID) {
                throw IllegalStateException("Unexpected attempt to save new alert [$alert] with an existing alert ID [${alert.id}]")
            }
            val alertIndex = if (alert.state == Alert.State.AUDIT && alertIndices.isAlertHistoryEnabled()) {
                dataSources.alertsHistoryIndex
            } else dataSources.alertsIndex
            IndexRequest(alertIndex)
                .routing(alert.monitorId)
                .source(alert.toXContentWithUser(XContentFactory.jsonBuilder()))
        }.toMutableList()

        if (requestsToRetry.isEmpty()) return listOf()

        // Retry Bulk requests if there was any 429 response.
        // The responses of a bulk request will be in the same order as the individual requests.
        // If the index request succeeded for an Alert, the document ID from the response is taken and saved in the Alert.
        // If the index request is to be retried, the Alert is saved separately as well so that its relative ordering is maintained in
        // relation to index request in the retried bulk request for when it eventually succeeds.
        retryPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
            val bulkRequest = BulkRequest().add(requestsToRetry).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            val bulkResponse: BulkResponse = client.suspendUntil { client.bulk(bulkRequest, it) }
            // TODO: This is only used to retrieve the retryCause, could instead fetch it from the bulkResponse iteration below
            val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }

            requestsToRetry = mutableListOf()
            val alertsBeingRetried = mutableListOf<Alert>()
            bulkResponse.items.forEach { item ->
                if (item.isFailed) {
                    // TODO: What if the failure cause was not TOO_MANY_REQUESTS, should these be saved and logged?
                    if (item.status() == RestStatus.TOO_MANY_REQUESTS) {
                        requestsToRetry.add(bulkRequest.requests()[item.itemId] as IndexRequest)
                        alertsBeingRetried.add(alertsBeingIndexed[item.itemId])
                    }
                } else {
                    // The ID of the BulkItemResponse in this case is the document ID resulting from the DocWriteRequest operation
                    savedAlerts.add(alertsBeingIndexed[item.itemId].copy(id = item.id))
                }
            }

            alertsBeingIndexed = alertsBeingRetried

            if (requestsToRetry.isNotEmpty()) {
                val retryCause = failedResponses.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
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

        val searchRequest = SearchRequest(alertIndex)
            .routing(monitorId)
            .source(searchSourceBuilder)
        val searchResponse: SearchResponse = client.suspendUntil { client.search(searchRequest, it) }
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

        val searchRequest = SearchRequest(alertIndex)
            .routing(workflowId)
            .source(searchSourceBuilder)
        val searchResponse: SearchResponse = client.suspendUntil { client.search(searchRequest, it) }
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
