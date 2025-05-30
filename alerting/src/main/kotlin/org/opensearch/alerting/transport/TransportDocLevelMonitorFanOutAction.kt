/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchSecurityException
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.admin.indices.refresh.RefreshAction
import org.opensearch.action.admin.indices.refresh.RefreshRequest
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.get.MultiGetItemResponse
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchAction
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.AlertService
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.TriggerService
import org.opensearch.alerting.action.GetDestinationsAction
import org.opensearch.alerting.action.GetDestinationsRequest
import org.opensearch.alerting.action.GetDestinationsResponse
import org.opensearch.alerting.model.AlertContext
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.model.destination.DestinationContextFactory
import org.opensearch.alerting.opensearchapi.InjectorContextElement
import org.opensearch.alerting.opensearchapi.convertToMap
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.opensearchapi.withClosableContext
import org.opensearch.alerting.script.DocumentLevelTriggerExecutionContext
import org.opensearch.alerting.script.QueryLevelTriggerExecutionContext
import org.opensearch.alerting.script.TriggerExecutionContext
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_BACKOFF_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_BACKOFF_MILLIS
import org.opensearch.alerting.settings.AlertingSettings.Companion.DOC_LEVEL_MONITOR_FANOUT_MAX_DURATION
import org.opensearch.alerting.settings.AlertingSettings.Companion.DOC_LEVEL_MONITOR_FETCH_ONLY_QUERY_FIELDS_ENABLED
import org.opensearch.alerting.settings.AlertingSettings.Companion.DOC_LEVEL_MONITOR_SHARD_FETCH_SIZE
import org.opensearch.alerting.settings.AlertingSettings.Companion.FINDINGS_INDEXING_BATCH_SIZE
import org.opensearch.alerting.settings.AlertingSettings.Companion.MAX_ACTIONABLE_ALERT_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.PERCOLATE_QUERY_DOCS_SIZE_MEMORY_PERCENTAGE_LIMIT
import org.opensearch.alerting.settings.AlertingSettings.Companion.PERCOLATE_QUERY_MAX_NUM_DOCS_IN_MEMORY
import org.opensearch.alerting.settings.DestinationSettings
import org.opensearch.alerting.util.defaultToPerExecutionAction
import org.opensearch.alerting.util.destinationmigration.NotificationActionConfigs
import org.opensearch.alerting.util.destinationmigration.NotificationApiUtils
import org.opensearch.alerting.util.destinationmigration.getTitle
import org.opensearch.alerting.util.destinationmigration.publishLegacyNotification
import org.opensearch.alerting.util.destinationmigration.sendNotification
import org.opensearch.alerting.util.getActionExecutionPolicy
import org.opensearch.alerting.util.isAllowed
import org.opensearch.alerting.util.isTestAction
import org.opensearch.alerting.util.parseSampleDocTags
import org.opensearch.alerting.util.printsSampleDocData
import org.opensearch.alerting.util.use
import org.opensearch.cluster.routing.Preference
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.AlertingPluginInterface
import org.opensearch.commons.alerting.action.DocLevelMonitorFanOutAction
import org.opensearch.commons.alerting.action.DocLevelMonitorFanOutRequest
import org.opensearch.commons.alerting.action.DocLevelMonitorFanOutResponse
import org.opensearch.commons.alerting.action.PublishFindingsRequest
import org.opensearch.commons.alerting.action.SubscribeFindingsResponse
import org.opensearch.commons.alerting.model.ActionExecutionResult
import org.opensearch.commons.alerting.model.ActionRunResult
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.DocumentLevelTrigger
import org.opensearch.commons.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.commons.alerting.model.Finding
import org.opensearch.commons.alerting.model.IndexExecutionContext
import org.opensearch.commons.alerting.model.InputRunResults
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorMetadata
import org.opensearch.commons.alerting.model.MonitorRunResult
import org.opensearch.commons.alerting.model.Table
import org.opensearch.commons.alerting.model.WorkflowRunContext
import org.opensearch.commons.alerting.model.action.Action
import org.opensearch.commons.alerting.model.action.PerAlertActionScope
import org.opensearch.commons.alerting.model.userErrorMessage
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.alerting.util.string
import org.opensearch.commons.notifications.model.NotificationConfigInfo
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.Strings
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.Operator
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indices.IndexClosedException
import org.opensearch.monitor.jvm.JvmStats
import org.opensearch.percolator.PercolateQueryBuilderExt
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript
import org.opensearch.search.SearchHit
import org.opensearch.search.SearchHits
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.search.sort.SortOrder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.node.NodeClient
import java.io.IOException
import java.time.Instant
import java.util.UUID
import java.util.stream.Collectors

private val log = LogManager.getLogger(TransportDocLevelMonitorFanOutAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportDocLevelMonitorFanOutAction
@Inject constructor(
    transportService: TransportService,
    val client: Client,
    val actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val triggerService: TriggerService,
    val alertService: AlertService,
    val scriptService: ScriptService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<DocLevelMonitorFanOutRequest, DocLevelMonitorFanOutResponse>(
    DocLevelMonitorFanOutAction.NAME, transportService, actionFilters, ::DocLevelMonitorFanOutRequest
),
    SecureTransportAction {
    var nonPercolateSearchesTimeTakenStat = 0L
    var percolateQueriesTimeTakenStat = 0L
    var totalDocsQueriedStat = 0L
    var docTransformTimeTakenStat = 0L
    var totalDocsSizeInBytesStat = 0L
    var docsSizeOfBatchInBytes = 0L
    var findingsToTriggeredQueries: Map<String, List<DocLevelQuery>> = mutableMapOf()

    @Volatile var percQueryMaxNumDocsInMemory: Int = PERCOLATE_QUERY_MAX_NUM_DOCS_IN_MEMORY.get(settings)
    @Volatile var docLevelMonitorFanoutMaxDuration = DOC_LEVEL_MONITOR_FANOUT_MAX_DURATION.get(settings)
    @Volatile var percQueryDocsSizeMemoryPercentageLimit: Int = PERCOLATE_QUERY_DOCS_SIZE_MEMORY_PERCENTAGE_LIMIT.get(settings)
    @Volatile var docLevelMonitorShardFetchSize: Int = DOC_LEVEL_MONITOR_SHARD_FETCH_SIZE.get(settings)
    @Volatile var findingsIndexBatchSize: Int = FINDINGS_INDEXING_BATCH_SIZE.get(settings)
    @Volatile var maxActionableAlertCount: Long = MAX_ACTIONABLE_ALERT_COUNT.get(settings)
    @Volatile var retryPolicy = BackoffPolicy.constantBackoff(ALERT_BACKOFF_MILLIS.get(settings), ALERT_BACKOFF_COUNT.get(settings))
    @Volatile var allowList: List<String> = DestinationSettings.ALLOW_LIST.get(settings)
    @Volatile var fetchOnlyQueryFieldNames = DOC_LEVEL_MONITOR_FETCH_ONLY_QUERY_FIELDS_ENABLED.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(PERCOLATE_QUERY_MAX_NUM_DOCS_IN_MEMORY) {
            percQueryMaxNumDocsInMemory = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(DOC_LEVEL_MONITOR_FANOUT_MAX_DURATION) {
            docLevelMonitorFanoutMaxDuration = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(PERCOLATE_QUERY_DOCS_SIZE_MEMORY_PERCENTAGE_LIMIT) {
            percQueryDocsSizeMemoryPercentageLimit = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(DOC_LEVEL_MONITOR_SHARD_FETCH_SIZE) {
            docLevelMonitorShardFetchSize = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(FINDINGS_INDEXING_BATCH_SIZE) {
            findingsIndexBatchSize = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(MAX_ACTIONABLE_ALERT_COUNT) {
            maxActionableAlertCount = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_BACKOFF_MILLIS, ALERT_BACKOFF_COUNT) { millis, count ->
            retryPolicy = BackoffPolicy.constantBackoff(millis, count)
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(DestinationSettings.ALLOW_LIST) {
            allowList = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(DOC_LEVEL_MONITOR_FETCH_ONLY_QUERY_FIELDS_ENABLED) {
            fetchOnlyQueryFieldNames = it
        }
    }

    @Volatile
    override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    override fun doExecute(
        task: Task,
        request: DocLevelMonitorFanOutRequest,
        listener: ActionListener<DocLevelMonitorFanOutResponse>
    ) {
        scope.launch {
            executeMonitor(request, listener)
        }
    }

    private suspend fun executeMonitor(
        request: DocLevelMonitorFanOutRequest,
        listener: ActionListener<DocLevelMonitorFanOutResponse>
    ) {
        try {
            val endTime = Instant.now().plusMillis(docLevelMonitorFanoutMaxDuration.millis())
            val monitor = request.monitor
            var monitorResult = MonitorRunResult<DocumentLevelTriggerRunResult>(monitor.name, Instant.now(), Instant.now())
            val updatedIndexNames = request.indexExecutionContext!!.updatedIndexNames
            val monitorMetadata = request.monitorMetadata
            val shardIds = request.shardIds
            val indexExecutionContext = request.indexExecutionContext
            val concreteIndicesSeenSoFar = request.concreteIndicesSeenSoFar
            val dryrun = request.dryRun
            val executionId = request.executionId
            val workflowRunContext = request.workflowRunContext

            val queryToDocIds = mutableMapOf<DocLevelQuery, MutableSet<String>>()
            val inputRunResults = mutableMapOf<String, MutableSet<String>>()
            val docsToQueries = mutableMapOf<String, MutableList<String>>()
            val transformedDocs = mutableListOf<Pair<String, TransformedDocDto>>()
            val findingIdToDocSource = mutableMapOf<String, MultiGetItemResponse>()
            val isTempMonitor = dryrun || monitor.id == Monitor.NO_ID

            val docLevelMonitorInput = request.monitor.inputs[0] as DocLevelMonitorInput
            val queries: List<DocLevelQuery> = docLevelMonitorInput.queries
            val fieldsToBeQueried = mutableSetOf<String>()
            if (fetchOnlyQueryFieldNames) {
                for (it in queries) {
                    if (it.queryFieldNames.isEmpty()) {
                        fieldsToBeQueried.clear()
                        log.debug(
                            "Monitor ${request.monitor.id} : " +
                                "Doc Level query ${it.id} : ${it.query} doesn't have queryFieldNames populated. " +
                                "Cannot optimize monitor to fetch only query-relevant fields. " +
                                "Querying entire doc source."
                        )
                        break
                    }
                    fieldsToBeQueried.addAll(it.queryFieldNames)
                }
                if (fieldsToBeQueried.isNotEmpty()) {
                    log.debug(
                        "Monitor ${monitor.id} Querying only fields " +
                            "${fieldsToBeQueried.joinToString()} instead of entire _source of documents"
                    )
                }
            }

            fetchShardDataAndMaybeExecutePercolateQueries(
                monitor,
                endTime,
                indexExecutionContext!!,
                monitorMetadata,
                inputRunResults,
                docsToQueries,
                updatedIndexNames,
                concreteIndicesSeenSoFar,
                ArrayList(fieldsToBeQueried),
                shardIds.map { it.id },
                transformedDocs
            ) { shard, maxSeqNo -> // function passed to update last run context with new max sequence number
                indexExecutionContext.updatedLastRunContext[shard] = maxSeqNo
            }
            if (transformedDocs.isNotEmpty()) {
                performPercolateQueryAndResetCounters(
                    monitor,
                    monitorMetadata,
                    updatedIndexNames,
                    concreteIndicesSeenSoFar,
                    inputRunResults,
                    docsToQueries,
                    transformedDocs
                )
            }
            monitorResult = monitorResult.copy(inputResults = InputRunResults(listOf(inputRunResults)))

            /*
             populate the map queryToDocIds with pairs of <DocLevelQuery object from queries in monitor metadata &
             list of matched docId from inputRunResults>
             this fixes the issue of passing id, name, tags fields of DocLevelQuery object correctly to TriggerExpressionParser
             */
            queries.forEach {
                if (inputRunResults.containsKey(it.id)) {
                    queryToDocIds[it] = inputRunResults[it.id]!!
                }
            }

            val idQueryMap: Map<String, DocLevelQuery> = queries.associateBy { it.id }

            val triggerResults = mutableMapOf<String, DocumentLevelTriggerRunResult>()
            // If there are no triggers defined, we still want to generate findings
            if (monitor.triggers.isEmpty()) {
                if (dryrun == false && monitor.id != Monitor.NO_ID) {
                    createFindings(monitor, docsToQueries, idQueryMap, true)
                }
            } else {
                /**
                 * if should_persist_findings_and_alerts flag is not set, doc-level trigger generates alerts else doc-level trigger
                 * generates a single alert with multiple findings.
                 */
                if (monitor.shouldCreateSingleAlertForFindings == null || monitor.shouldCreateSingleAlertForFindings == false) {
                    monitor.triggers.forEach {
                        triggerResults[it.id] = runForEachDocTrigger(
                            monitorResult,
                            it as DocumentLevelTrigger,
                            monitor,
                            idQueryMap,
                            docsToQueries,
                            queryToDocIds,
                            dryrun,
                            executionId = executionId,
                            findingIdToDocSource,
                            workflowRunContext = workflowRunContext
                        )
                    }
                } else if (monitor.shouldCreateSingleAlertForFindings == true) {
                    monitor.triggers.forEach {
                        triggerResults[it.id] = runForEachDocTriggerCreateSingleGroupedAlert(
                            monitorResult,
                            it as DocumentLevelTrigger,
                            monitor,
                            queryToDocIds,
                            dryrun,
                            executionId,
                            workflowRunContext
                        )
                    }
                }
            }

            if (!isTempMonitor) {
                // If any error happened during trigger execution, upsert monitor error alert
                val errorMessage = constructErrorMessageFromTriggerResults(triggerResults = triggerResults)
                log.info(errorMessage)
                if (errorMessage.isNotEmpty()) {
                    alertService.upsertMonitorErrorAlert(
                        monitor = monitor,
                        errorMessage = errorMessage,
                        executionId = executionId,
                        workflowRunContext
                    )
                } else {
                    onSuccessfulMonitorRun(monitor)
                }
            }

            listener.onResponse(
                DocLevelMonitorFanOutResponse(
                    nodeId = clusterService.localNode().id,
                    executionId = request.executionId,
                    monitorId = monitor.id,
                    indexExecutionContext.updatedLastRunContext,
                    InputRunResults(listOf(inputRunResults)),
                    triggerResults
                )
            )
        } catch (e: Exception) {
            log.error(
                "${request.monitor.id} Failed to run fan_out on node ${clusterService.localNode().id}." +
                    " for Monitor Type ${request.monitor.monitorType} ExecutionId ${request.executionId}",
                e
            )
            listener.onFailure(AlertingException.wrap(e))
        }
    }

    /**
     * run doc-level triggers ignoring findings and alerts and generating a single alert.
     */
    private suspend fun runForEachDocTriggerCreateSingleGroupedAlert(
        monitorResult: MonitorRunResult<DocumentLevelTriggerRunResult>,
        trigger: DocumentLevelTrigger,
        monitor: Monitor,
        queryToDocIds: Map<DocLevelQuery, Set<String>>,
        dryrun: Boolean,
        executionId: String,
        workflowRunContext: WorkflowRunContext?
    ): DocumentLevelTriggerRunResult {
        val triggerResult = triggerService.runDocLevelTrigger(monitor, trigger, queryToDocIds)
        if (triggerResult.triggeredDocs.isNotEmpty()) {
            val findingIds = if (workflowRunContext?.findingIds != null) {
                workflowRunContext.findingIds
            } else {
                listOf()
            }
            val triggerCtx = DocumentLevelTriggerExecutionContext(monitor, trigger)
            val alert = alertService.composeDocLevelAlert(
                findingIds!!,
                triggerResult.triggeredDocs,
                triggerCtx,
                monitorResult.alertError() ?: triggerResult.alertError(),
                executionId = executionId,
                workflorwRunContext = workflowRunContext
            )
            for (action in trigger.actions) {
                this.runAction(action, triggerCtx.copy(alerts = listOf(AlertContext(alert))), monitor, dryrun)
            }

            if (!dryrun && monitor.id != Monitor.NO_ID) {
                val actionResults = triggerResult.actionResultsMap.getOrDefault(alert.id, emptyMap())
                val actionExecutionResults = actionResults.values.map { actionRunResult ->
                    ActionExecutionResult(actionRunResult.actionId, actionRunResult.executionTime, if (actionRunResult.throttled) 1 else 0)
                }
                val updatedAlert = alert.copy(actionExecutionResults = actionExecutionResults)

                retryPolicy.let {
                    alertService.saveAlerts(
                        monitor.dataSources,
                        listOf(updatedAlert),
                        it,
                        routingId = monitor.id
                    )
                }
            }
        }
        return DocumentLevelTriggerRunResult(trigger.name, listOf(), monitorResult.error)
    }

    private suspend fun runForEachDocTrigger(
        monitorResult: MonitorRunResult<DocumentLevelTriggerRunResult>,
        trigger: DocumentLevelTrigger,
        monitor: Monitor,
        idQueryMap: Map<String, DocLevelQuery>,
        docsToQueries: MutableMap<String, MutableList<String>>,
        queryToDocIds: Map<DocLevelQuery, Set<String>>,
        dryrun: Boolean,
        executionId: String,
        findingIdToDocSource: MutableMap<String, MultiGetItemResponse>,
        workflowRunContext: WorkflowRunContext?
    ): DocumentLevelTriggerRunResult {
        val triggerCtx = DocumentLevelTriggerExecutionContext(monitor, trigger)
        val triggerResult = triggerService.runDocLevelTrigger(monitor, trigger, queryToDocIds)

        val triggerFindingDocPairs = mutableListOf<Pair<String, String>>()

        // TODO: Implement throttling for findings
        val findingToDocPairs = createFindings(
            monitor,
            docsToQueries,
            idQueryMap,
            !dryrun && monitor.id != Monitor.NO_ID,
            executionId
        )

        findingToDocPairs.forEach {
            // Only pick those entries whose docs have triggers associated with them
            if (triggerResult.triggeredDocs.contains(it.second)) {
                triggerFindingDocPairs.add(Pair(it.first, it.second))
            }
        }

        val actionCtx = triggerCtx.copy(
            triggeredDocs = triggerResult.triggeredDocs,
            relatedFindings = findingToDocPairs.map { it.first },
            error = monitorResult.error ?: triggerResult.error
        )

        if (printsSampleDocData(trigger) && triggerFindingDocPairs.isNotEmpty())
            getDocSources(
                findingToDocPairs = findingToDocPairs,
                monitor = monitor,
                findingIdToDocSource = findingIdToDocSource
            )

        val alerts = mutableListOf<Alert>()
        val alertContexts = mutableListOf<AlertContext>()
        triggerFindingDocPairs.forEach {
            val alert = alertService.composeDocLevelAlert(
                listOf(it.first),
                listOf(it.second),
                triggerCtx,
                monitorResult.alertError() ?: triggerResult.alertError(),
                executionId = executionId,
                workflorwRunContext = workflowRunContext
            )
            alerts.add(alert)

            val docSource = findingIdToDocSource[alert.findingIds.first()]?.response?.convertToMap()

            alertContexts.add(
                AlertContext(
                    alert = alert,
                    associatedQueries = alert.findingIds.flatMap { findingId ->
                        findingsToTriggeredQueries.getOrDefault(findingId, emptyList()) ?: emptyList()
                    },
                    sampleDocs = listOfNotNull(docSource)
                )
            )
        }

        val shouldDefaultToPerExecution = defaultToPerExecutionAction(
            maxActionableAlertCount,
            monitorId = monitor.id,
            triggerId = trigger.id,
            totalActionableAlertCount = alerts.size,
            monitorOrTriggerError = actionCtx.error
        )

        for (action in trigger.actions) {
            val actionExecutionScope = action.getActionExecutionPolicy(monitor)!!.actionExecutionScope
            if (actionExecutionScope is PerAlertActionScope && !shouldDefaultToPerExecution) {
                for (alertContext in alertContexts) {
                    val actionResults = this.runAction(action, actionCtx.copy(alerts = listOf(alertContext)), monitor, dryrun)
                    triggerResult.actionResultsMap.getOrPut(alertContext.alert.id) { mutableMapOf() }
                    triggerResult.actionResultsMap[alertContext.alert.id]?.set(action.id, actionResults)
                }
            } else if (alertContexts.isNotEmpty()) {
                val actionResults = this.runAction(action, actionCtx.copy(alerts = alertContexts), monitor, dryrun)
                for (alert in alerts) {
                    triggerResult.actionResultsMap.getOrPut(alert.id) { mutableMapOf() }
                    triggerResult.actionResultsMap[alert.id]?.set(action.id, actionResults)
                }
            }
        }

        // Alerts are saved after the actions since if there are failures in the actions, they can be stated in the alert
        if (!dryrun && monitor.id != Monitor.NO_ID) {
            val updatedAlerts = alerts.map { alert ->
                val actionResults = triggerResult.actionResultsMap.getOrDefault(alert.id, emptyMap())
                val actionExecutionResults = actionResults.values.map { actionRunResult ->
                    ActionExecutionResult(actionRunResult.actionId, actionRunResult.executionTime, if (actionRunResult.throttled) 1 else 0)
                }
                alert.copy(actionExecutionResults = actionExecutionResults)
            }

            retryPolicy.let {
                alertService.saveAlerts(
                    monitor.dataSources,
                    updatedAlerts,
                    it,
                    routingId = monitor.id
                )
            }
        }
        return triggerResult
    }

    /**
     * 1. Bulk index all findings based on shouldCreateFinding flag
     * 2. invoke publishFinding() to kickstart auto-correlations
     * 3. Returns a list of pairs for finding id to doc id
     */
    private suspend fun createFindings(
        monitor: Monitor,
        docsToQueries: MutableMap<String, MutableList<String>>,
        idQueryMap: Map<String, DocLevelQuery>,
        shouldCreateFinding: Boolean,
        workflowExecutionId: String? = null,
    ): List<Pair<String, String>> {

        val findingDocPairs = mutableListOf<Pair<String, String>>()
        val findings = mutableListOf<Finding>()
        val indexRequests = mutableListOf<IndexRequest>()
        val findingsToTriggeredQueries = mutableMapOf<String, List<DocLevelQuery>>()

        docsToQueries.forEach {
            val triggeredQueries = it.value.map { queryId -> idQueryMap[queryId]!! }

            // Before the "|" is the doc id and after the "|" is the index
            val docIndex = it.key.split("|")

            val finding = Finding(
                id = UUID.randomUUID().toString(),
                relatedDocIds = listOf(docIndex[0]),
                correlatedDocIds = listOf(docIndex[0]),
                monitorId = monitor.id,
                monitorName = monitor.name,
                index = docIndex[1],
                docLevelQueries = triggeredQueries,
                timestamp = Instant.now(),
                executionId = workflowExecutionId
            )
            findingDocPairs.add(Pair(finding.id, it.key))
            findings.add(finding)
            findingsToTriggeredQueries[finding.id] = triggeredQueries

            val findingStr =
                finding.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS)
                    .string()
            log.debug("Findings: $findingStr")

            if (shouldCreateFinding and (
                monitor.shouldCreateSingleAlertForFindings == null ||
                    monitor.shouldCreateSingleAlertForFindings == false
                )
            ) {
                indexRequests += IndexRequest(monitor.dataSources.findingsIndex)
                    .source(findingStr, XContentType.JSON)
                    .id(finding.id)
                    .opType(DocWriteRequest.OpType.CREATE)
            }
        }

        if (indexRequests.isNotEmpty()) {
            bulkIndexFindings(monitor, indexRequests)
        }

        if (monitor.shouldCreateSingleAlertForFindings == null || monitor.shouldCreateSingleAlertForFindings == false) {
            try {
                findings.forEach { finding ->
                    publishFinding(monitor, finding)
                }
            } catch (e: Exception) {
                // suppress exception
                log.error("Optional finding callback failed", e)
            }
        }
        this.findingsToTriggeredQueries += findingsToTriggeredQueries

        return findingDocPairs
    }

    private suspend fun bulkIndexFindings(
        monitor: Monitor,
        indexRequests: List<IndexRequest>
    ) {
        indexRequests.chunked(findingsIndexBatchSize).forEach { batch ->
            val bulkResponse: BulkResponse = client.suspendUntil {
                bulk(BulkRequest().add(batch), it)
            }
            if (bulkResponse.hasFailures()) {
                bulkResponse.items.forEach { item ->
                    if (item.isFailed) {
                        log.error("Failed indexing the finding ${item.id} of monitor [${monitor.id}]")
                    }
                }
            } else {
                log.debug("[${bulkResponse.items.size}] All findings successfully indexed.")
            }
        }
        client.execute(RefreshAction.INSTANCE, RefreshRequest(monitor.dataSources.findingsIndex))
    }

    private fun publishFinding(
        monitor: Monitor,
        finding: Finding
    ) {
        val publishFindingsRequest = PublishFindingsRequest(monitor.id, finding)
        AlertingPluginInterface.publishFinding(
            client as NodeClient,
            publishFindingsRequest,
            object : ActionListener<SubscribeFindingsResponse> {
                override fun onResponse(response: SubscribeFindingsResponse) {}

                override fun onFailure(e: Exception) {}
            }
        )
    }

    suspend fun runAction(
        action: Action,
        ctx: TriggerExecutionContext,
        monitor: Monitor,
        dryrun: Boolean
    ): ActionRunResult {
        return try {
            if (ctx is QueryLevelTriggerExecutionContext && !MonitorRunnerService.isActionActionable(action, ctx.alert?.alert)) {
                return ActionRunResult(action.id, action.name, mapOf(), true, null, null)
            }
            val actionOutput = mutableMapOf<String, String>()
            actionOutput[Action.SUBJECT] = if (action.subjectTemplate != null)
                compileTemplate(action.subjectTemplate!!, ctx)
            else ""
            actionOutput[Action.MESSAGE] = compileTemplate(action.messageTemplate, ctx)
            if (Strings.isNullOrEmpty(actionOutput[Action.MESSAGE])) {
                throw IllegalStateException("Message content missing in the Destination with id: ${action.destinationId}")
            }
            if (!dryrun) {
                client.threadPool().threadContext.stashContext().use {
                    withClosableContext(
                        InjectorContextElement(
                            monitor.id,
                            settings,
                            client.threadPool().threadContext,
                            monitor.user?.roles,
                            monitor.user
                        )
                    ) {
                        actionOutput[Action.MESSAGE_ID] = getConfigAndSendNotification(
                            action,
                            actionOutput[Action.SUBJECT],
                            actionOutput[Action.MESSAGE]!!
                        )
                    }
                }
            }
            ActionRunResult(
                action.id,
                action.name,
                actionOutput,
                false,
                Instant.ofEpochMilli(client.threadPool().absoluteTimeInMillis()),
                null
            )
        } catch (e: Exception) {
            ActionRunResult(action.id, action.name, mapOf(), false, Instant.ofEpochMilli(client.threadPool().absoluteTimeInMillis()), e)
        }
    }

    protected suspend fun getConfigAndSendNotification(
        action: Action,
        subject: String?,
        message: String
    ): String {
        val config = getConfigForNotificationAction(action)
        if (config.destination == null && config.channel == null) {
            throw IllegalStateException("Unable to find a Notification Channel or Destination config with id [${action.destinationId}]")
        }

        // Adding a check on TEST_ACTION Destination type here to avoid supporting it as a LegacyBaseMessage type
        // just for Alerting integration tests
        if (config.destination?.isTestAction() == true) {
            return "test action"
        }

        if (config.destination?.isAllowed(allowList) == false) {
            throw IllegalStateException(
                "Monitor contains a Destination type that is not allowed: ${config.destination.type}"
            )
        }

        var actionResponseContent = ""
        actionResponseContent = config.channel
            ?.sendNotification(
                client,
                config.channel.getTitle(subject),
                message
            ) ?: actionResponseContent

        actionResponseContent = config.destination
            ?.buildLegacyBaseMessage(subject, message, getDestinationContextFactory().getDestinationContext(config.destination))
            ?.publishLegacyNotification(client)
            ?: actionResponseContent

        return actionResponseContent
    }

    private fun isFanOutTimeEnded(endTime: Instant): Boolean {
        return Instant.now().isAfter(endTime)
    }

    /** 1. Fetch data per shard for given index. (only 10000 docs are fetched.
     * needs to be converted to scroll if not performant enough)
     *  2. Transform documents to conform to format required for percolate query
     *  3a. Check if docs in memory are crossing threshold defined by setting.
     *  3b. If yes, perform percolate query and update docToQueries Map with all hits from percolate queries */
    suspend fun fetchShardDataAndMaybeExecutePercolateQueries( // package-private for testing visibility
        monitor: Monitor,
        endTime: Instant,
        indexExecutionCtx: IndexExecutionContext,
        monitorMetadata: MonitorMetadata,
        inputRunResults: MutableMap<String, MutableSet<String>>,
        docsToQueries: MutableMap<String, MutableList<String>>,
        monitorInputIndices: List<String>,
        concreteIndices: List<String>,
        fieldsToBeQueried: List<String>,
        shardList: List<Int>,
        transformedDocs: MutableList<Pair<String, TransformedDocDto>>,
        updateLastRunContext: (String, String) -> Unit
    ) {
        for (shardId in shardList) {
            val shard = shardId.toString()
            try {
                val prevSeqNo = indexExecutionCtx.lastRunContext[shard].toString().toLongOrNull()
                val from = prevSeqNo ?: SequenceNumbers.NO_OPS_PERFORMED
                if (isFanOutTimeEnded(endTime)) {
                    log.info(
                        "Doc level monitor ${monitor.id}: " +
                            "Fanout execution on node ${clusterService.localNode().id}" +
                            " unable to complete in $docLevelMonitorFanoutMaxDuration." +
                            " Skipping shard [${indexExecutionCtx.concreteIndexName}][$shardId]"
                    )
                    continue
                }
                // First get the max sequence number for the shard
                val maxSeqNo = getMaxSeqNoForShard(
                    monitor,
                    indexExecutionCtx.concreteIndexName,
                    shard,
                    indexExecutionCtx.docIds
                )

                if (maxSeqNo == null || maxSeqNo <= from) {
                    // No new documents to process
                    updateLastRunContext(shard, (prevSeqNo ?: SequenceNumbers.NO_OPS_PERFORMED).toString())
                    continue
                }
                // Process documents in chunks between prevSeqNo and maxSeqNo
                var currentSeqNo = from
                while (currentSeqNo < maxSeqNo) {
                    if (isFanOutTimeEnded(endTime)) { // process percolate queries and exit
                        if (
                            transformedDocs.isNotEmpty() &&
                            shouldPerformPercolateQueryAndFlushInMemoryDocs(transformedDocs.size)
                        ) {
                            performPercolateQueryAndResetCounters(
                                monitor,
                                monitorMetadata,
                                monitorInputIndices,
                                concreteIndices,
                                inputRunResults,
                                docsToQueries,
                                transformedDocs
                            )
                        }
                        log.info(
                            "Doc level monitor ${monitor.id}: " +
                                "Fanout execution on node ${clusterService.localNode().id}" +
                                " unable to complete in $docLevelMonitorFanoutMaxDuration!! Gracefully exiting." +
                                "FanoutShardStats: shard[${indexExecutionCtx.concreteIndexName}][$shardId], " +
                                "start_seq_no[$from], current_seq_no[$currentSeqNo], max_seq_no[$maxSeqNo]"
                        )
                        break
                    }
                    val hits = searchShard(
                        monitor,
                        indexExecutionCtx.concreteIndexName,
                        shard,
                        currentSeqNo,
                        maxSeqNo,
                        indexExecutionCtx.docIds,
                        fieldsToBeQueried
                    )

                    if (hits.hits.isEmpty()) {
                        break
                    }

                    val startTime = System.currentTimeMillis()
                    val newDocs = transformSearchHitsAndReconstructDocs(
                        hits,
                        indexExecutionCtx.indexName,
                        indexExecutionCtx.concreteIndexName,
                        monitor.id,
                        indexExecutionCtx.conflictingFields,
                    )

                    transformedDocs.addAll(newDocs)

                    if (
                        transformedDocs.isNotEmpty() &&
                        shouldPerformPercolateQueryAndFlushInMemoryDocs(transformedDocs.size)
                    ) {
                        performPercolateQueryAndResetCounters(
                            monitor,
                            monitorMetadata,
                            monitorInputIndices,
                            concreteIndices,
                            inputRunResults,
                            docsToQueries,
                            transformedDocs
                        )
                    }
                    docTransformTimeTakenStat += System.currentTimeMillis() - startTime

                    // Move to next chunk - use the last document's sequence number
                    currentSeqNo = hits.hits.last().seqNo
                    // update last seen sequence number after every set of seen docs
                    updateLastRunContext(shard, currentSeqNo.toString())
                }
            } catch (e: Exception) {
                log.error(
                    "Monitor ${monitor.id} :" +
                        "Failed to run fetch data from shard [$shard] of index [${indexExecutionCtx.concreteIndexName}]. " +
                        "Error: ${e.message}",
                    e
                )
                if (e is IndexClosedException) {
                    throw e
                }
            }
            if (
                transformedDocs.isNotEmpty() &&
                shouldPerformPercolateQueryAndFlushInMemoryDocs(transformedDocs.size)
            ) {
                performPercolateQueryAndResetCounters(
                    monitor,
                    monitorMetadata,
                    monitorInputIndices,
                    concreteIndices,
                    inputRunResults,
                    docsToQueries,
                    transformedDocs
                )
            }
        }
    }

    private suspend fun performPercolateQueryAndResetCounters(
        monitor: Monitor,
        monitorMetadata: MonitorMetadata,
        monitorInputIndices: List<String>,
        concreteIndices: List<String>,
        inputRunResults: MutableMap<String, MutableSet<String>>,
        docsToQueries: MutableMap<String, MutableList<String>>,
        transformedDocs: MutableList<Pair<String, TransformedDocDto>>
    ) {
        try {
            val percolateQueryResponseHits = runPercolateQueryOnTransformedDocs(
                transformedDocs,
                monitor,
                monitorMetadata,
                concreteIndices,
                monitorInputIndices,
            )

            percolateQueryResponseHits.forEach { hit ->
                var id = hit.id
                concreteIndices.forEach { id = id.replace("_${it}_${monitor.id}", "") }
                monitorInputIndices.forEach { id = id.replace("_${it}_${monitor.id}", "") }
                val docIndices = hit.field("_percolator_document_slot").values.map { it.toString().toInt() }
                docIndices.forEach { idx ->
                    val docIndex = "${transformedDocs[idx].first}|${transformedDocs[idx].second.concreteIndexName}"
                    inputRunResults.getOrPut(id) { mutableSetOf() }.add(docIndex)
                    docsToQueries.getOrPut(docIndex) { mutableListOf() }.add(id)
                }
            }
            totalDocsQueriedStat += transformedDocs.size.toLong()
        } finally {
            transformedDocs.clear()
            docsSizeOfBatchInBytes = 0
        }
    }

    private suspend fun getMaxSeqNoForShard(
        monitor: Monitor,
        index: String,
        shard: String,
        docIds: List<String>? = null
    ): Long? {
        val boolQueryBuilder = BoolQueryBuilder()

        if (monitor.shouldCreateSingleAlertForFindings == null || monitor.shouldCreateSingleAlertForFindings == false) {
            if (!docIds.isNullOrEmpty()) {
                boolQueryBuilder.filter(QueryBuilders.termsQuery("_id", docIds))
            }
        } else if (monitor.shouldCreateSingleAlertForFindings == true) {
            val docIdsParam = mutableListOf<String>()
            if (docIds != null) {
                docIdsParam.addAll(docIds)
            }
            boolQueryBuilder.filter(QueryBuilders.termsQuery("_id", docIdsParam))
        }

        val request = SearchRequest()
            .indices(index)
            .preference("_shards:$shard")
            .source(
                SearchSourceBuilder()
                    .size(1)
                    .sort("_seq_no", SortOrder.DESC)
                    .seqNoAndPrimaryTerm(true)
                    .query(boolQueryBuilder)
            )

        val response: SearchResponse = client.suspendUntil { client.search(request, it) }
        if (response.status() !== RestStatus.OK) {
            throw IOException(
                "Failed to get max sequence number for shard: [$shard] in index [$index]. Response status is ${response.status()}"
            )
        }

        nonPercolateSearchesTimeTakenStat += response.took.millis
        return if (response.hits.hits.isNotEmpty()) response.hits.hits[0].seqNo else null
    }

    /** Executes percolate query on the docs against the monitor's query index and return the hits from the search response*/
    private suspend fun runPercolateQueryOnTransformedDocs(
        docs: MutableList<Pair<String, TransformedDocDto>>,
        monitor: Monitor,
        monitorMetadata: MonitorMetadata,
        concreteIndices: List<String>,
        monitorInputIndices: List<String>,
    ): SearchHits {
        val indices = docs.stream().map { it.second.indexName }.distinct().collect(Collectors.toList())
        val boolQueryBuilder = BoolQueryBuilder().must(buildShouldClausesOverPerIndexMatchQueries(indices))
        val percolateQueryBuilder =
            PercolateQueryBuilderExt("query", docs.map { it.second.docSource }, XContentType.JSON)
        if (monitor.id.isNotEmpty()) {
            boolQueryBuilder.must(QueryBuilders.matchQuery("monitor_id", monitor.id).operator(Operator.AND))
        }
        boolQueryBuilder.filter(percolateQueryBuilder)
        val queryIndices =
            docs.map { monitorMetadata.sourceToQueryIndexMapping[it.second.indexName + monitor.id] }.distinct()
        if (queryIndices.isEmpty()) {
            val message =
                "Monitor ${monitor.id}: Failed to resolve query Indices from source indices during monitor execution!" +
                    " sourceIndices: $monitorInputIndices"
            log.error(message)
            throw AlertingException.wrap(
                OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR)
            )
        }

        val searchRequest =
            SearchRequest().indices(*queryIndices.toTypedArray()).preference(Preference.PRIMARY_FIRST.type())
        val searchSourceBuilder = SearchSourceBuilder()
        searchSourceBuilder.query(boolQueryBuilder)
        searchRequest.source(searchSourceBuilder)
        log.debug(
            "Monitor ${monitor.id}: " +
                "Executing percolate query for docs from source indices " +
                "$monitorInputIndices against query index $queryIndices"
        )
        var response: SearchResponse
        try {
            response = client.suspendUntil {
                client.execute(SearchAction.INSTANCE, searchRequest, it)
            }
        } catch (e: Exception) {
            throw IllegalStateException(
                "Monitor ${monitor.id}:" +
                    " Failed to run percolate search for sourceIndex [${concreteIndices.joinToString()}] " +
                    "and queryIndex [${queryIndices.joinToString()}] for ${docs.size} document(s)",
                e
            )
        }

        if (response.status() !== RestStatus.OK) {
            throw IOException(
                "Monitor ${monitor.id}: Failed to search percolate index: ${queryIndices.joinToString()}. " +
                    "Response status is ${response.status()}"
            )
        }
        log.debug("Monitor ${monitor.id} PERF_DEBUG: Percolate query time taken millis = ${response.took}")
        percolateQueriesTimeTakenStat += response.took.millis
        return response.hits
    }

    /** we cannot use terms query because `index` field's mapping is of type TEXT and not keyword. Refer doc-level-queries.json*/
    private fun buildShouldClausesOverPerIndexMatchQueries(indices: List<String>): BoolQueryBuilder {
        val boolQueryBuilder = QueryBuilders.boolQuery()
        indices.forEach { boolQueryBuilder.should(QueryBuilders.matchQuery("index", it)) }
        return boolQueryBuilder
    }

    /** Executes search query on given shard of given index to fetch docs with sequence number greater than prevSeqNo.
     * This method hence fetches only docs from shard which haven't been queried before
     */
    private suspend fun searchShard(
        monitor: Monitor,
        index: String,
        shard: String,
        prevSeqNo: Long?,
        maxSeqNo: Long,
        docIds: List<String>? = null,
        fieldsToFetch: List<String>,
    ): SearchHits {
        if (prevSeqNo?.equals(maxSeqNo) == true && maxSeqNo != 0L) {
            return SearchHits.empty()
        }
        val boolQueryBuilder = BoolQueryBuilder()
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("_seq_no").gt(prevSeqNo).lte(maxSeqNo))

        if (monitor.shouldCreateSingleAlertForFindings == null || monitor.shouldCreateSingleAlertForFindings == false) {
            if (!docIds.isNullOrEmpty()) {
                boolQueryBuilder.filter(QueryBuilders.termsQuery("_id", docIds))
            }
        } else if (monitor.shouldCreateSingleAlertForFindings == true) {
            val docIdsParam = mutableListOf<String>()
            if (docIds != null) {
                docIdsParam.addAll(docIds)
            }
            boolQueryBuilder.filter(QueryBuilders.termsQuery("_id", docIdsParam))
        }

        val request: SearchRequest = SearchRequest()
            .indices(index)
            .preference("_shards:$shard")
            .source(
                SearchSourceBuilder()
                    .version(true)
                    .sort("_seq_no", SortOrder.ASC)
                    .seqNoAndPrimaryTerm(true)
                    .query(boolQueryBuilder)
                    .size(docLevelMonitorShardFetchSize)
            )

        if (fieldsToFetch.isNotEmpty() && fetchOnlyQueryFieldNames) {
            request.source().fetchSource(false)
            for (field in fieldsToFetch) {
                request.source().fetchField(field)
            }
        }
        val response: SearchResponse = client.suspendUntil { client.search(request, it) }
        if (response.status() !== RestStatus.OK) {
            throw IOException("Failed to search shard: [$shard] in index [$index]. Response status is ${response.status()}")
        }
        nonPercolateSearchesTimeTakenStat += response.took.millis
        return response.hits
    }

    /** Transform field names and index names in all the search hits to format required to run percolate search against them.
     * Hits are transformed using method transformDocumentFieldNames() */
    private fun transformSearchHitsAndReconstructDocs(
        hits: SearchHits,
        index: String,
        concreteIndex: String,
        monitorId: String,
        conflictingFields: List<String>,
    ): List<Pair<String, TransformedDocDto>> {
        return hits.mapNotNull(fun(hit: SearchHit): Pair<String, TransformedDocDto>? {
            try {
                val sourceMap = if (hit.hasSource()) {
                    hit.sourceAsMap
                } else {
                    constructSourceMapFromFieldsInHit(hit)
                }
                transformDocumentFieldNames(
                    sourceMap,
                    conflictingFields,
                    "_${index}_$monitorId",
                    "_${concreteIndex}_$monitorId",
                    ""
                )
                var xContentBuilder = XContentFactory.jsonBuilder().map(sourceMap)
                val sourceRef = BytesReference.bytes(xContentBuilder)
                docsSizeOfBatchInBytes += sourceRef.ramBytesUsed()
                totalDocsSizeInBytesStat += sourceRef.ramBytesUsed()
                return Pair(
                    hit.id,
                    TransformedDocDto(index, concreteIndex, hit.id, sourceRef)
                )
            } catch (e: Exception) {
                log.error("Monitor $monitorId: Failed to transform payload $hit for percolate query", e)
                // skip any document which we fail to transform because we anyway won't be able to run percolate queries on them.
                return null
            }
        })
    }

    private fun constructSourceMapFromFieldsInHit(hit: SearchHit): MutableMap<String, Any> {
        if (hit.fields == null)
            return mutableMapOf()
        val sourceMap: MutableMap<String, Any> = mutableMapOf()
        for (field in hit.fields) {
            if (field.value.values != null && field.value.values.isNotEmpty())
                if (field.value.values.size == 1) {
                    sourceMap[field.key] = field.value.values[0]
                } else sourceMap[field.key] = field.value.values
        }
        return sourceMap
    }

    /**
     * Traverses document fields in leaves recursively and appends [fieldNameSuffixIndex] to field names with same names
     * but different mappings & [fieldNameSuffixPattern] to field names which have unique names.
     *
     * Example for index name is my_log_index and Monitor ID is TReewWdsf2gdJFV:
     * {                         {
     *   "a": {                     "a": {
     *     "b": 1234      ---->       "b_my_log_index_TReewWdsf2gdJFV": 1234
     *   }                          }
     * }
     *
     * @param jsonAsMap               Input JSON (as Map)
     * @param fieldNameSuffix         Field suffix which is appended to existing field name
     */
    private fun transformDocumentFieldNames(
        jsonAsMap: MutableMap<String, Any>,
        conflictingFields: List<String>,
        fieldNameSuffixPattern: String,
        fieldNameSuffixIndex: String,
        fieldNamePrefix: String
    ) {
        val tempMap = mutableMapOf<String, Any>()
        val it: MutableIterator<Map.Entry<String, Any>> = jsonAsMap.entries.iterator()
        while (it.hasNext()) {
            val entry = it.next()
            if (entry.value is Map<*, *>) {
                transformDocumentFieldNames(
                    entry.value as MutableMap<String, Any>,
                    conflictingFields,
                    fieldNameSuffixPattern,
                    fieldNameSuffixIndex,
                    if (fieldNamePrefix == "") entry.key else "$fieldNamePrefix.${entry.key}"
                )
            } else if (!entry.key.endsWith(fieldNameSuffixPattern) && !entry.key.endsWith(fieldNameSuffixIndex)) {
                var alreadyReplaced = false
                conflictingFields.forEach { conflictingField ->
                    if (conflictingField == "$fieldNamePrefix.${entry.key}" || (fieldNamePrefix == "" && conflictingField == entry.key)) {
                        tempMap["${entry.key}$fieldNameSuffixIndex"] = entry.value
                        it.remove()
                        alreadyReplaced = true
                    }
                }
                if (!alreadyReplaced) {
                    tempMap["${entry.key}$fieldNameSuffixPattern"] = entry.value
                    it.remove()
                }
            }
        }
        jsonAsMap.putAll(tempMap)
    }

    private fun shouldPerformPercolateQueryAndFlushInMemoryDocs(
        numDocs: Int
    ): Boolean {
        return isInMemoryDocsSizeExceedingMemoryLimit(docsSizeOfBatchInBytes) ||
            isInMemoryNumDocsExceedingMaxDocsPerPercolateQueryLimit(numDocs)
    }

    /**
     * Returns true, if the docs fetched from shards thus far amount to less than threshold
     * amount of percentage (default:10. setting is dynamic and configurable) of the total heap size or not.
     *
     */
    private fun isInMemoryDocsSizeExceedingMemoryLimit(docsBytesSize: Long): Boolean {
        var thresholdPercentage = percQueryDocsSizeMemoryPercentageLimit
        val heapMaxBytes = JvmStats.jvmStats().mem.heapMax.bytes
        val thresholdBytes = (thresholdPercentage.toDouble() / 100.0) * heapMaxBytes

        return docsBytesSize > thresholdBytes
    }

    private fun isInMemoryNumDocsExceedingMaxDocsPerPercolateQueryLimit(numDocs: Int): Boolean {
        var maxNumDocsThreshold = percQueryMaxNumDocsInMemory
        return numDocs >= maxNumDocsThreshold
    }

    /**
     * Performs an mGet request to retrieve the documents associated with findings.
     *
     * When possible, this will only retrieve the document fields that are specifically
     * referenced for printing in the mustache template.
     */
    private suspend fun getDocSources(
        findingToDocPairs: List<Pair<String, String>>,
        monitor: Monitor,
        findingIdToDocSource: MutableMap<String, MultiGetItemResponse>
    ) {
        val docFieldTags = parseSampleDocTags(monitor.triggers)
        val request = MultiGetRequest()

        // Perform mGet request in batches.
        findingToDocPairs.chunked(findingsIndexBatchSize).forEach { batch ->
            batch.forEach { (findingId, docIdAndIndex) ->
                val docIdAndIndexSplit = docIdAndIndex.split("|")
                val docId = docIdAndIndexSplit[0]
                val concreteIndex = docIdAndIndexSplit[1]
                if (findingId.isNotEmpty() && docId.isNotEmpty() && concreteIndex.isNotEmpty()) {
                    val docItem = MultiGetRequest.Item(concreteIndex, docId)
                    if (docFieldTags.isNotEmpty())
                        docItem.fetchSourceContext(FetchSourceContext(true, docFieldTags.toTypedArray(), emptyArray()))
                    request.add(docItem)
                }
                val response = client.suspendUntil { client.multiGet(request, it) }
                response.responses.forEach { item ->
                    findingIdToDocSource[findingId] = item
                }
            }
        }
    }

    /**
     * The "destination" ID referenced in a Monitor Action could either be a Notification config or a Destination config
     * depending on whether the background migration process has already migrated it from a Destination to a Notification config.
     *
     * To cover both of these cases, the Notification config will take precedence and if it is not found, the Destination will be retrieved.
     */
    private suspend fun getConfigForNotificationAction(
        action: Action
    ): NotificationActionConfigs {
        var destination: Destination? = null
        var notificationPermissionException: Exception? = null

        var channel: NotificationConfigInfo? = null
        try {
            channel =
                NotificationApiUtils.getNotificationConfigInfo(client as NodeClient, action.destinationId)
        } catch (e: OpenSearchSecurityException) {
            notificationPermissionException = e
        }

        // If the channel was not found, try to retrieve the Destination
        if (channel == null) {
            destination = try {
                val table = Table(
                    "asc",
                    "destination.name.keyword",
                    null,
                    1,
                    0,
                    null
                )
                val getDestinationsRequest = GetDestinationsRequest(
                    action.destinationId,
                    0L,
                    null,
                    table,
                    "ALL"
                )

                val getDestinationsResponse: GetDestinationsResponse = client.suspendUntil {
                    client.execute(GetDestinationsAction.INSTANCE, getDestinationsRequest, it)
                }
                getDestinationsResponse.destinations.firstOrNull()
            } catch (e: IllegalStateException) {
                // Catching the exception thrown when the Destination was not found so the NotificationActionConfigs object can be returned
                null
            } catch (e: OpenSearchSecurityException) {
                if (notificationPermissionException != null)
                    throw notificationPermissionException
                else
                    throw e
            }

            if (destination == null && notificationPermissionException != null)
                throw notificationPermissionException
        }

        return NotificationActionConfigs(destination, channel)
    }

    private fun getDestinationContextFactory(): DestinationContextFactory {
        val destinationSettings = DestinationSettings.loadDestinationSettings(settings)
        return DestinationContextFactory(client, xContentRegistry, destinationSettings)
    }

    private fun compileTemplate(template: Script, ctx: TriggerExecutionContext): String {
        return scriptService.compile(template, TemplateScript.CONTEXT)
            .newInstance(template.params + mapOf("ctx" to ctx.asTemplateArg()))
            .execute()
    }

    private suspend fun onSuccessfulMonitorRun(monitor: Monitor) {
        alertService.clearMonitorErrorAlert(monitor)
        if (monitor.dataSources.alertsHistoryIndex != null) {
            alertService.moveClearedErrorAlertsToHistory(
                monitor.id,
                monitor.dataSources.alertsIndex,
                monitor.dataSources.alertsHistoryIndex!!
            )
        }
    }

    private fun constructErrorMessageFromTriggerResults(
        triggerResults: MutableMap<String, DocumentLevelTriggerRunResult>? = null
    ): String {
        var errorMessage = ""
        if (triggerResults != null) {
            val triggersErrorBuilder = StringBuilder()
            triggerResults.forEach {
                if (it.value.error != null) {
                    triggersErrorBuilder.append("[${it.key}]: [${it.value.error!!.userErrorMessage()}]").append(" | ")
                }
            }
            if (triggersErrorBuilder.isNotEmpty()) {
                errorMessage = "Trigger errors: $triggersErrorBuilder"
            }
        }
        return errorMessage
    }

    /**
     * POJO holding information about each doc's concrete index, id, input index pattern/alias/datastream name
     * and doc source. A list of these POJOs would be passed to percolate query execution logic.
     */
    data class TransformedDocDto(
        var indexName: String,
        var concreteIndexName: String,
        var docId: String,
        var docSource: BytesReference
    )
}
