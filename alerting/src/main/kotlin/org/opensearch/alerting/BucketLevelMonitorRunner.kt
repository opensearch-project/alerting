/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.model.ActionRunResult
import org.opensearch.alerting.model.BucketLevelTriggerRunResult
import org.opensearch.alerting.model.InputRunResults
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.opensearchapi.InjectorContextElement
import org.opensearch.alerting.opensearchapi.retry
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.opensearchapi.withClosableContext
import org.opensearch.alerting.script.BucketLevelTriggerExecutionContext
import org.opensearch.alerting.util.defaultToPerExecutionAction
import org.opensearch.alerting.util.getActionExecutionPolicy
import org.opensearch.alerting.util.getBucketKeysHash
import org.opensearch.alerting.util.getCombinedTriggerRunResult
import org.opensearch.alerting.workflow.WorkflowRunContext
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.BucketLevelTrigger
import org.opensearch.commons.alerting.model.Finding
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.alerting.model.action.AlertCategory
import org.opensearch.commons.alerting.model.action.PerAlertActionScope
import org.opensearch.commons.alerting.model.action.PerExecutionActionScope
import org.opensearch.commons.alerting.util.string
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.script.Script
import org.opensearch.script.ScriptType
import org.opensearch.script.TemplateScript
import org.opensearch.search.aggregations.AggregatorFactories
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import java.time.Instant
import java.util.UUID

object BucketLevelMonitorRunner : MonitorRunner() {
    private val logger = LogManager.getLogger(javaClass)

    override suspend fun runMonitor(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryrun: Boolean,
        workflowRunContext: WorkflowRunContext?
    ): MonitorRunResult<BucketLevelTriggerRunResult> {
        val roles = MonitorRunnerService.getRolesForMonitor(monitor)
        logger.debug("Running monitor: ${monitor.name} with roles: $roles Thread: ${Thread.currentThread().name}")

        if (periodStart == periodEnd) {
            logger.warn("Start and end time are the same: $periodStart. This monitor will probably only run once.")
        }

        var monitorResult = MonitorRunResult<BucketLevelTriggerRunResult>(monitor.name, periodStart, periodEnd)
        val currentAlerts = try {
            monitorCtx.alertIndices!!.createOrUpdateAlertIndex(monitor.dataSources)
            monitorCtx.alertIndices!!.createOrUpdateInitialAlertHistoryIndex(monitor.dataSources)
            if (monitor.dataSources.findingsEnabled == true) {
                monitorCtx.alertIndices!!.createOrUpdateInitialFindingHistoryIndex(monitor.dataSources)
            }
            monitorCtx.alertService!!.loadCurrentAlertsForBucketLevelMonitor(monitor)
        } catch (e: Exception) {
            // We can't save ERROR alerts to the index here as we don't know if there are existing ACTIVE alerts
            val id = if (monitor.id.trim().isEmpty()) "_na_" else monitor.id
            logger.error("Error loading alerts for monitor: $id", e)
            return monitorResult.copy(error = e)
        }

        /*
         * Since the aggregation query can consist of multiple pages, each iteration of the do-while loop only has partial results
         * from the runBucketLevelTrigger results whereas the currentAlerts has a complete view of existing Alerts. This means that
         * it can be confirmed if an Alert is new or de-duped local to the do-while loop if a key appears or doesn't appear in
         * the currentAlerts. However, it cannot be guaranteed that an existing Alert is COMPLETED until all pages have been
         * iterated over (since a bucket that did not appear in one page of the aggregation results, could appear in a later page).
         *
         * To solve for this, the currentAlerts will be acting as a list of "potentially completed alerts" throughout the execution.
         * When categorizing the Alerts in each iteration, de-duped Alerts will be removed from the currentAlerts map
         * (for the Trigger being executed) and the Alerts left in currentAlerts after all pages have been iterated through can
         * be marked as COMPLETED since they were never de-duped.
         *
         * Meanwhile, the nextAlerts map will contain Alerts that will exist at the end of this Monitor execution. It is a compilation
         * across Triggers because in the case of executing actions at a PER_EXECUTION frequency, all the Alerts are needed before executing
         * Actions which can only be done once all of the aggregation results (and Triggers given the pagination logic) have been evaluated.
         */
        val triggerResults = mutableMapOf<String, BucketLevelTriggerRunResult>()
        val triggerContexts = mutableMapOf<String, BucketLevelTriggerExecutionContext>()
        val nextAlerts = mutableMapOf<String, MutableMap<AlertCategory, MutableList<Alert>>>()
        var firstIteration = true
        var firstPageOfInputResults = InputRunResults(listOf(), null)
        do {
            // TODO: Since a composite aggregation is being used for the input query, the total bucket count cannot be determined.
            //  If a setting is imposed that limits buckets that can be processed for Bucket-Level Monitors, we'd need to iterate over
            //  the buckets until we hit that threshold. In that case, we'd want to exit the execution without creating any alerts since the
            //  buckets we iterate over before hitting the limit is not deterministic. Is there a better way to fail faster in this case?
            withClosableContext(InjectorContextElement(monitor.id, monitorCtx.settings!!, monitorCtx.threadPool!!.threadContext, roles)) {
                // Storing the first page of results in the case of pagination input results to prevent empty results
                // in the final output of monitorResult which occurs when all pages have been exhausted.
                // If it's favorable to return the last page, will need to check how to accomplish that with multiple aggregation paths
                // with different page counts.
                val inputResults = monitorCtx.inputService!!.collectInputResults(
                    monitor,
                    periodStart,
                    periodEnd,
                    monitorResult.inputResults,
                    workflowRunContext
                )
                if (firstIteration) {
                    firstPageOfInputResults = inputResults
                    firstIteration = false
                }
                monitorResult = monitorResult.copy(inputResults = inputResults)
            }

            for (trigger in monitor.triggers) {
                // The currentAlerts map is formed by iterating over the Monitor's Triggers as keys so null should not be returned here
                val currentAlertsForTrigger = currentAlerts[trigger]!!
                val triggerCtx = BucketLevelTriggerExecutionContext(monitor, trigger as BucketLevelTrigger, monitorResult)
                triggerContexts[trigger.id] = triggerCtx
                val triggerResult = monitorCtx.triggerService!!.runBucketLevelTrigger(monitor, trigger, triggerCtx)
                triggerResults[trigger.id] = triggerResult.getCombinedTriggerRunResult(triggerResults[trigger.id])

                /*
                 * If an error was encountered when running the trigger, it means that something went wrong when parsing the input results
                 * for the filtered buckets returned from the pipeline bucket selector injected into the input query.
                 *
                 * In this case, the returned aggregation result buckets are empty so the categorization of the Alerts that happens below
                 * should be skipped/invalidated since comparing the current Alerts to an empty result will lead the execution to believe
                 * that all Alerts have been COMPLETED. Not doing so would mean it would not be possible to propagate the error into the
                 * existing Alerts in a way the user can easily view them since they will have all been moved to the history index.
                 */
                if (triggerResults[trigger.id]?.error != null) continue
                val findings =
                    if (monitor.triggers.size == 1 && monitor.dataSources.findingsEnabled == true) {
                        logger.debug("Creating bucket level findings")
                        createFindings(
                            triggerResult,
                            monitor,
                            monitorCtx,
                            periodStart,
                            periodEnd,
                            !dryrun && monitor.id != Monitor.NO_ID,
                            workflowRunContext
                        )
                    } else {
                        emptyList()
                    }
                // TODO: Should triggerResult's aggregationResultBucket be a list? If not, getCategorizedAlertsForBucketLevelMonitor can
                //  be refactored to use a map instead
                val categorizedAlerts = monitorCtx.alertService!!.getCategorizedAlertsForBucketLevelMonitor(
                    monitor,
                    trigger,
                    currentAlertsForTrigger,
                    triggerResult.aggregationResultBuckets.values.toList(),
                    findings
                ).toMutableMap()
                val dedupedAlerts = categorizedAlerts.getOrDefault(AlertCategory.DEDUPED, emptyList())
                var newAlerts = categorizedAlerts.getOrDefault(AlertCategory.NEW, emptyList())

                /*
                 * Index de-duped and new Alerts here (if it's not a test Monitor) so they are available at the time the Actions are executed.
                 *
                 * The new Alerts have to be returned and saved back with their indexed doc ID to prevent duplicate documents
                 * when the Alerts are updated again after Action execution.
                 *
                 * Note: Index operations can fail for various reasons (such as write blocks on cluster), in such a case, the Actions
                 * will still execute with the Alert information in the ctx but the Alerts may not be visible.
                 */
                if (!dryrun && monitor.id != Monitor.NO_ID) {
                    monitorCtx.alertService!!.saveAlerts(
                        monitor.dataSources,
                        dedupedAlerts,
                        monitorCtx.retryPolicy!!,
                        allowUpdatingAcknowledgedAlert = true
                    )
                    newAlerts = monitorCtx.alertService!!.saveNewAlerts(monitor.dataSources, newAlerts, monitorCtx.retryPolicy!!)
                }

                // Store deduped and new Alerts to accumulate across pages
                if (!nextAlerts.containsKey(trigger.id)) {
                    nextAlerts[trigger.id] = mutableMapOf(
                        AlertCategory.DEDUPED to mutableListOf(),
                        AlertCategory.NEW to mutableListOf(),
                        AlertCategory.COMPLETED to mutableListOf()
                    )
                }
                nextAlerts[trigger.id]?.get(AlertCategory.DEDUPED)?.addAll(dedupedAlerts)
                nextAlerts[trigger.id]?.get(AlertCategory.NEW)?.addAll(newAlerts)
            }
        } while (monitorResult.inputResults.afterKeysPresent())

        // The completed Alerts are whatever are left in the currentAlerts.
        // However, this operation will only be done if there was no trigger error, since otherwise the nextAlerts were not collected
        // in favor of just using the currentAlerts as-is.
        currentAlerts.forEach { (trigger, keysToAlertsMap) ->
            if (triggerResults[trigger.id]?.error == null) {
                nextAlerts[trigger.id]?.get(AlertCategory.COMPLETED)
                    ?.addAll(monitorCtx.alertService!!.convertToCompletedAlerts(keysToAlertsMap))
            }
        }

        for (trigger in monitor.triggers) {
            val alertsToUpdate = mutableSetOf<Alert>()
            val completedAlertsToUpdate = mutableSetOf<Alert>()
            // Filter ACKNOWLEDGED Alerts from the deduped list so they do not have Actions executed for them.
            // New Alerts are ignored since they cannot be acknowledged yet.
            val dedupedAlerts = nextAlerts[trigger.id]?.get(AlertCategory.DEDUPED)
                ?.filterNot { it.state == Alert.State.ACKNOWLEDGED }?.toMutableList()
                ?: mutableListOf()
            // Update nextAlerts so the filtered DEDUPED Alerts are reflected for PER_ALERT Action execution
            nextAlerts[trigger.id]?.set(AlertCategory.DEDUPED, dedupedAlerts)
            val newAlerts = nextAlerts[trigger.id]?.get(AlertCategory.NEW) ?: mutableListOf()
            val completedAlerts = nextAlerts[trigger.id]?.get(AlertCategory.COMPLETED) ?: mutableListOf()

            // Adding all the COMPLETED Alerts to a separate set and removing them if they get added
            // to alertsToUpdate to ensure the Alert doc is updated at the end in either case
            completedAlertsToUpdate.addAll(completedAlerts)

            // All trigger contexts and results should be available at this point since all triggers were evaluated in the main do-while loop
            val triggerCtx = triggerContexts[trigger.id]!!
            val triggerResult = triggerResults[trigger.id]!!
            val monitorOrTriggerError = monitorResult.error ?: triggerResult.error
            val shouldDefaultToPerExecution = defaultToPerExecutionAction(
                monitorCtx.maxActionableAlertCount,
                monitorId = monitor.id,
                triggerId = trigger.id,
                totalActionableAlertCount = dedupedAlerts.size + newAlerts.size + completedAlerts.size,
                monitorOrTriggerError = monitorOrTriggerError
            )
            for (action in trigger.actions) {
                // ActionExecutionPolicy should not be null for Bucket-Level Monitors since it has a default config when not set explicitly
                val actionExecutionScope = action.getActionExecutionPolicy(monitor)!!.actionExecutionScope
                if (actionExecutionScope is PerAlertActionScope && !shouldDefaultToPerExecution) {
                    for (alertCategory in actionExecutionScope.actionableAlerts) {
                        val alertsToExecuteActionsFor = nextAlerts[trigger.id]?.get(alertCategory) ?: mutableListOf()
                        for (alert in alertsToExecuteActionsFor) {
                            val actionCtx = getActionContextForAlertCategory(
                                alertCategory,
                                alert,
                                triggerCtx,
                                monitorOrTriggerError
                            )
                            // AggregationResultBucket should not be null here
                            val alertBucketKeysHash = alert.aggregationResultBucket!!.getBucketKeysHash()
                            if (!triggerResult.actionResultsMap.containsKey(alertBucketKeysHash)) {
                                triggerResult.actionResultsMap[alertBucketKeysHash] = mutableMapOf()
                            }

                            // Keeping the throttled response separate from runAction for now since
                            // throttling is not supported for PER_EXECUTION
                            val actionResult = if (MonitorRunnerService.isActionActionable(action, alert)) {
                                this.runAction(action, actionCtx, monitorCtx, monitor, dryrun)
                            } else {
                                ActionRunResult(action.id, action.name, mapOf(), true, null, null)
                            }

                            triggerResult.actionResultsMap[alertBucketKeysHash]?.set(action.id, actionResult)
                            alertsToUpdate.add(alert)
                            // Remove the alert from completedAlertsToUpdate in case it is present there since
                            // its update will be handled in the alertsToUpdate batch
                            completedAlertsToUpdate.remove(alert)
                        }
                    }
                } else if (actionExecutionScope is PerExecutionActionScope || shouldDefaultToPerExecution) {
                    // If all categories of Alerts are empty, there is nothing to message on and we can skip the Action.
                    // If the error is not null, this is disregarded and the Action is executed anyway so the user can be notified.
                    if (monitorOrTriggerError == null && dedupedAlerts.isEmpty() && newAlerts.isEmpty() && completedAlerts.isEmpty()) {
                        continue
                    }

                    val actionCtx = triggerCtx.copy(
                        dedupedAlerts = dedupedAlerts,
                        newAlerts = newAlerts,
                        completedAlerts = completedAlerts,
                        error = monitorResult.error ?: triggerResult.error
                    )
                    val actionResult = this.runAction(action, actionCtx, monitorCtx, monitor, dryrun)
                    // If there was an error during trigger execution then the Alerts to be updated are the current Alerts since the state
                    // was not changed. Otherwise, the Alerts to be updated are the sum of the deduped, new and completed Alerts.
                    val alertsToIterate = if (monitorOrTriggerError == null) {
                        (dedupedAlerts + newAlerts + completedAlerts)
                    } else currentAlerts[trigger]?.map { it.value } ?: listOf()
                    // Save the Action run result for every Alert
                    for (alert in alertsToIterate) {
                        val alertBucketKeysHash = alert.aggregationResultBucket!!.getBucketKeysHash()
                        if (!triggerResult.actionResultsMap.containsKey(alertBucketKeysHash)) {
                            triggerResult.actionResultsMap[alertBucketKeysHash] = mutableMapOf()
                        }
                        triggerResult.actionResultsMap[alertBucketKeysHash]?.set(action.id, actionResult)
                        alertsToUpdate.add(alert)
                        // Remove the alert from completedAlertsToUpdate in case it is present there since
                        // its update will be handled in the alertsToUpdate batch
                        completedAlertsToUpdate.remove(alert)
                    }
                }
            }

            // Alerts are only added to alertsToUpdate after Action execution meaning the action results for it should be present
            // in the actionResultsMap but returning a default value when accessing the map to be safe.
            val updatedAlerts = alertsToUpdate.map { alert ->
                val bucketKeysHash = alert.aggregationResultBucket!!.getBucketKeysHash()
                val actionResults = triggerResult.actionResultsMap.getOrDefault(bucketKeysHash, emptyMap<String, ActionRunResult>())
                monitorCtx.alertService!!.updateActionResultsForBucketLevelAlert(
                    alert.copy(lastNotificationTime = MonitorRunnerService.currentTime()),
                    actionResults,
                    // TODO: Update BucketLevelTriggerRunResult.alertError() to retrieve error based on the first failed Action
                    monitorResult.alertError() ?: triggerResult.alertError()
                )
            }

            // Update Alerts with action execution results (if it's not a test Monitor).
            // ACKNOWLEDGED Alerts should not be saved here since actions are not executed for them.
            if (!dryrun && monitor.id != Monitor.NO_ID) {
                monitorCtx.alertService!!.saveAlerts(
                    monitor.dataSources,
                    updatedAlerts,
                    monitorCtx.retryPolicy!!,
                    allowUpdatingAcknowledgedAlert = false
                )
                // Save any COMPLETED Alerts that were not covered in updatedAlerts
                monitorCtx.alertService!!.saveAlerts(
                    monitor.dataSources,
                    completedAlertsToUpdate.toList(),
                    monitorCtx.retryPolicy!!,
                    allowUpdatingAcknowledgedAlert = false
                )
            }
        }

        return monitorResult.copy(inputResults = firstPageOfInputResults, triggerResults = triggerResults)
    }

    private suspend fun createFindings(
        triggerResult: BucketLevelTriggerRunResult,
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        shouldCreateFinding: Boolean,
        workflowRunContext: WorkflowRunContext? = null
    ): List<String> {
        monitor.inputs.forEach { input ->
            if (input is SearchInput) {
                val bucketValues: Set<String> = triggerResult.aggregationResultBuckets.keys
                val query = input.query
                var fieldName = ""

                for (aggFactory in (query.aggregations() as AggregatorFactories.Builder).aggregatorFactories) {
                    when (aggFactory) {
                        is CompositeAggregationBuilder -> {
                            var groupByFields = 0 // if number of fields used to group by > 1 we won't calculate findings
                            val sources = aggFactory.sources()
                            for (source in sources) {
                                if (groupByFields > 0) {
                                    logger.error("grouByFields > 0. not generating findings for bucket level monitor ${monitor.id}")
                                    return listOf()
                                }
                                groupByFields++
                                fieldName = source.field()
                            }
                        }
                        is TermsAggregationBuilder -> {
                            fieldName = aggFactory.field()
                        }
                        else -> {
                            logger.error(
                                "Bucket level monitor findings supported only for composite and term aggs. Found [{${aggFactory.type}}]"
                            )
                            return listOf()
                        }
                    }
                }
                if (fieldName != "") {
                    val searchParams = mapOf(
                        "period_start" to periodStart.toEpochMilli(),
                        "period_end" to periodEnd.toEpochMilli()
                    )
                    val searchSource = monitorCtx.scriptService!!.compile(
                        Script(
                            ScriptType.INLINE,
                            Script.DEFAULT_TEMPLATE_LANG,
                            query.toString(),
                            searchParams
                        ),
                        TemplateScript.CONTEXT
                    )
                        .newInstance(searchParams)
                        .execute()
                    val sr = SearchRequest(*input.indices.toTypedArray())
                    XContentType.JSON.xContent().createParser(monitorCtx.xContentRegistry, LoggingDeprecationHandler.INSTANCE, searchSource)
                        .use {
                            val source = SearchSourceBuilder.fromXContent(it)
                            val queryBuilder = if (input.query.query() == null) BoolQueryBuilder()
                            else QueryBuilders.boolQuery().must(source.query())
                            queryBuilder.filter(QueryBuilders.termsQuery(fieldName, bucketValues))
                            sr.source().query(queryBuilder)
                        }
                    val searchResponse: SearchResponse = monitorCtx.client!!.suspendUntil { monitorCtx.client!!.search(sr, it) }
                    return createFindingPerIndex(searchResponse, monitor, monitorCtx, shouldCreateFinding, workflowRunContext?.executionId)
                } else {
                    logger.error("Couldn't resolve groupBy field. Not generating bucket level monitor findings for monitor %${monitor.id}")
                }
            }
        }
        return listOf()
    }

    private suspend fun createFindingPerIndex(
        searchResponse: SearchResponse,
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        shouldCreateFinding: Boolean,
        workflowExecutionId: String? = null
    ): List<String> {
        val docIdsByIndexName: MutableMap<String, MutableList<String>> = mutableMapOf()
        for (hit in searchResponse.hits.hits) {
            val ids = docIdsByIndexName.getOrDefault(hit.index, mutableListOf())
            ids.add(hit.id)
            docIdsByIndexName[hit.index] = ids
        }
        val findings = mutableListOf<String>()
        var requestsToRetry: MutableList<IndexRequest> = mutableListOf()
        docIdsByIndexName.entries.forEach { it ->
            run {
                val finding = Finding(
                    id = UUID.randomUUID().toString(),
                    relatedDocIds = it.value,
                    monitorId = monitor.id,
                    monitorName = monitor.name,
                    index = it.key,
                    timestamp = Instant.now(),
                    docLevelQueries = listOf(),
                    executionId = workflowExecutionId
                )

                val findingStr = finding.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS).string()
                logger.debug("Bucket level monitor ${monitor.id} Findings: $findingStr")
                if (shouldCreateFinding) {
                    logger.debug("Saving bucket level monitor findings for monitor ${monitor.id}")
                    val indexRequest = IndexRequest(monitor.dataSources.findingsIndex)
                        .source(findingStr, XContentType.JSON)
                        .id(finding.id)
                        .routing(finding.id)
                    requestsToRetry.add(indexRequest)
                }
                findings.add(finding.id)
            }
        }
        if (requestsToRetry.isEmpty()) return listOf()
        monitorCtx.retryPolicy!!.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
            val bulkRequest = BulkRequest().add(requestsToRetry).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            val bulkResponse: BulkResponse = monitorCtx.client!!.suspendUntil { monitorCtx.client!!.bulk(bulkRequest, it) }
            requestsToRetry = mutableListOf()
            val findingsBeingRetried = mutableListOf<Alert>()
            bulkResponse.items.forEach { item ->
                if (item.isFailed) {
                    if (item.status() == RestStatus.TOO_MANY_REQUESTS) {
                        requestsToRetry.add(bulkRequest.requests()[item.itemId] as IndexRequest)
                        findingsBeingRetried.add(findingsBeingRetried[item.itemId])
                    }
                }
            }
        }
        return findings
    }

    private fun getActionContextForAlertCategory(
        alertCategory: AlertCategory,
        alert: Alert,
        ctx: BucketLevelTriggerExecutionContext,
        error: Exception?
    ): BucketLevelTriggerExecutionContext {
        return when (alertCategory) {
            AlertCategory.DEDUPED ->
                ctx.copy(dedupedAlerts = listOf(alert), newAlerts = emptyList(), completedAlerts = emptyList(), error = error)
            AlertCategory.NEW ->
                ctx.copy(dedupedAlerts = emptyList(), newAlerts = listOf(alert), completedAlerts = emptyList(), error = error)
            AlertCategory.COMPLETED ->
                ctx.copy(dedupedAlerts = emptyList(), newAlerts = emptyList(), completedAlerts = listOf(alert), error = error)
        }
    }
}
