package org.opensearch.alerting

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.elasticapi.InjectorContextElement
import org.opensearch.alerting.model.ActionRunResult
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.AlertingConfigAccessor
import org.opensearch.alerting.model.BucketLevelTrigger
import org.opensearch.alerting.model.BucketLevelTriggerRunResult
import org.opensearch.alerting.model.InputRunResults
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.action.Action
import org.opensearch.alerting.model.action.ActionExecutionScope
import org.opensearch.alerting.model.action.AlertCategory
import org.opensearch.alerting.model.action.PerAlertActionScope
import org.opensearch.alerting.model.action.PerExecutionActionScope
import org.opensearch.alerting.script.BucketLevelTriggerExecutionContext
import org.opensearch.alerting.script.TriggerExecutionContext
import org.opensearch.alerting.util.getActionExecutionPolicy
import org.opensearch.alerting.util.getBucketKeysHash
import org.opensearch.alerting.util.getCombinedTriggerRunResult
import org.opensearch.alerting.util.isAllowed
import org.opensearch.common.Strings
import java.time.Instant

object BucketLevelMonitorRunner : MonitorRunner {
    private val logger = LogManager.getLogger(javaClass)

    override suspend fun runMonitor(
        monitor: Monitor,
        monitorCtx: MonitorRunnerExecutionContext,
        periodStart: Instant,
        periodEnd: Instant,
        dryrun: Boolean
    ): MonitorRunResult<BucketLevelTriggerRunResult> {
        val roles = MonitorRunnerService.getRolesForMonitor(monitor)
        logger.debug("Running monitor: ${monitor.name} with roles: $roles Thread: ${Thread.currentThread().name}")

        if (periodStart == periodEnd) {
            logger.warn("Start and end time are the same: $periodStart. This monitor will probably only run once.")
        }

        var monitorResult = MonitorRunResult<BucketLevelTriggerRunResult>(monitor.name, periodStart, periodEnd)
        val currentAlerts = try {
            monitorCtx.alertIndices!!.createOrUpdateAlertIndex()
            monitorCtx.alertIndices!!.createOrUpdateInitialAlertHistoryIndex()
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
            runBlocking(InjectorContextElement(monitor.id, monitorCtx.settings!!, monitorCtx.threadPool!!.threadContext, roles)) {
                // Storing the first page of results in the case of pagination input results to prevent empty results
                // in the final output of monitorResult which occurs when all pages have been exhausted.
                // If it's favorable to return the last page, will need to check how to accomplish that with multiple aggregation paths
                // with different page counts.
                val inputResults = monitorCtx.inputService!!.collectInputResults(
                    monitor,
                    periodStart,
                    periodEnd,
                    monitorResult.inputResults
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

                // TODO: Should triggerResult's aggregationResultBucket be a list? If not, getCategorizedAlertsForBucketLevelMonitor can
                //  be refactored to use a map instead
                val categorizedAlerts = monitorCtx.alertService!!.getCategorizedAlertsForBucketLevelMonitor(
                    monitor, trigger, currentAlertsForTrigger, triggerResult.aggregationResultBuckets.values.toList()
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
                    monitorCtx.alertService!!.saveAlerts(dedupedAlerts, monitorCtx.retryPolicy!!, allowUpdatingAcknowledgedAlert = true)
                    newAlerts = monitorCtx.alertService!!.saveNewAlerts(newAlerts, monitorCtx.retryPolicy!!)
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
            if (triggerResults[trigger.id]?.error == null)
                nextAlerts[trigger.id]?.get(AlertCategory.COMPLETED)
                    ?.addAll(monitorCtx.alertService!!.convertToCompletedAlerts(keysToAlertsMap))
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
                monitorCtx,
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
                                alertCategory, alert, triggerCtx, monitorOrTriggerError
                            )
                            // AggregationResultBucket should not be null here
                            val alertBucketKeysHash = alert.aggregationResultBucket!!.getBucketKeysHash()
                            if (!triggerResult.actionResultsMap.containsKey(alertBucketKeysHash)) {
                                triggerResult.actionResultsMap[alertBucketKeysHash] = mutableMapOf()
                            }

                            // Keeping the throttled response separate from runAction for now since
                            // throttling is not supported for PER_EXECUTION
                            val actionResult = if (MonitorRunnerService.isActionActionable(action, alert)) {
                                this.runAction(action, actionCtx, monitorCtx, dryrun)
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
                    if (monitorOrTriggerError == null && dedupedAlerts.isEmpty() && newAlerts.isEmpty() && completedAlerts.isEmpty())
                        continue

                    val actionCtx = triggerCtx.copy(
                        dedupedAlerts = dedupedAlerts,
                        newAlerts = newAlerts,
                        completedAlerts = completedAlerts,
                        error = monitorResult.error ?: triggerResult.error
                    )
                    val actionResult = this.runAction(action, actionCtx, monitorCtx, dryrun)
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
                monitorCtx.alertService!!.saveAlerts(updatedAlerts, monitorCtx.retryPolicy!!, allowUpdatingAcknowledgedAlert = false)
                // Save any COMPLETED Alerts that were not covered in updatedAlerts
                monitorCtx.alertService!!.saveAlerts(
                    completedAlertsToUpdate.toList(),
                    monitorCtx.retryPolicy!!,
                    allowUpdatingAcknowledgedAlert = false
                )
            }
        }

        return monitorResult.copy(inputResults = firstPageOfInputResults, triggerResults = triggerResults)
    }

    override suspend fun runAction(
        action: Action,
        ctx: TriggerExecutionContext,
        monitorCtx: MonitorRunnerExecutionContext,
        dryrun: Boolean
    ): ActionRunResult {
        return try {
            val actionOutput = mutableMapOf<String, String>()
            actionOutput[Action.SUBJECT] = if (action.subjectTemplate != null)
                MonitorRunnerService.compileTemplate(action.subjectTemplate, ctx)
            else ""
            actionOutput[Action.MESSAGE] = MonitorRunnerService.compileTemplate(action.messageTemplate, ctx)
            if (Strings.isNullOrEmpty(actionOutput[Action.MESSAGE])) {
                throw IllegalStateException("Message content missing in the Destination with id: ${action.destinationId}")
            }
            if (!dryrun) {
                withContext(Dispatchers.IO) {
                    val destination = AlertingConfigAccessor.getDestinationInfo(
                        monitorCtx.client!!,
                        monitorCtx.xContentRegistry!!,
                        action.destinationId
                    )
                    if (!destination.isAllowed(monitorCtx.allowList)) {
                        throw IllegalStateException("Monitor contains a Destination type that is not allowed: ${destination.type}")
                    }

                    val destinationCtx = monitorCtx.destinationContextFactory!!.getDestinationContext(destination)
                    actionOutput[Action.MESSAGE_ID] = destination.publish(
                        actionOutput[Action.SUBJECT],
                        actionOutput[Action.MESSAGE]!!,
                        destinationCtx,
                        monitorCtx.hostDenyList
                    )
                }
            }
            ActionRunResult(action.id, action.name, actionOutput, false, MonitorRunnerService.currentTime(), null)
        } catch (e: Exception) {
            ActionRunResult(action.id, action.name, mapOf(), false, MonitorRunnerService.currentTime(), e)
        }
    }

    private fun defaultToPerExecutionAction(
        monitorCtx: MonitorRunnerExecutionContext,
        monitorId: String,
        triggerId: String,
        totalActionableAlertCount: Int,
        monitorOrTriggerError: Exception?
    ): Boolean {
        // If the monitorId or triggerResult has an error, then also default to PER_EXECUTION to communicate the error
        if (monitorOrTriggerError != null) {
            logger.debug(
                "Trigger [$triggerId] in monitor [$monitorId] encountered an error. Defaulting to " +
                    "[${ActionExecutionScope.Type.PER_EXECUTION}] for action execution to communicate error."
            )
            return true
        }

        // If the MAX_ACTIONABLE_ALERT_COUNT is set to -1, consider it unbounded and proceed regardless of actionable Alert count
        if (monitorCtx.maxActionableAlertCount < 0) return false

        // If the total number of Alerts to execute Actions on exceeds the MAX_ACTIONABLE_ALERT_COUNT setting then default to
        // PER_EXECUTION for less intrusive Actions
        if (totalActionableAlertCount > monitorCtx.maxActionableAlertCount) {
            logger.debug(
                "The total actionable alerts for trigger [$triggerId] in monitor [$monitorId] is [$totalActionableAlertCount] " +
                    "which exceeds the maximum of [${monitorCtx.maxActionableAlertCount}]. " +
                    "Defaulting to [${ActionExecutionScope.Type.PER_EXECUTION}] for action execution."
            )
            return true
        }

        return false
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
