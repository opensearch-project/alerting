/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.alerts.moveAlerts
import org.opensearch.alerting.core.JobRunner
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.elasticapi.InjectorContextElement
import org.opensearch.alerting.elasticapi.retry
import org.opensearch.alerting.elasticapi.withClosableContext
import org.opensearch.alerting.model.ActionRunResult
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.AlertingConfigAccessor
import org.opensearch.alerting.model.BucketLevelTrigger
import org.opensearch.alerting.model.BucketLevelTriggerRunResult
import org.opensearch.alerting.model.InputRunResults
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.QueryLevelTrigger
import org.opensearch.alerting.model.QueryLevelTriggerRunResult
import org.opensearch.alerting.model.action.Action
import org.opensearch.alerting.model.action.Action.Companion.MESSAGE
import org.opensearch.alerting.model.action.Action.Companion.MESSAGE_ID
import org.opensearch.alerting.model.action.Action.Companion.SUBJECT
import org.opensearch.alerting.model.action.ActionExecutionScope
import org.opensearch.alerting.model.action.AlertCategory
import org.opensearch.alerting.model.action.PerAlertActionScope
import org.opensearch.alerting.model.action.PerExecutionActionScope
import org.opensearch.alerting.model.destination.DestinationContextFactory
import org.opensearch.alerting.script.BucketLevelTriggerExecutionContext
import org.opensearch.alerting.script.QueryLevelTriggerExecutionContext
import org.opensearch.alerting.script.TriggerExecutionContext
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_BACKOFF_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_BACKOFF_MILLIS
import org.opensearch.alerting.settings.AlertingSettings.Companion.DEFAULT_MAX_ACTIONABLE_ALERT_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MAX_ACTIONABLE_ALERT_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MOVE_ALERTS_BACKOFF_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MOVE_ALERTS_BACKOFF_MILLIS
import org.opensearch.alerting.settings.DestinationSettings
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST_NONE
import org.opensearch.alerting.settings.DestinationSettings.Companion.HOST_DENY_LIST
import org.opensearch.alerting.settings.DestinationSettings.Companion.loadDestinationSettings
import org.opensearch.alerting.settings.LegacyOpenDistroDestinationSettings.Companion.HOST_DENY_LIST_NONE
import org.opensearch.alerting.util.getActionExecutionPolicy
import org.opensearch.alerting.util.getBucketKeysHash
import org.opensearch.alerting.util.getCombinedTriggerRunResult
import org.opensearch.alerting.util.isADMonitor
import org.opensearch.alerting.util.isAllowed
import org.opensearch.alerting.util.isBucketLevelMonitor
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.Strings
import org.opensearch.common.component.AbstractLifecycleComponent
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript
import org.opensearch.threadpool.ThreadPool
import java.time.Instant
import kotlin.coroutines.CoroutineContext

object MonitorRunner : JobRunner, CoroutineScope, AbstractLifecycleComponent() {

    private val logger = LogManager.getLogger(javaClass)

    private lateinit var clusterService: ClusterService
    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var scriptService: ScriptService
    private lateinit var settings: Settings
    private lateinit var threadPool: ThreadPool
    private lateinit var alertIndices: AlertIndices
    private lateinit var inputService: InputService
    private lateinit var triggerService: TriggerService
    private lateinit var alertService: AlertService

    @Volatile private lateinit var retryPolicy: BackoffPolicy
    @Volatile private lateinit var moveAlertsRetryPolicy: BackoffPolicy

    @Volatile private var allowList = ALLOW_LIST_NONE
    @Volatile private var hostDenyList = HOST_DENY_LIST_NONE

    @Volatile private lateinit var destinationSettings: Map<String, DestinationSettings.Companion.SecureDestinationSettings>
    @Volatile private lateinit var destinationContextFactory: DestinationContextFactory

    @Volatile private var maxActionableAlertCount = DEFAULT_MAX_ACTIONABLE_ALERT_COUNT

    private lateinit var runnerSupervisor: Job
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + runnerSupervisor

    fun registerClusterService(clusterService: ClusterService): MonitorRunner {
        this.clusterService = clusterService
        return this
    }

    fun registerClient(client: Client): MonitorRunner {
        this.client = client
        return this
    }

    fun registerNamedXContentRegistry(xContentRegistry: NamedXContentRegistry): MonitorRunner {
        this.xContentRegistry = xContentRegistry
        return this
    }

    fun registerScriptService(scriptService: ScriptService): MonitorRunner {
        this.scriptService = scriptService
        return this
    }

    fun registerSettings(settings: Settings): MonitorRunner {
        this.settings = settings
        return this
    }

    fun registerThreadPool(threadPool: ThreadPool): MonitorRunner {
        this.threadPool = threadPool
        return this
    }

    fun registerAlertIndices(alertIndices: AlertIndices): MonitorRunner {
        this.alertIndices = alertIndices
        return this
    }

    fun registerInputService(inputService: InputService): MonitorRunner {
        this.inputService = inputService
        return this
    }

    fun registerTriggerService(triggerService: TriggerService): MonitorRunner {
        this.triggerService = triggerService
        return this
    }

    fun registerAlertService(alertService: AlertService): MonitorRunner {
        this.alertService = alertService
        return this
    }

    // Must be called after registerClusterService and registerSettings in AlertingPlugin
    fun registerConsumers(): MonitorRunner {
        retryPolicy = BackoffPolicy.constantBackoff(ALERT_BACKOFF_MILLIS.get(settings), ALERT_BACKOFF_COUNT.get(settings))
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_BACKOFF_MILLIS, ALERT_BACKOFF_COUNT) { millis, count ->
            retryPolicy = BackoffPolicy.constantBackoff(millis, count)
        }

        moveAlertsRetryPolicy =
            BackoffPolicy.exponentialBackoff(MOVE_ALERTS_BACKOFF_MILLIS.get(settings), MOVE_ALERTS_BACKOFF_COUNT.get(settings))
        clusterService.clusterSettings.addSettingsUpdateConsumer(MOVE_ALERTS_BACKOFF_MILLIS, MOVE_ALERTS_BACKOFF_COUNT) { millis, count ->
            moveAlertsRetryPolicy = BackoffPolicy.exponentialBackoff(millis, count)
        }

        allowList = ALLOW_LIST.get(settings)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) {
            allowList = it
        }

        // Host deny list is not a dynamic setting so no consumer is registered but the variable is set here
        hostDenyList = HOST_DENY_LIST.get(settings)

        maxActionableAlertCount = MAX_ACTIONABLE_ALERT_COUNT.get(settings)
        clusterService.clusterSettings.addSettingsUpdateConsumer(MAX_ACTIONABLE_ALERT_COUNT) {
            maxActionableAlertCount = it
        }

        return this
    }

    // To be safe, call this last as it depends on a number of other components being registered beforehand (client, settings, etc.)
    fun registerDestinationSettings(): MonitorRunner {
        destinationSettings = loadDestinationSettings(settings)
        destinationContextFactory = DestinationContextFactory(client, xContentRegistry, destinationSettings)
        return this
    }

    // Updates destination settings when the reload API is called so that new keystore values are visible
    fun reloadDestinationSettings(settings: Settings) {
        destinationSettings = loadDestinationSettings(settings)

        // Update destinationContextFactory as well since destinationSettings has been updated
        destinationContextFactory.updateDestinationSettings(destinationSettings)
    }

    override fun doStart() {
        runnerSupervisor = SupervisorJob()
    }

    override fun doStop() {
        runnerSupervisor.cancel()
    }

    override fun doClose() { }

    override fun postIndex(job: ScheduledJob) {
        if (job !is Monitor) {
            throw IllegalArgumentException("Invalid job type")
        }

        launch {
            try {
                moveAlertsRetryPolicy.retry(logger) {
                    if (alertIndices.isInitialized()) {
                        moveAlerts(client, job.id, job)
                    }
                }
            } catch (e: Exception) {
                logger.error("Failed to move active alerts for monitor [${job.id}].", e)
            }
        }
    }

    override fun postDelete(jobId: String) {
        launch {
            try {
                moveAlertsRetryPolicy.retry(logger) {
                    if (alertIndices.isInitialized()) {
                        moveAlerts(client, jobId, null)
                    }
                }
            } catch (e: Exception) {
                logger.error("Failed to move active alerts for monitor [$jobId].", e)
            }
        }
    }

    override fun runJob(job: ScheduledJob, periodStart: Instant, periodEnd: Instant) {
        if (job !is Monitor) {
            throw IllegalArgumentException("Invalid job type")
        }

        launch {
            if (job.isBucketLevelMonitor()) {
                runBucketLevelMonitor(job, periodStart, periodEnd)
            } else {
                runQueryLevelMonitor(job, periodStart, periodEnd)
            }
        }
    }

    suspend fun runQueryLevelMonitor(monitor: Monitor, periodStart: Instant, periodEnd: Instant, dryrun: Boolean = false):
        MonitorRunResult<QueryLevelTriggerRunResult> {
        val roles = getRolesForMonitor(monitor)
        logger.debug("Running monitor: ${monitor.name} with roles: $roles Thread: ${Thread.currentThread().name}")

        if (periodStart == periodEnd) {
            logger.warn("Start and end time are the same: $periodStart. This monitor will probably only run once.")
        }

        var monitorResult = MonitorRunResult<QueryLevelTriggerRunResult>(monitor.name, periodStart, periodEnd)
        val currentAlerts = try {
            alertIndices.createOrUpdateAlertIndex()
            alertIndices.createOrUpdateInitialHistoryIndex()
            alertService.loadCurrentAlertsForQueryLevelMonitor(monitor)
        } catch (e: Exception) {
            // We can't save ERROR alerts to the index here as we don't know if there are existing ACTIVE alerts
            val id = if (monitor.id.trim().isEmpty()) "_na_" else monitor.id
            logger.error("Error loading alerts for monitor: $id", e)
            return monitorResult.copy(error = e)
        }
        if (!isADMonitor(monitor)) {
            withClosableContext(InjectorContextElement(monitor.id, settings, threadPool.threadContext, roles)) {
                monitorResult = monitorResult.copy(inputResults = inputService.collectInputResults(monitor, periodStart, periodEnd))
            }
        } else {
            monitorResult = monitorResult.copy(inputResults = inputService.collectInputResultsForADMonitor(monitor, periodStart, periodEnd))
        }

        val updatedAlerts = mutableListOf<Alert>()
        val triggerResults = mutableMapOf<String, QueryLevelTriggerRunResult>()
        for (trigger in monitor.triggers) {
            val currentAlert = currentAlerts[trigger]
            val triggerCtx = QueryLevelTriggerExecutionContext(monitor, trigger as QueryLevelTrigger, monitorResult, currentAlert)
            val triggerResult = triggerService.runQueryLevelTrigger(monitor, trigger, triggerCtx)
            triggerResults[trigger.id] = triggerResult

            if (triggerService.isQueryLevelTriggerActionable(triggerCtx, triggerResult)) {
                val actionCtx = triggerCtx.copy(error = monitorResult.error ?: triggerResult.error)
                for (action in trigger.actions) {
                    triggerResult.actionResults[action.id] = runAction(action, actionCtx, dryrun)
                }
            }

            val updatedAlert = alertService.composeQueryLevelAlert(
                triggerCtx, triggerResult,
                monitorResult.alertError() ?: triggerResult.alertError()
            )
            if (updatedAlert != null) updatedAlerts += updatedAlert
        }

        // Don't save alerts if this is a test monitor
        if (!dryrun && monitor.id != Monitor.NO_ID) {
            alertService.saveAlerts(updatedAlerts, retryPolicy)
        }
        return monitorResult.copy(triggerResults = triggerResults)
    }

    // TODO: This method has grown very large with all the business logic that has been added.
    //  Revisit this during refactoring and break it down to be more manageable.
    suspend fun runBucketLevelMonitor(
        monitor: Monitor,
        periodStart: Instant,
        periodEnd: Instant,
        dryrun: Boolean = false
    ): MonitorRunResult<BucketLevelTriggerRunResult> {
        val roles = getRolesForMonitor(monitor)
        logger.debug("Running monitor: ${monitor.name} with roles: $roles Thread: ${Thread.currentThread().name}")

        if (periodStart == periodEnd) {
            logger.warn("Start and end time are the same: $periodStart. This monitor will probably only run once.")
        }

        var monitorResult = MonitorRunResult<BucketLevelTriggerRunResult>(monitor.name, periodStart, periodEnd)
        val currentAlerts = try {
            alertIndices.createOrUpdateAlertIndex()
            alertIndices.createOrUpdateInitialHistoryIndex()
            alertService.loadCurrentAlertsForBucketLevelMonitor(monitor)
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
            withClosableContext(InjectorContextElement(monitor.id, settings, threadPool.threadContext, roles)) {
                // Storing the first page of results in the case of pagination input results to prevent empty results
                // in the final output of monitorResult which occurs when all pages have been exhausted.
                // If it's favorable to return the last page, will need to check how to accomplish that with multiple aggregation paths
                // with different page counts.
                val inputResults = inputService.collectInputResults(monitor, periodStart, periodEnd, monitorResult.inputResults)
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
                val triggerResult = triggerService.runBucketLevelTrigger(monitor, trigger, triggerCtx)
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
                val categorizedAlerts = alertService.getCategorizedAlertsForBucketLevelMonitor(
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
                    alertService.saveAlerts(dedupedAlerts, retryPolicy, allowUpdatingAcknowledgedAlert = true)
                    newAlerts = alertService.saveNewAlerts(newAlerts, retryPolicy)
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
                nextAlerts[trigger.id]?.get(AlertCategory.COMPLETED)?.addAll(alertService.convertToCompletedAlerts(keysToAlertsMap))
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
                            val actionResult = if (isActionActionable(action, alert)) {
                                runAction(action, actionCtx, dryrun)
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
                    val actionResult = runAction(action, actionCtx, dryrun)
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
                alertService.updateActionResultsForBucketLevelAlert(
                    alert.copy(lastNotificationTime = currentTime()),
                    actionResults,
                    // TODO: Update BucketLevelTriggerRunResult.alertError() to retrieve error based on the first failed Action
                    monitorResult.alertError() ?: triggerResult.alertError()
                )
            }

            // Update Alerts with action execution results (if it's not a test Monitor).
            // ACKNOWLEDGED Alerts should not be saved here since actions are not executed for them.
            if (!dryrun && monitor.id != Monitor.NO_ID) {
                alertService.saveAlerts(updatedAlerts, retryPolicy, allowUpdatingAcknowledgedAlert = false)
                // Save any COMPLETED Alerts that were not covered in updatedAlerts
                alertService.saveAlerts(completedAlertsToUpdate.toList(), retryPolicy, allowUpdatingAcknowledgedAlert = false)
            }
        }

        return monitorResult.copy(inputResults = firstPageOfInputResults, triggerResults = triggerResults)
    }

    private fun defaultToPerExecutionAction(
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
        if (maxActionableAlertCount < 0) return false

        // If the total number of Alerts to execute Actions on exceeds the MAX_ACTIONABLE_ALERT_COUNT setting then default to
        // PER_EXECUTION for less intrusive Actions
        if (totalActionableAlertCount > maxActionableAlertCount) {
            logger.debug(
                "The total actionable alerts for trigger [$triggerId] in monitor [$monitorId] is [$totalActionableAlertCount] " +
                    "which exceeds the maximum of [$maxActionableAlertCount]. Defaulting to [${ActionExecutionScope.Type.PER_EXECUTION}] " +
                    "for action execution."
            )
            return true
        }

        return false
    }

    private fun getRolesForMonitor(monitor: Monitor): List<String> {
        /*
         * We need to handle 3 cases:
         * 1. Monitors created by older versions and never updated. These monitors wont have User details in the
         * monitor object. `monitor.user` will be null. Insert `all_access, AmazonES_all_access` role.
         * 2. Monitors are created when security plugin is disabled, these will have empty User object.
         * (`monitor.user.name`, `monitor.user.roles` are empty )
         * 3. Monitors are created when security plugin is enabled, these will have an User object.
         */
        return if (monitor.user == null) {
            // fixme: discuss and remove hardcoded to settings?
            // TODO: Remove "AmazonES_all_access" role?
            settings.getAsList("", listOf("all_access", "AmazonES_all_access"))
        } else {
            monitor.user.roles
        }
    }

    // TODO: Can this be updated to just use 'Instant.now()'?
    //  'threadPool.absoluteTimeInMillis()' is referring to a cached value of System.currentTimeMillis() that by default updates every 200ms
    private fun currentTime() = Instant.ofEpochMilli(threadPool.absoluteTimeInMillis())

    private fun isActionActionable(action: Action, alert: Alert?): Boolean {
        if (alert == null || action.throttle == null) {
            return true
        }
        if (action.throttleEnabled) {
            val result = alert.actionExecutionResults.firstOrNull { r -> r.actionId == action.id }
            val lastExecutionTime: Instant? = result?.lastExecutionTime
            val throttledTimeBound = currentTime().minus(action.throttle.value.toLong(), action.throttle.unit)
            return (lastExecutionTime == null || lastExecutionTime.isBefore(throttledTimeBound))
        }
        return true
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

    private suspend fun runAction(action: Action, ctx: QueryLevelTriggerExecutionContext, dryrun: Boolean): ActionRunResult {
        return try {
            if (!isActionActionable(action, ctx.alert)) {
                return ActionRunResult(action.id, action.name, mapOf(), true, null, null)
            }
            val actionOutput = mutableMapOf<String, String>()
            actionOutput[SUBJECT] = if (action.subjectTemplate != null) compileTemplate(action.subjectTemplate, ctx) else ""
            actionOutput[MESSAGE] = compileTemplate(action.messageTemplate, ctx)
            if (Strings.isNullOrEmpty(actionOutput[MESSAGE])) {
                throw IllegalStateException("Message content missing in the Destination with id: ${action.destinationId}")
            }
            if (!dryrun) {
                withContext(Dispatchers.IO) {
                    val destination = AlertingConfigAccessor.getDestinationInfo(client, xContentRegistry, action.destinationId)
                    if (!destination.isAllowed(allowList)) {
                        throw IllegalStateException("Monitor contains a Destination type that is not allowed: ${destination.type}")
                    }

                    val destinationCtx = destinationContextFactory.getDestinationContext(destination)
                    actionOutput[MESSAGE_ID] = destination.publish(
                        actionOutput[SUBJECT],
                        actionOutput[MESSAGE]!!,
                        destinationCtx,
                        hostDenyList
                    )
                }
            }
            ActionRunResult(action.id, action.name, actionOutput, false, currentTime(), null)
        } catch (e: Exception) {
            ActionRunResult(action.id, action.name, mapOf(), false, currentTime(), e)
        }
    }

    // TODO: This is largely a duplicate of runAction above for BucketLevelTriggerExecutionContext for now.
    //   After suppression logic implementation, if this remains mostly the same, it can be refactored.
    private suspend fun runAction(action: Action, ctx: BucketLevelTriggerExecutionContext, dryrun: Boolean): ActionRunResult {
        return try {
            val actionOutput = mutableMapOf<String, String>()
            actionOutput[SUBJECT] = if (action.subjectTemplate != null) compileTemplate(action.subjectTemplate, ctx) else ""
            actionOutput[MESSAGE] = compileTemplate(action.messageTemplate, ctx)
            if (Strings.isNullOrEmpty(actionOutput[MESSAGE])) {
                throw IllegalStateException("Message content missing in the Destination with id: ${action.destinationId}")
            }
            if (!dryrun) {
                withContext(Dispatchers.IO) {
                    val destination = AlertingConfigAccessor.getDestinationInfo(client, xContentRegistry, action.destinationId)
                    if (!destination.isAllowed(allowList)) {
                        throw IllegalStateException("Monitor contains a Destination type that is not allowed: ${destination.type}")
                    }

                    val destinationCtx = destinationContextFactory.getDestinationContext(destination)
                    actionOutput[MESSAGE_ID] = destination.publish(
                        actionOutput[SUBJECT],
                        actionOutput[MESSAGE]!!,
                        destinationCtx,
                        hostDenyList
                    )
                }
            }
            ActionRunResult(action.id, action.name, actionOutput, false, currentTime(), null)
        } catch (e: Exception) {
            ActionRunResult(action.id, action.name, mapOf(), false, currentTime(), e)
        }
    }

    private fun compileTemplate(template: Script, ctx: TriggerExecutionContext): String {
        return scriptService.compile(template, TemplateScript.CONTEXT)
            .newInstance(template.params + mapOf("ctx" to ctx.asTemplateArg()))
            .execute()
    }
}
