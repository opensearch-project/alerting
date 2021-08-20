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

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.alerting

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.alerts.AlertError
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.alerts.moveAlerts
import org.opensearch.alerting.core.JobRunner
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.elasticapi.InjectorContextElement
import org.opensearch.alerting.elasticapi.convertToMap
import org.opensearch.alerting.elasticapi.firstFailureOrNull
import org.opensearch.alerting.elasticapi.retry
import org.opensearch.alerting.elasticapi.suspendUntil
import org.opensearch.alerting.model.ActionExecutionResult
import org.opensearch.alerting.model.ActionRunResult
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.Alert.State.ACKNOWLEDGED
import org.opensearch.alerting.model.Alert.State.ACTIVE
import org.opensearch.alerting.model.Alert.State.COMPLETED
import org.opensearch.alerting.model.Alert.State.DELETED
import org.opensearch.alerting.model.Alert.State.ERROR
import org.opensearch.alerting.model.AlertingConfigAccessor
import org.opensearch.alerting.model.InputRunResults
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.Trigger
import org.opensearch.alerting.model.TriggerRunResult
import org.opensearch.alerting.model.action.Action
import org.opensearch.alerting.model.action.Action.Companion.MESSAGE
import org.opensearch.alerting.model.action.Action.Companion.MESSAGE_ID
import org.opensearch.alerting.model.action.Action.Companion.SUBJECT
import org.opensearch.alerting.model.destination.DestinationContextFactory
import org.opensearch.alerting.script.TriggerExecutionContext
import org.opensearch.alerting.script.TriggerScript
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_BACKOFF_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_BACKOFF_MILLIS
import org.opensearch.alerting.settings.AlertingSettings.Companion.MOVE_ALERTS_BACKOFF_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MOVE_ALERTS_BACKOFF_MILLIS
import org.opensearch.alerting.settings.DestinationSettings
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST_NONE
import org.opensearch.alerting.settings.DestinationSettings.Companion.HOST_DENY_LIST
import org.opensearch.alerting.settings.DestinationSettings.Companion.loadDestinationSettings
import org.opensearch.alerting.settings.LegacyOpenDistroDestinationSettings.Companion.HOST_DENY_LIST_NONE
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.addUserBackendRolesFilter
import org.opensearch.alerting.util.isADMonitor
import org.opensearch.alerting.util.isAllowed
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.Strings
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.component.AbstractLifecycleComponent
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.ScriptType
import org.opensearch.script.TemplateScript
import org.opensearch.search.builder.SearchSourceBuilder
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
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_BACKOFF_MILLIS, ALERT_BACKOFF_COUNT) {
            millis, count -> retryPolicy = BackoffPolicy.constantBackoff(millis, count)
        }

        moveAlertsRetryPolicy =
            BackoffPolicy.exponentialBackoff(MOVE_ALERTS_BACKOFF_MILLIS.get(settings), MOVE_ALERTS_BACKOFF_COUNT.get(settings))
        clusterService.clusterSettings.addSettingsUpdateConsumer(MOVE_ALERTS_BACKOFF_MILLIS, MOVE_ALERTS_BACKOFF_COUNT) {
            millis, count -> moveAlertsRetryPolicy = BackoffPolicy.exponentialBackoff(millis, count)
        }

        allowList = ALLOW_LIST.get(settings)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) {
            allowList = it
        }

        // Host deny list is not a dynamic setting so no consumer is registered but the variable is set here
        hostDenyList = HOST_DENY_LIST.get(settings)

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

        launch { runMonitor(job, periodStart, periodEnd) }
    }

    suspend fun runMonitor(monitor: Monitor, periodStart: Instant, periodEnd: Instant, dryrun: Boolean = false): MonitorRunResult {
        /*
         * We need to handle 3 cases:
         * 1. Monitors created by older versions and never updated. These monitors wont have User details in the
         * monitor object. `monitor.user` will be null. Insert `all_access, AmazonES_all_access` role.
         * 2. Monitors are created when security plugin is disabled, these will have empty User object.
         * (`monitor.user.name`, `monitor.user.roles` are empty )
         * 3. Monitors are created when security plugin is enabled, these will have an User object.
         */
        val roles = if (monitor.user == null) {
            // fixme: discuss and remove hardcoded to settings?
            settings.getAsList("", listOf("all_access", "AmazonES_all_access"))
        } else {
            monitor.user.roles
        }
        logger.debug("Running monitor: ${monitor.name} with roles: $roles Thread: ${Thread.currentThread().name}")

        if (periodStart == periodEnd) {
            logger.warn("Start and end time are the same: $periodStart. This monitor will probably only run once.")
        }

        var monitorResult = MonitorRunResult(monitor.name, periodStart, periodEnd)
        val currentAlerts = try {
            alertIndices.createOrUpdateAlertIndex()
            alertIndices.createOrUpdateInitialHistoryIndex()
            alertService.loadCurrentAlerts(monitor)
        } catch (e: Exception) {
            // We can't save ERROR alerts to the index here as we don't know if there are existing ACTIVE alerts
            val id = if (monitor.id.trim().isEmpty()) "_na_" else monitor.id
            logger.error("Error loading alerts for monitor: $id", e)
            return monitorResult.copy(error = e)
        }
        if (!isADMonitor(monitor)) {
            runBlocking(InjectorContextElement(monitor.id, settings, threadPool.threadContext, roles)) {
                monitorResult = monitorResult.copy(inputResults = inputService.collectInputResults(monitor, periodStart, periodEnd))
            }
        } else {
            monitorResult = monitorResult.copy(inputResults = inputService.collectInputResultsForADMonitor(monitor, periodStart, periodEnd))
        }

        val updatedAlerts = mutableListOf<Alert>()
        val triggerResults = mutableMapOf<String, TriggerRunResult>()
        for (trigger in monitor.triggers) {
            val currentAlert = currentAlerts[trigger]
            val triggerCtx = TriggerExecutionContext(monitor, trigger, monitorResult, currentAlert)
            val triggerResult = triggerService.runTrigger(monitor, trigger, triggerCtx)
            triggerResults[trigger.id] = triggerResult

            if (triggerService.isTriggerActionable(triggerCtx, triggerResult)) {
                val actionCtx = triggerCtx.copy(error = monitorResult.error ?: triggerResult.error)
                for (action in trigger.actions) {
                    triggerResult.actionResults[action.id] = runAction(action, actionCtx, dryrun)
                }
            }

            val updatedAlert = alertService.composeAlert(triggerCtx, triggerResult,
                monitorResult.alertError() ?: triggerResult.alertError())
            if (updatedAlert != null) updatedAlerts += updatedAlert
        }

        // Don't save alerts if this is a test monitor
        if (!dryrun && monitor.id != Monitor.NO_ID) {
            alertService.saveAlerts(updatedAlerts, retryPolicy)
        }
        return monitorResult.copy(triggerResults = triggerResults)
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

    private suspend fun runAction(action: Action, ctx: TriggerExecutionContext, dryrun: Boolean): ActionRunResult {
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

    private fun compileTemplate(template: Script, ctx: TriggerExecutionContext): String {
        return scriptService.compile(template, TemplateScript.CONTEXT)
            .newInstance(template.params + mapOf("ctx" to ctx.asTemplateArg()))
            .execute()
    }
}
