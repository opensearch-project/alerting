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
import org.apache.logging.log4j.LogManager
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.alerts.moveAlerts
import org.opensearch.alerting.core.JobRunner
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.elasticapi.retry
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.action.Action
import org.opensearch.alerting.model.destination.DestinationContextFactory
import org.opensearch.alerting.script.TriggerExecutionContext
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_BACKOFF_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_BACKOFF_MILLIS
import org.opensearch.alerting.settings.AlertingSettings.Companion.MAX_ACTIONABLE_ALERT_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MOVE_ALERTS_BACKOFF_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MOVE_ALERTS_BACKOFF_MILLIS
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST
import org.opensearch.alerting.settings.DestinationSettings.Companion.HOST_DENY_LIST
import org.opensearch.alerting.settings.DestinationSettings.Companion.loadDestinationSettings
import org.opensearch.alerting.util.isBucketLevelMonitor
import org.opensearch.alerting.util.isDocLevelMonitor
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.component.AbstractLifecycleComponent
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript
import org.opensearch.threadpool.ThreadPool
import java.time.Instant
import kotlin.coroutines.CoroutineContext

object MonitorRunnerService : JobRunner, CoroutineScope, AbstractLifecycleComponent() {

    private val logger = LogManager.getLogger(javaClass)

    var monitorCtx: MonitorRunnerExecutionContext = MonitorRunnerExecutionContext()

    private lateinit var runnerSupervisor: Job
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + runnerSupervisor

    fun registerClusterService(clusterService: ClusterService): MonitorRunnerService {
        this.monitorCtx.clusterService = clusterService
        return this
    }

    fun registerClient(client: Client): MonitorRunnerService {
        this.monitorCtx.client = client
        return this
    }

    fun registerNamedXContentRegistry(xContentRegistry: NamedXContentRegistry): MonitorRunnerService {
        this.monitorCtx.xContentRegistry = xContentRegistry
        return this
    }

    fun registerScriptService(scriptService: ScriptService): MonitorRunnerService {
        this.monitorCtx.scriptService = scriptService
        return this
    }

    fun registerSettings(settings: Settings): MonitorRunnerService {
        this.monitorCtx.settings = settings
        return this
    }

    fun registerThreadPool(threadPool: ThreadPool): MonitorRunnerService {
        this.monitorCtx.threadPool = threadPool
        return this
    }

    fun registerAlertIndices(alertIndices: AlertIndices): MonitorRunnerService {
        this.monitorCtx.alertIndices = alertIndices
        return this
    }

    fun registerInputService(inputService: InputService): MonitorRunnerService {
        this.monitorCtx.inputService = inputService
        return this
    }

    fun registerTriggerService(triggerService: TriggerService): MonitorRunnerService {
        this.monitorCtx.triggerService = triggerService
        return this
    }

    fun registerAlertService(alertService: AlertService): MonitorRunnerService {
        this.monitorCtx.alertService = alertService
        return this
    }

    // Must be called after registerClusterService and registerSettings in AlertingPlugin
    fun registerConsumers(): MonitorRunnerService {
        monitorCtx.retryPolicy = BackoffPolicy.constantBackoff(
            ALERT_BACKOFF_MILLIS.get(monitorCtx.settings),
            ALERT_BACKOFF_COUNT.get(monitorCtx.settings)
        )
        monitorCtx.clusterService!!.clusterSettings.addSettingsUpdateConsumer(ALERT_BACKOFF_MILLIS, ALERT_BACKOFF_COUNT) { millis, count ->
            monitorCtx.retryPolicy = BackoffPolicy.constantBackoff(millis, count)
        }

        monitorCtx.moveAlertsRetryPolicy =
            BackoffPolicy.exponentialBackoff(
                MOVE_ALERTS_BACKOFF_MILLIS.get(monitorCtx.settings),
                MOVE_ALERTS_BACKOFF_COUNT.get(monitorCtx.settings)
            )
        monitorCtx.clusterService!!.clusterSettings.addSettingsUpdateConsumer(MOVE_ALERTS_BACKOFF_MILLIS, MOVE_ALERTS_BACKOFF_COUNT) {
            millis, count ->
            monitorCtx.moveAlertsRetryPolicy = BackoffPolicy.exponentialBackoff(millis, count)
        }

        monitorCtx.allowList = ALLOW_LIST.get(monitorCtx.settings)
        monitorCtx.clusterService!!.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) {
            monitorCtx.allowList = it
        }

        // Host deny list is not a dynamic setting so no consumer is registered but the variable is set here
        monitorCtx.hostDenyList = HOST_DENY_LIST.get(monitorCtx.settings)

        monitorCtx.maxActionableAlertCount = MAX_ACTIONABLE_ALERT_COUNT.get(monitorCtx.settings)
        monitorCtx.clusterService!!.clusterSettings.addSettingsUpdateConsumer(MAX_ACTIONABLE_ALERT_COUNT) {
            monitorCtx.maxActionableAlertCount = it
        }

        return this
    }

    // To be safe, call this last as it depends on a number of other components being registered beforehand (client, settings, etc.)
    fun registerDestinationSettings(): MonitorRunnerService {
        monitorCtx.destinationSettings = loadDestinationSettings(monitorCtx.settings!!)
        monitorCtx.destinationContextFactory =
            DestinationContextFactory(monitorCtx.client!!, monitorCtx.xContentRegistry!!, monitorCtx.destinationSettings!!)
        return this
    }

    // Updates destination settings when the reload API is called so that new keystore values are visible
    fun reloadDestinationSettings(settings: Settings) {
        monitorCtx.destinationSettings = loadDestinationSettings(settings)

        // Update destinationContextFactory as well since destinationSettings has been updated
        monitorCtx.destinationContextFactory!!.updateDestinationSettings(monitorCtx.destinationSettings!!)
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
                monitorCtx.moveAlertsRetryPolicy!!.retry(logger) {
                    if (monitorCtx.alertIndices!!.isAlertInitialized()) {
                        moveAlerts(monitorCtx.client!!, job.id, job)
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
                monitorCtx.moveAlertsRetryPolicy!!.retry(logger) {
                    if (monitorCtx.alertIndices!!.isAlertInitialized()) {
                        moveAlerts(monitorCtx.client!!, jobId, null)
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
            runJob(job, periodStart, periodEnd, false)
        }
    }

    suspend fun runJob(job: ScheduledJob, periodStart: Instant, periodEnd: Instant, dryrun: Boolean): MonitorRunResult<*> {
        val monitor = job as Monitor
        return if (monitor.isBucketLevelMonitor()) {
            BucketLevelMonitorRunner.runMonitor(monitor, monitorCtx, periodStart, periodEnd, dryrun)
        } else if (monitor.isDocLevelMonitor()) {
            DocumentReturningMonitorRunner.runMonitor(monitor, monitorCtx, periodStart, periodEnd, dryrun)
        } else {
            QueryLevelMonitorRunner.runMonitor(monitor, monitorCtx, periodStart, periodEnd, dryrun)
        }
    }

    // TODO: See if we can move below methods (or few of these) to a common utils
    internal fun getRolesForMonitor(monitor: Monitor): List<String> {
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
            monitorCtx.settings!!.getAsList("", listOf("all_access", "AmazonES_all_access"))
        } else {
            monitor.user.roles
        }
    }

    // TODO: Can this be updated to just use 'Instant.now()'?
    //  'threadPool.absoluteTimeInMillis()' is referring to a cached value of System.currentTimeMillis() that by default updates every 200ms
    internal fun currentTime() = Instant.ofEpochMilli(monitorCtx.threadPool!!.absoluteTimeInMillis())

    internal fun isActionActionable(action: Action, alert: Alert?): Boolean {
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

    internal fun compileTemplate(template: Script, ctx: TriggerExecutionContext): String {
        return monitorCtx.scriptService!!.compile(template, TemplateScript.CONTEXT)
            .newInstance(template.params + mapOf("ctx" to ctx.asTemplateArg()))
            .execute()
    }
}
