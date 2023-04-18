/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.workflow

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.alerting.AlertService
import org.opensearch.alerting.InputService
import org.opensearch.alerting.MonitorRunnerExecutionContext
import org.opensearch.alerting.TriggerService
import org.opensearch.alerting.WorkflowService
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.core.JobRunner
import org.opensearch.alerting.model.WorkflowRunResult
import org.opensearch.alerting.model.destination.DestinationContextFactory
import org.opensearch.alerting.script.TriggerExecutionContext
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_BACKOFF_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_BACKOFF_MILLIS
import org.opensearch.alerting.settings.AlertingSettings.Companion.INDEX_TIMEOUT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MAX_ACTIONABLE_ALERT_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MOVE_ALERTS_BACKOFF_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MOVE_ALERTS_BACKOFF_MILLIS
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST
import org.opensearch.alerting.settings.DestinationSettings.Companion.HOST_DENY_LIST
import org.opensearch.alerting.settings.DestinationSettings.Companion.loadDestinationSettings
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.component.AbstractLifecycleComponent
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.model.action.Action
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript
import org.opensearch.threadpool.ThreadPool
import java.time.Instant
import kotlin.coroutines.CoroutineContext

object WorkflowRunnerService : JobRunner, CoroutineScope, AbstractLifecycleComponent() {

    private val logger = LogManager.getLogger(javaClass)

    var monitorCtx: MonitorRunnerExecutionContext = MonitorRunnerExecutionContext()
    private lateinit var runnerSupervisor: Job
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + runnerSupervisor

    fun registerClusterService(clusterService: ClusterService): WorkflowRunnerService {
        monitorCtx.clusterService = clusterService
        return this
    }

    fun registerClient(client: Client): WorkflowRunnerService {
        monitorCtx.client = client
        return this
    }

    fun registerNamedXContentRegistry(xContentRegistry: NamedXContentRegistry): WorkflowRunnerService {
        monitorCtx.xContentRegistry = xContentRegistry
        return this
    }

    fun registerScriptService(scriptService: ScriptService): WorkflowRunnerService {
        monitorCtx.scriptService = scriptService
        return this
    }

    fun registerIndexNameExpressionResolver(indexNameExpressionResolver: IndexNameExpressionResolver): WorkflowRunnerService {
        monitorCtx.indexNameExpressionResolver = indexNameExpressionResolver
        return this
    }

    fun registerSettings(settings: Settings): WorkflowRunnerService {
        monitorCtx.settings = settings
        return this
    }

    fun registerThreadPool(threadPool: ThreadPool): WorkflowRunnerService {
        monitorCtx.threadPool = threadPool
        return this
    }

    fun registerAlertIndices(alertIndices: AlertIndices): WorkflowRunnerService {
        monitorCtx.alertIndices = alertIndices
        return this
    }

    fun registerInputService(inputService: InputService): WorkflowRunnerService {
        monitorCtx.inputService = inputService
        return this
    }

    fun registerWorkflowService(workflowService: WorkflowService): WorkflowRunnerService {
        monitorCtx.workflowService = workflowService
        return this
    }

    fun registerTriggerService(triggerService: TriggerService): WorkflowRunnerService {
        monitorCtx.triggerService = triggerService
        return this
    }

    fun registerAlertService(alertService: AlertService): WorkflowRunnerService {
        monitorCtx.alertService = alertService
        return this
    }

    fun registerDocLevelMonitorQueries(docLevelMonitorQueries: DocLevelMonitorQueries): WorkflowRunnerService {
        monitorCtx.docLevelMonitorQueries = docLevelMonitorQueries
        return this
    }

    // Must be called after registerClusterService and registerSettings in AlertingPlugin
    fun registerConsumers(): WorkflowRunnerService {
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

        monitorCtx.indexTimeout = INDEX_TIMEOUT.get(monitorCtx.settings)

        return this
    }

    // To be safe, call this last as it depends on a number of other components being registered beforehand (client, settings, etc.)
    fun registerDestinationSettings(): WorkflowRunnerService {
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
    }

    override fun postDelete(jobId: String) {
    }

    override fun runJob(job: ScheduledJob, periodStart: Instant, periodEnd: Instant) {
        if (job !is Workflow) {
            throw IllegalArgumentException("Invalid job type")
        }
        launch {
            runJob(job, periodStart, periodEnd, false)
        }
    }

    suspend fun runJob(job: ScheduledJob, periodStart: Instant, periodEnd: Instant, dryrun: Boolean): WorkflowRunResult {
        val workflow = job as Workflow
        return CompositeWorkflowRunner.runWorkflow(workflow, monitorCtx, periodStart, periodEnd, dryrun)
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
            monitor.user!!.roles
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
            val throttledTimeBound = currentTime().minus(action.throttle!!.value.toLong(), action.throttle!!.unit)
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
