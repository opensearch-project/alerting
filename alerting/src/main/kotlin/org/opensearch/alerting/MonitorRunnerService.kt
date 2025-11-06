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
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.search.TransportSearchAction.SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.alerting.action.ExecuteMonitorAction
import org.opensearch.alerting.action.ExecuteMonitorRequest
import org.opensearch.alerting.action.ExecuteMonitorResponse
import org.opensearch.alerting.action.ExecuteWorkflowAction
import org.opensearch.alerting.action.ExecuteWorkflowRequest
import org.opensearch.alerting.action.ExecuteWorkflowResponse
import org.opensearch.alerting.actionv2.ExecuteMonitorV2Action
import org.opensearch.alerting.actionv2.ExecuteMonitorV2Request
import org.opensearch.alerting.actionv2.ExecuteMonitorV2Response
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.alerts.AlertMover.Companion.moveAlerts
import org.opensearch.alerting.alertsv2.AlertV2Indices
import org.opensearch.alerting.alertsv2.AlertV2Mover.Companion.moveAlertV2s
import org.opensearch.alerting.core.JobRunner
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.core.lock.LockModel
import org.opensearch.alerting.core.lock.LockService
import org.opensearch.alerting.model.destination.DestinationContextFactory
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.modelv2.MonitorV2RunResult
import org.opensearch.alerting.modelv2.PPLSQLMonitor
import org.opensearch.alerting.modelv2.PPLSQLMonitor.Companion.PPL_SQL_MONITOR_TYPE
import org.opensearch.alerting.opensearchapi.retry
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.remote.monitors.RemoteDocumentLevelMonitorRunner
import org.opensearch.alerting.remote.monitors.RemoteMonitorRegistry
import org.opensearch.alerting.script.TriggerExecutionContext
import org.opensearch.alerting.script.TriggerV2ExecutionContext
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_BACKOFF_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_BACKOFF_MILLIS
import org.opensearch.alerting.settings.AlertingSettings.Companion.DOC_LEVEL_MONITOR_EXECUTION_MAX_DURATION
import org.opensearch.alerting.settings.AlertingSettings.Companion.DOC_LEVEL_MONITOR_FANOUT_MAX_DURATION
import org.opensearch.alerting.settings.AlertingSettings.Companion.DOC_LEVEL_MONITOR_FETCH_ONLY_QUERY_FIELDS_ENABLED
import org.opensearch.alerting.settings.AlertingSettings.Companion.DOC_LEVEL_MONITOR_SHARD_FETCH_SIZE
import org.opensearch.alerting.settings.AlertingSettings.Companion.FINDINGS_INDEXING_BATCH_SIZE
import org.opensearch.alerting.settings.AlertingSettings.Companion.INDEX_TIMEOUT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MAX_ACTIONABLE_ALERT_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MOVE_ALERTS_BACKOFF_COUNT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MOVE_ALERTS_BACKOFF_MILLIS
import org.opensearch.alerting.settings.AlertingSettings.Companion.PERCOLATE_QUERY_DOCS_SIZE_MEMORY_PERCENTAGE_LIMIT
import org.opensearch.alerting.settings.AlertingSettings.Companion.PERCOLATE_QUERY_MAX_NUM_DOCS_IN_MEMORY
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST
import org.opensearch.alerting.settings.DestinationSettings.Companion.HOST_DENY_LIST
import org.opensearch.alerting.settings.DestinationSettings.Companion.loadDestinationSettings
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.isDocLevelMonitor
import org.opensearch.alerting.workflow.CompositeWorkflowRunner
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.lifecycle.AbstractLifecycleComponent
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorRunResult
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.TriggerRunResult
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.model.WorkflowRunResult
import org.opensearch.commons.alerting.model.action.Action
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.alerting.util.IndexPatternUtils
import org.opensearch.commons.alerting.util.isBucketLevelMonitor
import org.opensearch.commons.alerting.util.isMonitorOfStandardType
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.monitor.jvm.JvmStats
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.UUID
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

    fun registerindexNameExpressionResolver(indexNameExpressionResolver: IndexNameExpressionResolver): MonitorRunnerService {
        this.monitorCtx.indexNameExpressionResolver = indexNameExpressionResolver
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

    fun registerAlertV2Indices(alertV2Indices: AlertV2Indices): MonitorRunnerService {
        this.monitorCtx.alertV2Indices = alertV2Indices
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

    fun registerDocLevelMonitorQueries(docLevelMonitorQueries: DocLevelMonitorQueries): MonitorRunnerService {
        this.monitorCtx.docLevelMonitorQueries = docLevelMonitorQueries
        return this
    }

    fun registerWorkflowService(workflowService: WorkflowService): MonitorRunnerService {
        this.monitorCtx.workflowService = workflowService
        return this
    }

    fun registerJvmStats(jvmStats: JvmStats): MonitorRunnerService {
        this.monitorCtx.jvmStats = jvmStats
        return this
    }

    fun registerRemoteMonitors(monitorRegistry: Map<String, RemoteMonitorRegistry>): MonitorRunnerService {
        this.monitorCtx.remoteMonitors = monitorRegistry
        return this
    }

    // Must be called after registerClusterService and registerSettings in AlertingPlugin
    fun registerConsumers(): MonitorRunnerService {
        monitorCtx.retryPolicy = BackoffPolicy.constantBackoff(
            ALERT_BACKOFF_MILLIS.get(monitorCtx.settings),
            ALERT_BACKOFF_COUNT.get(monitorCtx.settings)
        )

        monitorCtx.cancelAfterTimeInterval = SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING.get(monitorCtx.settings)

        monitorCtx.clusterService!!.clusterSettings.addSettingsUpdateConsumer(ALERT_BACKOFF_MILLIS, ALERT_BACKOFF_COUNT) { millis, count ->
            monitorCtx.retryPolicy = BackoffPolicy.constantBackoff(millis, count)
        }

        monitorCtx.moveAlertsRetryPolicy =
            BackoffPolicy.exponentialBackoff(
                MOVE_ALERTS_BACKOFF_MILLIS.get(monitorCtx.settings),
                MOVE_ALERTS_BACKOFF_COUNT.get(monitorCtx.settings)
            )
        monitorCtx.clusterService!!.clusterSettings.addSettingsUpdateConsumer(
            MOVE_ALERTS_BACKOFF_MILLIS,
            MOVE_ALERTS_BACKOFF_COUNT
        ) { millis, count ->
            monitorCtx.moveAlertsRetryPolicy = BackoffPolicy.exponentialBackoff(millis, count)
        }

        monitorCtx.clusterService!!.clusterSettings.addSettingsUpdateConsumer(SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING) {
            monitorCtx.cancelAfterTimeInterval = it
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

        monitorCtx.findingsIndexBatchSize = FINDINGS_INDEXING_BATCH_SIZE.get(monitorCtx.settings)
        monitorCtx.clusterService!!.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.FINDINGS_INDEXING_BATCH_SIZE) {
            monitorCtx.findingsIndexBatchSize = it
        }

        monitorCtx.fetchOnlyQueryFieldNames = DOC_LEVEL_MONITOR_FETCH_ONLY_QUERY_FIELDS_ENABLED.get(monitorCtx.settings)
        monitorCtx.clusterService!!.clusterSettings.addSettingsUpdateConsumer(DOC_LEVEL_MONITOR_FETCH_ONLY_QUERY_FIELDS_ENABLED) {
            monitorCtx.fetchOnlyQueryFieldNames = it
        }

        monitorCtx.percQueryMaxNumDocsInMemory = PERCOLATE_QUERY_MAX_NUM_DOCS_IN_MEMORY.get(monitorCtx.settings)
        monitorCtx.clusterService!!.clusterSettings.addSettingsUpdateConsumer(PERCOLATE_QUERY_MAX_NUM_DOCS_IN_MEMORY) {
            monitorCtx.percQueryMaxNumDocsInMemory = it
        }

        monitorCtx.docLevelMonitorFanoutMaxDuration = DOC_LEVEL_MONITOR_FANOUT_MAX_DURATION.get(monitorCtx.settings)
        monitorCtx.clusterService!!.clusterSettings.addSettingsUpdateConsumer(DOC_LEVEL_MONITOR_FANOUT_MAX_DURATION) {
            monitorCtx.docLevelMonitorFanoutMaxDuration = it
        }

        monitorCtx.docLevelMonitorExecutionMaxDuration = DOC_LEVEL_MONITOR_EXECUTION_MAX_DURATION.get(monitorCtx.settings)
        monitorCtx.clusterService!!.clusterSettings.addSettingsUpdateConsumer(DOC_LEVEL_MONITOR_EXECUTION_MAX_DURATION) {
            monitorCtx.docLevelMonitorExecutionMaxDuration = it
        }

        monitorCtx.percQueryDocsSizeMemoryPercentageLimit =
            PERCOLATE_QUERY_DOCS_SIZE_MEMORY_PERCENTAGE_LIMIT.get(monitorCtx.settings)
        monitorCtx.clusterService!!.clusterSettings
            .addSettingsUpdateConsumer(PERCOLATE_QUERY_DOCS_SIZE_MEMORY_PERCENTAGE_LIMIT) {
                monitorCtx.percQueryDocsSizeMemoryPercentageLimit = it
            }

        monitorCtx.docLevelMonitorShardFetchSize =
            DOC_LEVEL_MONITOR_SHARD_FETCH_SIZE.get(monitorCtx.settings)
        monitorCtx.clusterService!!.clusterSettings
            .addSettingsUpdateConsumer(DOC_LEVEL_MONITOR_SHARD_FETCH_SIZE) {
                monitorCtx.docLevelMonitorShardFetchSize = it
            }

        monitorCtx.totalNodesFanOut = AlertingSettings.DOC_LEVEL_MONITOR_FAN_OUT_NODES.get(monitorCtx.settings)
        monitorCtx.clusterService!!.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.DOC_LEVEL_MONITOR_FAN_OUT_NODES) {
            monitorCtx.totalNodesFanOut = it
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

    fun registerLockService(lockService: LockService): MonitorRunnerService {
        monitorCtx.lockService = lockService
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

    override fun doClose() {}

    override fun postIndex(job: ScheduledJob) {
        if (job is Monitor) {
            launch {
                try {
                    monitorCtx.moveAlertsRetryPolicy!!.retry(logger) {
                        if (monitorCtx.alertIndices!!.isAlertInitialized(job.dataSources)) {
                            moveAlerts(monitorCtx.client!!, job.id, job)
                        }
                    }
                } catch (e: Exception) {
                    logger.error("Failed to move active alerts for monitor [${job.id}].", e)
                }
            }
        } else if (job is Workflow) {
            launch {
                try {
                    monitorCtx.moveAlertsRetryPolicy!!.retry(logger) {
                        moveAlerts(monitorCtx.client!!, job.id, job, monitorCtx)
                    }
                } catch (e: Exception) {
                    logger.error("Failed to move active alerts for monitor [${job.id}].", e)
                }
            }
        } else if (job is MonitorV2) {
            launch {
                try {
                    monitorCtx.moveAlertsRetryPolicy!!.retry(logger) {
                        if (monitorCtx.alertV2Indices!!.isAlertV2Initialized()) {
                            moveAlertV2s(job.id, job, monitorCtx)
                        }
                    }
                } catch (e: Exception) {
                    logger.error("Failed to move active alertV2s for monitorV2 [${job.id}].", e)
                }
            }
        } else {
            throw IllegalArgumentException("Invalid job type")
        }
    }

    override fun postDelete(jobId: String) {
        launch {
            try {
                monitorCtx.moveAlertsRetryPolicy!!.retry(logger) {
                    moveAlerts(monitorCtx.client!!, jobId, null, monitorCtx)
                }
            } catch (e: Exception) {
                logger.error("Failed to move active alerts for workflow [$jobId]. Could be a monitor", e)
            }
            try {
                monitorCtx.moveAlertsRetryPolicy!!.retry(logger) {
                    if (monitorCtx.alertIndices!!.isAlertInitialized()) {
                        moveAlerts(monitorCtx.client!!, jobId, null)
                    }
                }
            } catch (e: Exception) {
                logger.error("Failed to move active alerts for monitor [$jobId].", e)
            }
            try {
                monitorCtx.moveAlertsRetryPolicy!!.retry(logger) {
                    if (monitorCtx.alertV2Indices!!.isAlertV2Initialized()) {
                        moveAlertV2s(jobId, null, monitorCtx)
                    }
                }
            } catch (e: Exception) {
                logger.error("Failed to move active alertV2s for monitorV2 [$jobId].", e)
            }
        }
    }

    override fun runJob(job: ScheduledJob, periodStart: Instant, periodEnd: Instant) {
        when (job) {
            is Workflow -> {
                launch {
                    var workflowLock: LockModel? = null
                    try {
                        workflowLock = monitorCtx.client!!.suspendUntil<Client, LockModel?> {
                            monitorCtx.lockService!!.acquireLock(job, it)
                        } ?: return@launch
                        logger.debug("lock ${workflowLock.lockId} acquired")

                        monitorCtx.client!!.suspendUntil<Client, ExecuteWorkflowResponse> {
                            monitorCtx.client!!.execute(
                                ExecuteWorkflowAction.INSTANCE,
                                ExecuteWorkflowRequest(
                                    false,
                                    TimeValue(periodEnd.toEpochMilli()),
                                    job.id,
                                    job,
                                    TimeValue(periodStart.toEpochMilli())
                                ),
                                it
                            )
                        }
                    } catch (e: Exception) {
                        logger.error("Workflow run failed for workflow with id ${job.id}", e)
                    } finally {
                        monitorCtx.client!!.suspendUntil<Client, Boolean> { monitorCtx.lockService!!.release(workflowLock, it) }
                        logger.debug("lock ${workflowLock?.lockId} released")
                    }
                }
            }
            is Monitor -> {
                launch {
                    var monitorLock: LockModel? = null
                    try {
                        monitorLock = monitorCtx.client!!.suspendUntil<Client, LockModel?> {
                            monitorCtx.lockService!!.acquireLock(job, it)
                        } ?: return@launch
                        logger.debug("lock ${monitorLock.lockId} acquired")
                        logger.debug(
                            "PERF_DEBUG: executing ${job.monitorType} ${job.id} on node " +
                                monitorCtx.clusterService!!.state().nodes().localNode.id
                        )
                        val executeMonitorRequest = ExecuteMonitorRequest(
                            false,
                            TimeValue(periodEnd.toEpochMilli()),
                            job.id,
                            job,
                            TimeValue(periodStart.toEpochMilli())
                        )
                        monitorCtx.client!!.suspendUntil<Client, ExecuteMonitorResponse> {
                            monitorCtx.client!!.execute(
                                ExecuteMonitorAction.INSTANCE,
                                executeMonitorRequest,
                                it
                            )
                        }
                    } catch (e: Exception) {
                        logger.error("Monitor run failed for monitor with id ${job.id}", e)
                    } finally {
                        monitorCtx.client!!.suspendUntil<Client, Boolean> { monitorCtx.lockService!!.release(monitorLock, it) }
                        logger.debug("lock ${monitorLock?.lockId} released")
                    }
                }
            }
            is MonitorV2 -> {
                if (job !is PPLSQLMonitor) {
                    throw IllegalStateException("Invalid MonitorV2 type: ${job.javaClass.name}")
                }

                launch {
                    var monitorLock: LockModel? = null
                    try {
                        monitorLock = monitorCtx.client!!.suspendUntil<Client, LockModel?> {
                            monitorCtx.lockService!!.acquireLock(job, it)
                        } ?: return@launch
                        logger.debug("lock ${monitorLock!!.lockId} acquired")
                        logger.debug(
                            "PERF_DEBUG: executing $PPL_SQL_MONITOR_TYPE ${job.id} on node " +
                                monitorCtx.clusterService!!.state().nodes().localNode.id
                        )
                        val executeMonitorV2Request = ExecuteMonitorV2Request(
                            false,
                            false,
                            job.id, // only need to pass in MonitorV2 ID
                            null, // no need to pass in MonitorV2 object itself
                            TimeValue(periodEnd.toEpochMilli())
                        )
                        monitorCtx.client!!.suspendUntil<Client, ExecuteMonitorV2Response> {
                            monitorCtx.client!!.execute(
                                ExecuteMonitorV2Action.INSTANCE,
                                executeMonitorV2Request,
                                it
                            )
                        }
                    } catch (e: Exception) {
                        logger.error("MonitorV2 run failed for monitor with id ${job.id}", e)
                    } finally {
                        monitorCtx.client!!.suspendUntil { monitorCtx.lockService!!.release(monitorLock, it) }
                        logger.debug("lock ${monitorLock?.lockId} released")
                    }
                }
            }
            else -> {
                throw IllegalArgumentException("Invalid job type")
            }
        }
    }

    suspend fun runJob(
        workflow: Workflow,
        periodStart: Instant,
        periodEnd: Instant,
        dryrun: Boolean,
        transportService: TransportService
    ): WorkflowRunResult {
        return CompositeWorkflowRunner.runWorkflow(workflow, monitorCtx, periodStart, periodEnd, dryrun, transportService)
    }

    suspend fun runJob(
        job: ScheduledJob,
        periodStart: Instant,
        periodEnd: Instant,
        dryrun: Boolean,
        transportService: TransportService
    ): MonitorRunResult<*> {
        // Updating the scheduled job index at the start of monitor execution runs for when there is an upgrade the the schema mapping
        // has not been updated.
        updateAlertingConfigIndexSchema()

        if (job is Workflow) {
            logger.info("Executing scheduled workflow - id: ${job.id}, periodStart: $periodStart, periodEnd: $periodEnd, dryrun: $dryrun")
            CompositeWorkflowRunner.runWorkflow(workflow = job, monitorCtx, periodStart, periodEnd, dryrun, transportService)
        }
        val monitor = job as Monitor
        val executionId = "${monitor.id}_${LocalDateTime.now(ZoneOffset.UTC)}_${UUID.randomUUID()}"

        if (monitor.isMonitorOfStandardType()) {
            if (
                dryrun &&
                monitor.inputs.isNotEmpty() &&
                monitor.inputs[0] is DocLevelMonitorInput &&
                (monitor.inputs[0] as DocLevelMonitorInput).indices.stream().anyMatch { IndexPatternUtils.containsPatternSyntax(it) }
            ) {
                throw AlertingException(
                    "Index patterns are not supported in doc level monitors.",
                    RestStatus.BAD_REQUEST,
                    IllegalArgumentException("Index patterns are not supported in doc level monitors.")
                )
            }
            logger.info(
                "Executing scheduled monitor - id: ${monitor.id}, type: ${monitor.monitorType}, periodStart: $periodStart, " +
                    "periodEnd: $periodEnd, dryrun: $dryrun, executionId: $executionId"
            )
            val runResult = if (monitor.isBucketLevelMonitor()) {
                BucketLevelMonitorRunner.runMonitor(
                    monitor,
                    monitorCtx,
                    periodStart,
                    periodEnd,
                    dryrun,
                    executionId = executionId,
                    transportService = transportService
                )
            } else if (monitor.isDocLevelMonitor()) {
                DocumentLevelMonitorRunner().runMonitor(
                    monitor,
                    monitorCtx,
                    periodStart,
                    periodEnd,
                    dryrun,
                    executionId = executionId,
                    transportService = transportService
                )
            } else {
                QueryLevelMonitorRunner.runMonitor(
                    monitor,
                    monitorCtx,
                    periodStart,
                    periodEnd,
                    dryrun,
                    executionId = executionId,
                    transportService = transportService
                )
            }
            return runResult
        } else {
            if (monitorCtx.remoteMonitors.containsKey(monitor.monitorType)) {
                if (monitor.monitorType.endsWith(Monitor.MonitorType.DOC_LEVEL_MONITOR.value)) {
                    logger.info("Executing remote document monitor of type ${monitor.monitorType} id ${monitor.id}")
                    return RemoteDocumentLevelMonitorRunner().runMonitor(
                        monitor,
                        monitorCtx,
                        periodStart,
                        periodEnd,
                        dryrun,
                        executionId = executionId,
                        transportService = transportService
                    )
                } else {
                    logger.info("Executing remote monitor of type ${monitor.monitorType} id ${monitor.id}")
                    return monitorCtx.remoteMonitors[monitor.monitorType]!!.monitorRunner.runMonitor(
                        monitor,
                        periodStart,
                        periodEnd,
                        dryrun,
                        executionId,
                        transportService
                    )
                }
            } else {
                return MonitorRunResult<TriggerRunResult>(
                    monitor.name,
                    periodStart,
                    periodEnd,
                    OpenSearchStatusException("Monitor Type ${monitor.monitorType} not known", RestStatus.BAD_REQUEST)
                )
            }
        }
    }

    // after the above JobRunner interface override runJob calls ExecuteMonitorV2 API,
    // the ExecuteMonitorV2 transport action calls this function to call the PPLSQLMonitorRunner,
    // where the core PPL/SQL Monitor execution logic resides
    suspend fun runJobV2(
        monitorV2: MonitorV2,
        periodEnd: Instant,
        dryrun: Boolean,
        manual: Boolean,
        transportService: TransportService,
    ): MonitorV2RunResult<*> {
        updateAlertingConfigIndexSchema()

        val executionId = "${monitorV2.id}_${LocalDateTime.now(ZoneOffset.UTC)}_${UUID.randomUUID()}"
        val monitorV2Type = when (monitorV2) {
            is PPLSQLMonitor -> PPL_SQL_MONITOR_TYPE
            else -> throw IllegalStateException("Unexpected MonitorV2 type: ${monitorV2.javaClass.name}")
        }

        logger.info(
            "Executing scheduled monitor v2 - id: ${monitorV2.id}, type: $monitorV2Type, " +
                "periodEnd: $periodEnd, dryrun: $dryrun, manual: $manual, executionId: $executionId"
        )

        // for now, always call PPLSQLMonitorRunner since only PPL Monitors are initially supported
        // to introduce new MonitorV2 type, create its MonitorRunner, and if/else branch
        // to the corresponding MonitorRunners based on type. For now, default to PPLSQLMonitorRunner
        val runResult = PPLSQLMonitorRunner.runMonitorV2(
            monitorV2,
            monitorCtx,
            periodEnd,
            dryrun,
            manual,
            executionId = executionId,
            transportService = transportService,
        )
        return runResult
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
        if (alert != null && alert.state == Alert.State.AUDIT)
            return false
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

    internal fun compileTemplateV2(template: Script, ctx: TriggerV2ExecutionContext): String {
        return monitorCtx.scriptService!!.compile(template, TemplateScript.CONTEXT)
            .newInstance(template.params + mapOf("ctx" to ctx.asTemplateArg()))
            .execute()
    }

    private fun updateAlertingConfigIndexSchema() {
        if (!IndexUtils.scheduledJobIndexUpdated && monitorCtx.clusterService != null && monitorCtx.client != null) {
            IndexUtils.updateIndexMapping(
                ScheduledJob.SCHEDULED_JOBS_INDEX,
                ScheduledJobIndices.scheduledJobMappings(), monitorCtx.clusterService!!.state(), monitorCtx.client!!.admin().indices(),
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(response: AcknowledgedResponse) {
                    }

                    override fun onFailure(t: Exception) {
                        logger.error("Failed to update config index schema", t)
                    }
                }
            )
        }
    }
}
