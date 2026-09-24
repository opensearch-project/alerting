/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.ActionRunResult
import org.opensearch.commons.alerting.model.BucketLevelTriggerRunResult
import org.opensearch.commons.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.commons.alerting.model.MonitorRunResult
import org.opensearch.commons.alerting.model.QueryLevelTriggerRunResult
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient
import software.amazon.awssdk.services.cloudwatch.model.Dimension
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Emits per-application and per-cell monitor execution metrics to CloudWatch so operational
 * alarms can compute error **rates** (failures / attempts) at both customer and cell scope.
 *
 * Why this exists: the alerting index only persists alerts that fired (or a latched
 * monitor-error-alert), so it cannot tell us how many times a monitor ran or how many runs
 * failed. The execution path — [org.opensearch.alerting.transport.TransportExecuteMonitorAction]
 * running in this OASIS-operated cluster — is the only place that observes every run and its
 * outcome, so counters are recorded there and flushed here.
 *
 * Metrics (all `Count`, namespace [AlertingSettings.CLOUDWATCH_METRICS_NAMESPACE]):
 *  - `MonitorExecutions` / `MonitorExecutionFailures`  → monitor-execution error rate
 *  - `SearchFailures`        (denominator: MonitorExecutions) → AOSS/input search error rate
 *  - `TriggerEvaluations` / `TriggerExecutionFailures` → trigger-eval error rate
 *  - `NotificationSends` / `NotificationSendFailures`  → notification-send error rate
 *
 * Each flush emits every metric at two dimension granularities so alarms can target either:
 *  - per-customer: `{Stage, Region, CellId, ApplicationId}`
 *  - per-cell rollup: `{Stage, Region, CellId}` (sum across applications)
 *
 * Counters are in-memory deltas reset on each flush, so a CloudWatch SUM over the period
 * reconstructs totals. Uses [DefaultCredentialsProvider] (the cluster role; requires
 * `cloudwatch:PutMetricData`) and the region from [AlertingSettings.REMOTE_METADATA_REGION].
 * Entirely gated by [AlertingSettings.CLOUDWATCH_METRICS_ENABLED] (default false).
 */
object AlertingMetricsService {

    private val log = LogManager.getLogger(AlertingMetricsService::class.java)

    const val METRIC_EXECUTIONS = "MonitorExecutions"
    const val METRIC_EXECUTION_FAILURES = "MonitorExecutionFailures"
    const val METRIC_SEARCH_FAILURES = "SearchFailures"
    const val METRIC_TRIGGER_EVALUATIONS = "TriggerEvaluations"
    const val METRIC_TRIGGER_FAILURES = "TriggerExecutionFailures"
    const val METRIC_NOTIFICATION_SENDS = "NotificationSends"
    const val METRIC_NOTIFICATION_FAILURES = "NotificationSendFailures"

    const val DIMENSION_STAGE = "Stage"
    const val DIMENSION_REGION = "Region"
    const val DIMENSION_CELL_ID = "CellId"
    const val DIMENSION_APPLICATION_ID = "ApplicationId"
    private const val UNKNOWN = "unknown"

    /** CloudWatch PutMetricData accepts at most 1000 metric data items per request. */
    private const val MAX_METRIC_DATA_PER_REQUEST = 1000

    /** Mutable per-application delta counters. */
    private class Counters {
        val executions = AtomicLong(0)
        val executionFailures = AtomicLong(0)
        val searchFailures = AtomicLong(0)
        val triggerEvaluations = AtomicLong(0)
        val triggerFailures = AtomicLong(0)
        val notificationSends = AtomicLong(0)
        val notificationFailures = AtomicLong(0)
    }

    /** Immutable snapshot of [Counters] taken at flush time. */
    private class Snapshot {
        var executions = 0L
        var executionFailures = 0L
        var searchFailures = 0L
        var triggerEvaluations = 0L
        var triggerFailures = 0L
        var notificationSends = 0L
        var notificationFailures = 0L

        val isEmpty: Boolean
            get() = executions == 0L && executionFailures == 0L && searchFailures == 0L &&
                triggerEvaluations == 0L && triggerFailures == 0L &&
                notificationSends == 0L && notificationFailures == 0L

        fun add(other: Snapshot) {
            executions += other.executions
            executionFailures += other.executionFailures
            searchFailures += other.searchFailures
            triggerEvaluations += other.triggerEvaluations
            triggerFailures += other.triggerFailures
            notificationSends += other.notificationSends
            notificationFailures += other.notificationFailures
        }
    }

    private val countersByApp = ConcurrentHashMap<String, Counters>()

    @Volatile private var enabled: Boolean = false
    @Volatile private var logOnly: Boolean = false
    @Volatile private var namespace: String = "ObservabilityOasis/Alerting"
    @Volatile private var stage: String = UNKNOWN
    @Volatile private var region: String = UNKNOWN
    @Volatile private var cellId: String = UNKNOWN
    @Volatile private var cloudWatchClient: CloudWatchClient? = null
    @Volatile private var flushTask: Scheduler.Cancellable? = null

    /**
     * Initializes the service from plugin settings and starts the periodic flush. Safe to call
     * once during plugin component creation. No-op (and no CloudWatch client) when disabled.
     */
    @Synchronized
    fun initialize(settings: Settings, threadPool: ThreadPool) {
        enabled = AlertingSettings.CLOUDWATCH_METRICS_ENABLED.get(settings)
        if (!enabled) {
            log.info("AlertingMetricsService disabled; not emitting monitor execution metrics to CloudWatch")
            return
        }
        logOnly = AlertingSettings.CLOUDWATCH_METRICS_LOG_ONLY.get(settings)
        namespace = AlertingSettings.CLOUDWATCH_METRICS_NAMESPACE.get(settings)
        stage = AlertingSettings.CLOUDWATCH_METRICS_STAGE.get(settings).ifBlank { UNKNOWN }
        cellId = AlertingSettings.CLOUDWATCH_METRICS_CELL_ID.get(settings).ifBlank { UNKNOWN }
        region = AlertingSettings.REMOTE_METADATA_REGION.get(settings).orEmpty().ifBlank { UNKNOWN }
        if (!logOnly) {
            if (region == UNKNOWN) {
                log.warn(
                    "AlertingMetricsService enabled but {} is blank; disabling metric emission",
                    AlertingSettings.REMOTE_METADATA_REGION.key
                )
                enabled = false
                return
            }
            cloudWatchClient = CloudWatchClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()
        }
        val intervalSeconds = AlertingSettings.CLOUDWATCH_METRICS_FLUSH_INTERVAL_SECONDS.get(settings).toLong()
        flushTask = threadPool.scheduleWithFixedDelay(
            { flush() },
            TimeValue.timeValueSeconds(intervalSeconds),
            ThreadPool.Names.GENERIC
        )
        log.info(
            "AlertingMetricsService enabled; logOnly={} namespace={} stage={} region={} cellId={} flushIntervalSeconds={}",
            logOnly, namespace, stage, region, cellId, intervalSeconds
        )
    }

    /**
     * Records one completed monitor execution and classifies its outcome from [runResult]:
     * monitor-level error, input/search error, per-trigger eval errors, and per-action
     * notification-send errors. Call once per non-dryrun execution.
     */
    fun recordRun(applicationId: String?, runResult: MonitorRunResult<*>) {
        if (!enabled) return
        val c = counters(applicationId)
        c.executions.incrementAndGet()
        if (runResult.error != null) c.executionFailures.incrementAndGet()
        if (runResult.inputResults.error != null) c.searchFailures.incrementAndGet()
        for (triggerResult in runResult.triggerResults.values) {
            c.triggerEvaluations.incrementAndGet()
            if (triggerResult.error != null) c.triggerFailures.incrementAndGet()
            for (action in actionResultsOf(triggerResult)) {
                c.notificationSends.incrementAndGet()
                if (action.error != null) c.notificationFailures.incrementAndGet()
            }
        }
    }

    /**
     * Records a monitor execution that failed before producing a [MonitorRunResult] (a thrown
     * exception). Counts as both an execution and an execution failure.
     */
    fun recordExecutionFailure(applicationId: String?) {
        if (!enabled) return
        val c = counters(applicationId)
        c.executions.incrementAndGet()
        c.executionFailures.incrementAndGet()
    }

    /** Flattens the heterogeneous per-trigger action-result shapes into a single collection. */
    private fun actionResultsOf(triggerResult: Any): Collection<ActionRunResult> = when (triggerResult) {
        is QueryLevelTriggerRunResult -> triggerResult.actionResults.values
        is BucketLevelTriggerRunResult -> triggerResult.actionResultsMap.values.flatMap { it.values }
        is DocumentLevelTriggerRunResult -> triggerResult.actionResultsMap.values.flatMap { it.values }
        else -> emptyList()
    }

    private fun counters(applicationId: String?): Counters {
        val key = if (applicationId.isNullOrBlank()) UNKNOWN else applicationId
        return countersByApp.computeIfAbsent(key) { Counters() }
    }

    /**
     * Snapshots and resets all per-app counters, emits per-app and per-cell-rollup metrics to
     * CloudWatch. Visible for testing. Never throws — emission failures are logged and the
     * (already-reset) deltas are dropped to avoid unbounded growth.
     */
    internal fun flush() {
        if (!enabled) return
        val data = ArrayList<MetricDatum>()
        val cellTotals = Snapshot()
        for ((appId, counters) in countersByApp) {
            val snap = snapshotAndReset(counters)
            if (snap.isEmpty) continue
            cellTotals.add(snap)
            data.addAll(toData(snap, appDimensions(appId)))
        }
        if (!cellTotals.isEmpty) {
            data.addAll(toData(cellTotals, cellDimensions()))
        }
        if (data.isEmpty()) return
        emit(data)
    }

    /** Emits assembled metric data: logs it in log-only mode, otherwise sends to CloudWatch. */
    private fun emit(data: List<MetricDatum>) {
        val client = cloudWatchClient
        if (client == null) {
            // Log-only sink (local testing): print each datum at INFO; no CloudWatch call.
            for (d in data) {
                val dims = d.dimensions().joinToString(",") { "${it.name()}=${it.value()}" }
                log.info("alerting-metric namespace={} {}{{{}}} = {}", namespace, d.metricName(), dims, d.value())
            }
            return
        }
        try {
            data.chunked(MAX_METRIC_DATA_PER_REQUEST).forEach { chunk ->
                client.putMetricData(
                    PutMetricDataRequest.builder().namespace(namespace).metricData(chunk).build()
                )
            }
        } catch (e: Exception) {
            log.warn("Failed to emit alerting execution metrics to CloudWatch", e)
        }
    }

    private fun snapshotAndReset(c: Counters): Snapshot {
        val s = Snapshot()
        s.executions = c.executions.getAndSet(0)
        s.executionFailures = c.executionFailures.getAndSet(0)
        s.searchFailures = c.searchFailures.getAndSet(0)
        s.triggerEvaluations = c.triggerEvaluations.getAndSet(0)
        s.triggerFailures = c.triggerFailures.getAndSet(0)
        s.notificationSends = c.notificationSends.getAndSet(0)
        s.notificationFailures = c.notificationFailures.getAndSet(0)
        return s
    }

    private fun baseDimensions(): List<Dimension> = listOf(
        Dimension.builder().name(DIMENSION_STAGE).value(stage).build(),
        Dimension.builder().name(DIMENSION_REGION).value(region).build(),
        Dimension.builder().name(DIMENSION_CELL_ID).value(cellId).build()
    )

    private fun cellDimensions(): List<Dimension> = baseDimensions()

    private fun appDimensions(appId: String): List<Dimension> =
        baseDimensions() + Dimension.builder().name(DIMENSION_APPLICATION_ID).value(appId).build()

    private fun toData(s: Snapshot, dimensions: List<Dimension>): List<MetricDatum> = listOf(
        datum(METRIC_EXECUTIONS, dimensions, s.executions),
        datum(METRIC_EXECUTION_FAILURES, dimensions, s.executionFailures),
        datum(METRIC_SEARCH_FAILURES, dimensions, s.searchFailures),
        datum(METRIC_TRIGGER_EVALUATIONS, dimensions, s.triggerEvaluations),
        datum(METRIC_TRIGGER_FAILURES, dimensions, s.triggerFailures),
        datum(METRIC_NOTIFICATION_SENDS, dimensions, s.notificationSends),
        datum(METRIC_NOTIFICATION_FAILURES, dimensions, s.notificationFailures)
    )

    private fun datum(name: String, dimensions: List<Dimension>, value: Long): MetricDatum =
        MetricDatum.builder()
            .metricName(name)
            .dimensions(dimensions)
            .value(value.toDouble())
            .unit(StandardUnit.COUNT)
            .build()

    /** Stops the flush task and closes the CloudWatch client. */
    @Synchronized
    fun close() {
        flushTask?.cancel()
        flushTask = null
        cloudWatchClient?.close()
        cloudWatchClient = null
        enabled = false
        logOnly = false
        countersByApp.clear()
    }

    /** Visible for testing: clears in-memory counters without touching the client/flush task. */
    internal fun resetForTest() {
        countersByApp.clear()
    }

    /** Visible for testing: current (un-flushed) executions counter for an app, 0 if absent. */
    internal fun peekExecutionsForTest(applicationId: String): Long =
        countersByApp[applicationId]?.executions?.get() ?: 0L

    /** Visible for testing: enables the service with an injected client + dimensions, no flush task. */
    internal fun initializeForTest(client: CloudWatchClient, ns: String, stageValue: String, regionValue: String, cellIdValue: String) {
        enabled = true
        logOnly = false
        cloudWatchClient = client
        namespace = ns
        stage = stageValue
        region = regionValue
        cellId = cellIdValue
        countersByApp.clear()
    }

    /** Visible for testing: enables the service in log-only mode (no client), no flush task. */
    internal fun initializeForTestLogOnly(ns: String, stageValue: String, regionValue: String, cellIdValue: String) {
        enabled = true
        logOnly = true
        cloudWatchClient = null
        namespace = ns
        stage = stageValue
        region = regionValue
        cellId = cellIdValue
        countersByApp.clear()
    }
}
