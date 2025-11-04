/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.alertsv2.AlertV2Indices
import org.opensearch.alerting.core.lock.LockService
import org.opensearch.alerting.model.destination.DestinationContextFactory
import org.opensearch.alerting.remote.monitors.RemoteMonitorRegistry
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.DestinationSettings
import org.opensearch.alerting.settings.LegacyOpenDistroDestinationSettings
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.monitor.jvm.JvmStats
import org.opensearch.script.ScriptService
import org.opensearch.threadpool.ThreadPool

data class MonitorRunnerExecutionContext(

    var clusterService: ClusterService? = null,
    var client: Client? = null,
    var xContentRegistry: NamedXContentRegistry? = null,
    var indexNameExpressionResolver: IndexNameExpressionResolver? = null,
    var scriptService: ScriptService? = null,
    var settings: Settings? = null,
    var threadPool: ThreadPool? = null,
    var alertIndices: AlertIndices? = null,
    var alertV2Indices: AlertV2Indices? = null,
    var inputService: InputService? = null,
    var triggerService: TriggerService? = null,
    var alertService: AlertService? = null,
    var docLevelMonitorQueries: DocLevelMonitorQueries? = null,
    var workflowService: WorkflowService? = null,
    var jvmStats: JvmStats? = null,
    var findingsToTriggeredQueries: Map<String, List<DocLevelQuery>>? = null,
    var remoteMonitors: Map<String, RemoteMonitorRegistry> = mapOf(),

    @Volatile var retryPolicy: BackoffPolicy? = null,
    @Volatile var moveAlertsRetryPolicy: BackoffPolicy? = null,

    @Volatile var allowList: List<String> = DestinationSettings.ALLOW_LIST_NONE,
    @Volatile var hostDenyList: List<String> = LegacyOpenDistroDestinationSettings.HOST_DENY_LIST_NONE,

    @Volatile var destinationSettings: Map<String, DestinationSettings.Companion.SecureDestinationSettings>? = null,
    @Volatile var destinationContextFactory: DestinationContextFactory? = null,

    @Volatile var maxActionableAlertCount: Long = AlertingSettings.DEFAULT_MAX_ACTIONABLE_ALERT_COUNT,
    @Volatile var indexTimeout: TimeValue? = null,
    @Volatile var cancelAfterTimeInterval: TimeValue? = null,
    @Volatile var findingsIndexBatchSize: Int = AlertingSettings.DEFAULT_FINDINGS_INDEXING_BATCH_SIZE,
    @Volatile var fetchOnlyQueryFieldNames: Boolean = true,
    @Volatile var percQueryMaxNumDocsInMemory: Int = AlertingSettings.DEFAULT_PERCOLATE_QUERY_NUM_DOCS_IN_MEMORY,
    @Volatile var docLevelMonitorFanoutMaxDuration: TimeValue = TimeValue.timeValueMinutes(
        AlertingSettings.DEFAULT_MAX_DOC_LEVEL_MONITOR_FANOUT_MAX_DURATION_MINUTES
    ),
    @Volatile var docLevelMonitorExecutionMaxDuration: TimeValue = TimeValue.timeValueMinutes(
        AlertingSettings.DEFAULT_MAX_DOC_LEVEL_MONITOR_EXECUTION_MAX_DURATION_MINUTES
    ),
    @Volatile var percQueryDocsSizeMemoryPercentageLimit: Int =
        AlertingSettings.DEFAULT_PERCOLATE_QUERY_DOCS_SIZE_MEMORY_PERCENTAGE_LIMIT,
    @Volatile var docLevelMonitorShardFetchSize: Int =
        AlertingSettings.DEFAULT_DOC_LEVEL_MONITOR_SHARD_FETCH_SIZE,
    @Volatile var totalNodesFanOut: Int = AlertingSettings.DEFAULT_FAN_OUT_NODES,
    @Volatile var lockService: LockService? = null,
)
