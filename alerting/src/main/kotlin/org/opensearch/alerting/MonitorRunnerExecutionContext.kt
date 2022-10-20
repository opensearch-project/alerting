/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.model.destination.DestinationContextFactory
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.DestinationSettings
import org.opensearch.alerting.settings.LegacyOpenDistroDestinationSettings
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.script.ScriptService
import org.opensearch.threadpool.ThreadPool

data class MonitorRunnerExecutionContext(

    var clusterService: ClusterService? = null,
    var client: Client? = null,
    var xContentRegistry: NamedXContentRegistry? = null,
    var scriptService: ScriptService? = null,
    var settings: Settings? = null,
    var threadPool: ThreadPool? = null,
    var alertIndices: AlertIndices? = null,
    var inputService: InputService? = null,
    var triggerService: TriggerService? = null,
    var alertService: AlertService? = null,
    var docLevelMonitorQueries: DocLevelMonitorQueries? = null,

    @Volatile var retryPolicy: BackoffPolicy? = null,
    @Volatile var moveAlertsRetryPolicy: BackoffPolicy? = null,

    @Volatile var allowList: List<String> = DestinationSettings.ALLOW_LIST_NONE,
    @Volatile var hostDenyList: List<String> = LegacyOpenDistroDestinationSettings.HOST_DENY_LIST_NONE,

    @Volatile var destinationSettings: Map<String, DestinationSettings.Companion.SecureDestinationSettings>? = null,
    @Volatile var destinationContextFactory: DestinationContextFactory? = null,

    @Volatile var maxActionableAlertCount: Long = AlertingSettings.DEFAULT_MAX_ACTIONABLE_ALERT_COUNT,
    @Volatile var indexTimeout: TimeValue? = null
)
