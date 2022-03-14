/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.action.ActionResponse
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsResponse
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse
import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest
import org.opensearch.action.admin.cluster.stats.ClusterStatsResponse
import org.opensearch.action.admin.cluster.tasks.PendingClusterTasksRequest
import org.opensearch.action.admin.cluster.tasks.PendingClusterTasksResponse
import org.opensearch.action.admin.indices.recovery.RecoveryRequest
import org.opensearch.action.admin.indices.recovery.RecoveryResponse
import org.opensearch.alerting.core.model.ClusterMetricsInput
import org.opensearch.alerting.core.model.ClusterMetricsInput.ClusterMetricType
import org.opensearch.alerting.elasticapi.convertToMap
import org.opensearch.alerting.settings.SupportedClusterMetricsSettings
import org.opensearch.alerting.settings.SupportedClusterMetricsSettings.Companion.resolveToActionRequest
import org.opensearch.client.Client
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.support.XContentMapValues

/**
 * Calls the appropriate transport action for the API requested in the [clusterMetricsInput].
 * @param clusterMetricsInput The [ClusterMetricsInput] to resolve.
 * @param client The [Client] used to call the respective transport action.
 * @throws IllegalArgumentException When the requested API is not supported by this feature.
 */
fun executeTransportAction(clusterMetricsInput: ClusterMetricsInput, client: Client): ActionResponse {
    val request = resolveToActionRequest(clusterMetricsInput)
    return when (clusterMetricsInput.clusterMetricType) {
        ClusterMetricType.CAT_PENDING_TASKS -> client.admin().cluster().pendingClusterTasks(request as PendingClusterTasksRequest).get()
        ClusterMetricType.CAT_RECOVERY -> client.admin().indices().recoveries(request as RecoveryRequest).get()
        ClusterMetricType.CAT_SNAPSHOTS -> client.admin().cluster().getSnapshots(request as GetSnapshotsRequest).get()
        ClusterMetricType.CAT_TASKS -> client.admin().cluster().listTasks(request as ListTasksRequest).get()
        ClusterMetricType.CLUSTER_HEALTH -> client.admin().cluster().health(request as ClusterHealthRequest).get()
        ClusterMetricType.CLUSTER_SETTINGS -> {
            val metadata = client.admin().cluster().state(request as ClusterStateRequest).get().state.metadata
            return ClusterGetSettingsResponse(metadata.persistentSettings(), metadata.transientSettings(), Settings.EMPTY)
        }
        ClusterMetricType.CLUSTER_STATS -> client.admin().cluster().clusterStats(request as ClusterStatsRequest).get()
        ClusterMetricType.NODES_STATS -> client.admin().cluster().nodesStats(request as NodesStatsRequest).get()
        else -> throw IllegalArgumentException("Unsupported API request type: ${request.javaClass.name}")
    }
}

/**
 * Populates a [HashMap] with the values in the [ActionResponse].
 * @return The [ActionResponse] values formatted in a [HashMap].
 * @throws IllegalArgumentException when the [ActionResponse] is not supported by this feature.
 */
fun ActionResponse.toMap(): Map<String, Any> {
    return when (this) {
        is ClusterHealthResponse -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricType.CLUSTER_HEALTH.defaultPath)
        )
        is ClusterStatsResponse -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricType.CLUSTER_STATS.defaultPath)
        )
        is ClusterGetSettingsResponse -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricType.CLUSTER_SETTINGS.defaultPath)
        )
        is NodesStatsResponse -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricType.NODES_STATS.defaultPath)
        )
        is PendingClusterTasksResponse -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricType.CAT_PENDING_TASKS.defaultPath)
        )
        is RecoveryResponse -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricType.CAT_RECOVERY.defaultPath)
        )
        is GetSnapshotsResponse -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricType.CAT_SNAPSHOTS.defaultPath)
        )
        is ListTasksResponse -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricType.CAT_TASKS.defaultPath)
        )
        else -> throw IllegalArgumentException("Unsupported ActionResponse type: ${this.javaClass.name}")
    }
}

/**
 * Populates a [HashMap] with only the values that support being exposed to users.
 * @param mappedActionResponse The response from the [ClusterMetricsInput] API call.
 * @param supportedJsonPayload The JSON payload as configured in [SupportedClusterMetricsSettings.RESOURCE_FILE].
 * @return The response values [HashMap] without the redacted fields.
 */
@Suppress("UNCHECKED_CAST")
fun redactFieldsFromResponse(
    mappedActionResponse: Map<String, Any>,
    supportedJsonPayload: Map<String, ArrayList<String>>
): Map<String, Any> {
    return when {
        supportedJsonPayload.isEmpty() -> mappedActionResponse
        else -> {
            val output = hashMapOf<String, Any>()
            for ((key, value) in supportedJsonPayload) {
                when (val mappedValue = mappedActionResponse[key]) {
                    is Map<*, *> -> output[key] = XContentMapValues.filter(
                        mappedActionResponse[key] as MutableMap<String, *>?,
                        value.toTypedArray(), arrayOf()
                    )
                    else -> output[key] = mappedValue ?: hashMapOf<String, Any>()
                }
            }
            output
        }
    }
}
