/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.clusterMetricsMonitorHelpers

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
import org.opensearch.action.admin.cluster.state.ClusterStateResponse
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest
import org.opensearch.action.admin.cluster.stats.ClusterStatsResponse
import org.opensearch.action.admin.cluster.tasks.PendingClusterTasksRequest
import org.opensearch.action.admin.cluster.tasks.PendingClusterTasksResponse
import org.opensearch.action.admin.indices.recovery.RecoveryRequest
import org.opensearch.action.admin.indices.recovery.RecoveryResponse
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.alerting.opensearchapi.convertToMap
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.SupportedClusterMetricsSettings
import org.opensearch.alerting.settings.SupportedClusterMetricsSettings.Companion.resolveToActionRequest
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.support.XContentMapValues
import org.opensearch.commons.alerting.model.ClusterMetricsInput
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

/**
 * Calls the appropriate transport action for the API requested in the [clusterMetricsInput].
 * @param clusterMetricsInput The [ClusterMetricsInput] to resolve.
 * @param client The [Client] used to call the respective transport action.
 * @throws IllegalArgumentException When the requested API is not supported by this feature.
 */
suspend fun executeTransportAction(clusterMetricsInput: ClusterMetricsInput, client: Client): ActionResponse {
    val request = resolveToActionRequest(clusterMetricsInput)
    return when (clusterMetricsInput.clusterMetricType) {
        ClusterMetricsInput.ClusterMetricType.CAT_INDICES -> {
            request as CatIndicesRequestWrapper
            val healthResponse: ClusterHealthResponse =
                client.suspendUntil { admin().cluster().health(request.clusterHealthRequest) }
            val indexSettingsResponse: GetSettingsResponse =
                client.suspendUntil { client.admin().indices().getSettings(request.indexSettingsRequest) }
            val indicesResponse: IndicesStatsResponse =
                client.suspendUntil { client.admin().indices().stats(request.indicesStatsRequest) }
            val stateResponse: ClusterStateResponse =
                client.suspendUntil { client.admin().cluster().state(request.clusterStateRequest) }
            return CatIndicesResponseWrapper(healthResponse, stateResponse, indexSettingsResponse, indicesResponse)
        }
        ClusterMetricsInput.ClusterMetricType.CAT_PENDING_TASKS ->
            client.suspendUntil { client.admin().cluster().pendingClusterTasks(request as PendingClusterTasksRequest) }
        ClusterMetricsInput.ClusterMetricType.CAT_RECOVERY ->
            client.suspendUntil { client.admin().indices().recoveries(request as RecoveryRequest) }
        ClusterMetricsInput.ClusterMetricType.CAT_SHARDS -> {
            request as CatShardsRequestWrapper
            val stateResponse: ClusterStateResponse =
                client.suspendUntil { client.admin().cluster().state(request.clusterStateRequest) }
            val indicesResponse: IndicesStatsResponse =
                client.suspendUntil { client.admin().indices().stats(request.indicesStatsRequest) }
            return CatShardsResponseWrapper(stateResponse, indicesResponse)
        }
        ClusterMetricsInput.ClusterMetricType.CAT_SNAPSHOTS ->
            client.suspendUntil { client.admin().cluster().getSnapshots(request as GetSnapshotsRequest) }
        ClusterMetricsInput.ClusterMetricType.CAT_TASKS ->
            client.suspendUntil { client.admin().cluster().listTasks(request as ListTasksRequest) }
        ClusterMetricsInput.ClusterMetricType.CLUSTER_HEALTH ->
            client.suspendUntil { client.admin().cluster().health(request as ClusterHealthRequest) }
        ClusterMetricsInput.ClusterMetricType.CLUSTER_SETTINGS -> {
            val stateResponse: ClusterStateResponse =
                client.suspendUntil { client.admin().cluster().state(request as ClusterStateRequest) }
            val metadata: Metadata = stateResponse.state.metadata
            return ClusterGetSettingsResponse(metadata.persistentSettings(), metadata.transientSettings(), Settings.EMPTY)
        }
        ClusterMetricsInput.ClusterMetricType.CLUSTER_STATS ->
            client.suspendUntil { client.admin().cluster().clusterStats(request as ClusterStatsRequest) }
        ClusterMetricsInput.ClusterMetricType.NODES_STATS ->
            client.suspendUntil { client.admin().cluster().nodesStats(request as NodesStatsRequest) }
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
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricsInput.ClusterMetricType.CLUSTER_HEALTH.defaultPath)
        )
        is ClusterStatsResponse -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricsInput.ClusterMetricType.CLUSTER_STATS.defaultPath)
        )
        is ClusterGetSettingsResponse -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricsInput.ClusterMetricType.CLUSTER_SETTINGS.defaultPath)
        )
        is CatIndicesResponseWrapper -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricsInput.ClusterMetricType.CAT_INDICES.defaultPath)
        )
        is CatShardsResponseWrapper -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricsInput.ClusterMetricType.CAT_SHARDS.defaultPath)
        )
        is NodesStatsResponse -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricsInput.ClusterMetricType.NODES_STATS.defaultPath)
        )
        is PendingClusterTasksResponse -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricsInput.ClusterMetricType.CAT_PENDING_TASKS.defaultPath)
        )
        is RecoveryResponse -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricsInput.ClusterMetricType.CAT_RECOVERY.defaultPath)
        )
        is GetSnapshotsResponse -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricsInput.ClusterMetricType.CAT_SNAPSHOTS.defaultPath)
        )
        is ListTasksResponse -> redactFieldsFromResponse(
            this.convertToMap(),
            SupportedClusterMetricsSettings.getSupportedJsonPayload(ClusterMetricsInput.ClusterMetricType.CAT_TASKS.defaultPath)
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
