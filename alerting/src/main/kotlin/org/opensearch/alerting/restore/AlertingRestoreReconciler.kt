/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.restore

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.opensearch.alerting.util.WriteAliasRepair
import org.opensearch.cluster.service.ClusterService
import org.opensearch.snapshots.RestoreContext
import org.opensearch.transport.client.Client

/**
 * Repairs alerting state as soon as a snapshot restore finishes, before any monitor runs. Called from AlertingPlugin.onRestore.
 *
 * Runs each repair in the background, so the restore is not held up. Each repair checks whether the restore touched its state
 * and changes nothing otherwise, so a second call for the same restore is a no-op.
 *
 * [client] must be the plugin client, so the repairs run as this plugin's identity, which can only change alerting's own system
 * indices.
 */
class AlertingRestoreReconciler(client: Client, clusterService: ClusterService) {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    private val writeAliasRepair = WriteAliasRepair(client, clusterService)

    fun onRestore(context: RestoreContext) {
        scope.launch { writeAliasRepair.repair(context) }
    }
}
