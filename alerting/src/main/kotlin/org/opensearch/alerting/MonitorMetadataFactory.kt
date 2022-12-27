/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.action.admin.indices.get.GetIndexRequest
import org.opensearch.action.admin.indices.get.GetIndexResponse
import org.opensearch.alerting.model.MonitorMetadata
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.index.seqno.SequenceNumbers

object MonitorMetadataFactory {

    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var clusterService: ClusterService
    private lateinit var settings: Settings

    fun initialize(
        client: Client,
        clusterService: ClusterService,
        xContentRegistry: NamedXContentRegistry,
        settings: Settings
    ) {
        this.clusterService = clusterService
        this.client = client
        this.xContentRegistry = xContentRegistry
        this.settings = settings
    }

    suspend fun newInstance(monitor: Monitor, createWithRunContext: Boolean): MonitorMetadata {
        val monitorIndex = if (monitor.monitorType == Monitor.MonitorType.DOC_LEVEL_MONITOR)
            (monitor.inputs[0] as DocLevelMonitorInput).indices[0]
        else null
        val runContext =
            if (monitor.monitorType == Monitor.MonitorType.DOC_LEVEL_MONITOR && createWithRunContext)
                createFullRunContext(monitorIndex)
            else emptyMap()
        return MonitorMetadata(
            id = "${monitor.id}-metadata",
            seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            monitorId = monitor.id,
            lastActionExecutionTimes = emptyList(),
            lastRunContext = runContext,
            sourceToQueryIndexMapping = mutableMapOf()
        )
    }

    private suspend fun createFullRunContext(
        index: String?,
        existingRunContext: MutableMap<String, MutableMap<String, Any>>? = null
    ): MutableMap<String, MutableMap<String, Any>> {
        if (index == null) return mutableMapOf()
        val getIndexRequest = GetIndexRequest().indices(index)
        val getIndexResponse: GetIndexResponse = client.suspendUntil {
            client.admin().indices().getIndex(getIndexRequest, it)
        }
        val indices = getIndexResponse.indices()
        val lastRunContext = existingRunContext?.toMutableMap() ?: mutableMapOf()
        indices.forEach { indexName ->
            if (!lastRunContext.containsKey(indexName))
                lastRunContext[indexName] = DocumentLevelMonitorRunner.createRunContext(clusterService, client, indexName)
        }
        return lastRunContext
    }
}
