/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.admin.indices.get.GetIndexRequest
import org.opensearch.action.admin.indices.get.GetIndexResponse
import org.opensearch.action.admin.indices.stats.IndicesStatsAction
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.await
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorMetadata
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.remote.monitors.RemoteDocLevelMonitorInput
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.remote.metadata.client.GetDataObjectRequest
import org.opensearch.remote.metadata.client.PutDataObjectRequest
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.client.Client

private val log = LogManager.getLogger(MonitorMetadataService::class.java)

object MonitorMetadataService :
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("MonitorMetadataService")) {

    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var clusterService: ClusterService
    private lateinit var settings: Settings
    private lateinit var sdkClient: SdkClient

    @Volatile
    private lateinit var indexTimeout: TimeValue

    fun initialize(
        client: Client,
        clusterService: ClusterService,
        xContentRegistry: NamedXContentRegistry,
        settings: Settings,
        sdkClient: SdkClient
    ) {
        this.clusterService = clusterService
        this.client = client
        this.xContentRegistry = xContentRegistry
        this.settings = settings
        this.sdkClient = sdkClient
        this.indexTimeout = AlertingSettings.INDEX_TIMEOUT.get(settings)
        this.clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.INDEX_TIMEOUT) { indexTimeout = it }
    }

    private fun getTenantId(): String? =
        client.threadPool().threadContext.getHeader(AlertingPlugin.TENANT_ID_HEADER)

    @Suppress("ComplexMethod", "ReturnCount")
    suspend fun upsertMetadata(metadata: MonitorMetadata, updating: Boolean): MonitorMetadata {
        try {
            if (clusterService.state().routingTable.hasIndex(ScheduledJob.SCHEDULED_JOBS_INDEX)) {
                val metadataObj = ToXContentObject { builder, _ ->
                    metadata.toXContent(builder, ToXContent.MapParams(mapOf("with_type" to "true")))
                }
                val putRequestBuilder = PutDataObjectRequest.builder()
                    .index(ScheduledJob.SCHEDULED_JOBS_INDEX)
                    .id(metadata.id)
                    .tenantId(getTenantId())
                    .dataObject(metadataObj)

                if (updating) {
                    putRequestBuilder.ifSeqNo(metadata.seqNo).ifPrimaryTerm(metadata.primaryTerm)
                        .overwriteIfExists(true)
                } else {
                    putRequestBuilder.overwriteIfExists(false)
                }

                val putResponse = sdkClient.putDataObjectAsync(putRequestBuilder.build()).await()
                if (putResponse.isFailed) {
                    val failureReason = "The upsert metadata call failed: ${putResponse.cause()?.message}"
                    log.error(failureReason)
                    throw AlertingException(
                        failureReason,
                        putResponse.status() ?: RestStatus.INTERNAL_SERVER_ERROR,
                        putResponse.cause() ?: IllegalStateException(failureReason)
                    )
                }
                log.debug("Successfully upserted MonitorMetadata:${metadata.id} ")
                return metadata.copy(
                    seqNo = putResponse.indexResponse()?.seqNo ?: SequenceNumbers.UNASSIGNED_SEQ_NO,
                    primaryTerm = putResponse.indexResponse()?.primaryTerm ?: SequenceNumbers.UNASSIGNED_PRIMARY_TERM
                )
            } else {
                val failureReason = "Job index ${ScheduledJob.SCHEDULED_JOBS_INDEX} does not exist to update monitor metadata"
                throw OpenSearchStatusException(failureReason, RestStatus.INTERNAL_SERVER_ERROR)
            }
        } catch (e: Exception) {
            throw AlertingException.wrap(e)
        }
    }

    /**
     * Document monitors are keeping the context of the last run.
     * Since one monitor can be part of multiple workflows we need to be sure that execution of the current workflow
     * doesn't interfere with the other workflows that are dependent on the given monitor
     */
    suspend fun getOrCreateMetadata(
        monitor: Monitor,
        createWithRunContext: Boolean = true,
        skipIndex: Boolean = false,
        workflowMetadataId: String? = null,
        forceCreateLastRunContext: Boolean = false
    ): Pair<MonitorMetadata, Boolean> {
        try {
            val created = true
            var metadata = getMetadata(monitor, workflowMetadataId)
            if (forceCreateLastRunContext) {
                metadata = metadata?.copy(lastRunContext = createUpdatedRunContext(monitor))
            }
            return if (metadata != null) {
                metadata to !created
            } else {
                val newMetadata = createNewMetadata(monitor, createWithRunContext = createWithRunContext, workflowMetadataId)
                if (skipIndex) {
                    newMetadata to created
                } else {
                    upsertMetadata(newMetadata, updating = false) to created
                }
            }
        } catch (e: Exception) {
            throw AlertingException.wrap(e)
        }
    }

    private suspend fun createUpdatedRunContext(
        monitor: Monitor
    ): Map<String, MutableMap<String, Any>> {
        val monitorIndex = if (monitor.monitorType == Monitor.MonitorType.DOC_LEVEL_MONITOR.value)
            (monitor.inputs[0] as DocLevelMonitorInput).indices[0]
        else if (monitor.monitorType.endsWith(Monitor.MonitorType.DOC_LEVEL_MONITOR.value))
            (monitor.inputs[0] as RemoteDocLevelMonitorInput).docLevelMonitorInput.indices[0]
        else null
        val runContext = if (monitor.monitorType.endsWith(Monitor.MonitorType.DOC_LEVEL_MONITOR.value))
            createFullRunContext(monitorIndex)
        else emptyMap()
        return runContext
    }

    suspend fun getMetadata(monitor: Monitor, workflowMetadataId: String? = null): MonitorMetadata? {
        try {
            val metadataId = MonitorMetadata.getId(monitor, workflowMetadataId)
            val getRequest = GetDataObjectRequest.builder()
                .index(ScheduledJob.SCHEDULED_JOBS_INDEX)
                .id(metadataId)
                .tenantId(getTenantId())
                .build()

            val response = sdkClient.getDataObjectAsync(getRequest).await()
            val getResponse = response.getResponse()
            return if (getResponse != null && getResponse.isExists) {
                val xcp = XContentHelper.createParser(
                    xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    getResponse.sourceAsBytesRef, XContentType.JSON
                )
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                MonitorMetadata.parse(xcp, getResponse.id, getResponse.seqNo, getResponse.primaryTerm)
            } else {
                null
            }
        } catch (e: Exception) {
            if (AlertingException.isIndexNotFoundException(e)) {
                return null
            } else {
                throw AlertingException.wrap(e)
            }
        }
    }

    suspend fun recreateRunContext(metadata: MonitorMetadata, monitor: Monitor): MonitorMetadata {
        try {
            val monitorIndex = if (monitor.monitorType == Monitor.MonitorType.DOC_LEVEL_MONITOR.value)
                (monitor.inputs[0] as DocLevelMonitorInput).indices[0]
            else if (monitor.monitorType.endsWith(Monitor.MonitorType.DOC_LEVEL_MONITOR.value))
                (monitor.inputs[0] as RemoteDocLevelMonitorInput).docLevelMonitorInput.indices[0]
            else null
            val runContext = if (monitor.monitorType.endsWith(Monitor.MonitorType.DOC_LEVEL_MONITOR.value))
                createFullRunContext(monitorIndex, metadata.lastRunContext as MutableMap<String, MutableMap<String, Any>>)
            else null
            return if (runContext != null) {
                metadata.copy(
                    lastRunContext = runContext
                )
            } else {
                metadata
            }
        } catch (e: Exception) {
            throw AlertingException.wrap(e)
        }
    }

    private suspend fun createNewMetadata(
        monitor: Monitor,
        createWithRunContext: Boolean,
        workflowMetadataId: String? = null,
    ): MonitorMetadata {
        val monitorIndex = if (monitor.monitorType == Monitor.MonitorType.DOC_LEVEL_MONITOR.value)
            (monitor.inputs[0] as DocLevelMonitorInput).indices[0]
        else if (monitor.monitorType.endsWith(Monitor.MonitorType.DOC_LEVEL_MONITOR.value))
            (monitor.inputs[0] as RemoteDocLevelMonitorInput).docLevelMonitorInput.indices[0]
        else null
        val runContext = if (monitor.monitorType.endsWith(Monitor.MonitorType.DOC_LEVEL_MONITOR.value))
            createFullRunContext(monitorIndex)
        else emptyMap()
        return MonitorMetadata(
            id = MonitorMetadata.getId(monitor, workflowMetadataId),
            seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            monitorId = monitor.id,
            lastActionExecutionTimes = emptyList(),
            lastRunContext = runContext,
            sourceToQueryIndexMapping = mutableMapOf()
        )
    }

    suspend fun createFullRunContext(
        index: String?,
        existingRunContext: MutableMap<String, MutableMap<String, Any>>? = null,
    ): MutableMap<String, MutableMap<String, Any>> {
        val lastRunContext = existingRunContext?.toMutableMap() ?: mutableMapOf()
        try {
            if (index == null) return mutableMapOf()

            val indices = mutableListOf<String>()
            if (IndexUtils.isAlias(index, clusterService.state()) ||
                IndexUtils.isDataStream(index, clusterService.state())
            ) {
                IndexUtils.getWriteIndex(index, clusterService.state())?.let { indices.add(it) }
            } else {
                val getIndexRequest = GetIndexRequest().indices(index)
                val getIndexResponse: GetIndexResponse = client.suspendUntil {
                    client.admin().indices().getIndex(getIndexRequest, it)
                }
                indices.addAll(getIndexResponse.indices())
            }

            indices.forEach { indexName ->
                if (!lastRunContext.containsKey(indexName)) {
                    lastRunContext[indexName] = createRunContextForIndex(indexName)
                }
            }
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            throw AlertingException("Failed fetching index stats", RestStatus.INTERNAL_SERVER_ERROR, unwrappedException)
        } catch (e: OpenSearchSecurityException) {
            throw AlertingException(
                "Failed fetching index stats - missing required index permissions: ${e.localizedMessage}",
                RestStatus.INTERNAL_SERVER_ERROR,
                e
            )
        } catch (e: Exception) {
            throw AlertingException("Failed fetching index stats", RestStatus.INTERNAL_SERVER_ERROR, e)
        }
        return lastRunContext
    }

    suspend fun createRunContextForIndex(index: String, createdRecently: Boolean = false): MutableMap<String, Any> {
        val request = IndicesStatsRequest().indices(index).clear()
        val response: IndicesStatsResponse = client.suspendUntil { execute(IndicesStatsAction.INSTANCE, request, it) }
        if (response.status != RestStatus.OK) {
            val errorMessage = "Failed fetching index stats for index:$index"
            throw AlertingException(errorMessage, RestStatus.INTERNAL_SERVER_ERROR, IllegalStateException(errorMessage))
        }
        val shards = response.shards.filter { it.shardRouting.primary() && it.shardRouting.active() }
        val lastRunContext = HashMap<String, Any>()
        lastRunContext["index"] = index
        val count = shards.size
        lastRunContext["shards_count"] = count

        for (shard in shards) {
            lastRunContext[shard.shardRouting.id.toString()] =
                if (createdRecently) -1L
                else shard.seqNoStats?.globalCheckpoint ?: SequenceNumbers.UNASSIGNED_SEQ_NO
        }
        return lastRunContext
    }
}
