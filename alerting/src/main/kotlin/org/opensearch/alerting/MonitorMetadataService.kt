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
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.DocWriteResponse
import org.opensearch.action.admin.indices.get.GetIndexRequest
import org.opensearch.action.admin.indices.get.GetIndexResponse
import org.opensearch.action.admin.indices.stats.IndicesStatsAction
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.model.MonitorMetadata
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.rest.RestStatus
import org.opensearch.transport.RemoteTransportException

private val log = LogManager.getLogger(MonitorMetadataService::class.java)

object MonitorMetadataService :
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("MonitorMetadataService")) {

    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var clusterService: ClusterService
    private lateinit var settings: Settings

    @Volatile private lateinit var indexTimeout: TimeValue

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
        this.indexTimeout = AlertingSettings.INDEX_TIMEOUT.get(settings)
        this.clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.INDEX_TIMEOUT) { indexTimeout = it }
    }

    @Suppress("ComplexMethod", "ReturnCount")
    suspend fun upsertMetadata(metadata: MonitorMetadata, updating: Boolean): MonitorMetadata {
        try {
            val indexRequest = IndexRequest(ScheduledJob.SCHEDULED_JOBS_INDEX)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
                .id(metadata.id)
                .routing(metadata.monitorId)
                .setIfSeqNo(metadata.seqNo)
                .setIfPrimaryTerm(metadata.primaryTerm)
                .timeout(indexTimeout)

            if (updating) {
                indexRequest.id(metadata.id).setIfSeqNo(metadata.seqNo).setIfPrimaryTerm(metadata.primaryTerm)
            } else {
                indexRequest.opType(DocWriteRequest.OpType.CREATE)
            }
            val response: IndexResponse = client.suspendUntil { index(indexRequest, it) }
            when (response.result) {
                DocWriteResponse.Result.DELETED, DocWriteResponse.Result.NOOP, DocWriteResponse.Result.NOT_FOUND, null -> {
                    val failureReason = "The upsert metadata call failed with a ${response.result?.lowercase} result"
                    log.error(failureReason)
                    throw AlertingException(failureReason, RestStatus.INTERNAL_SERVER_ERROR, IllegalStateException(failureReason))
                }
                DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED -> {
                    log.debug("Successfully upserted MonitorMetadata:${metadata.id} ")
                }
            }
            return metadata.copy(
                seqNo = response.seqNo,
                primaryTerm = response.primaryTerm
            )
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
        workflowId: String? = null
    ): Pair<MonitorMetadata, Boolean> {
        try {
            val created = true
            val metadata = getMetadata(monitor, workflowId)
            return if (metadata != null) {
                metadata to !created
            } else {
                val newMetadata = createNewMetadata(monitor, createWithRunContext = createWithRunContext, workflowId)
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

    suspend fun getMetadata(monitor: Monitor, workflowId: String? = null): MonitorMetadata? {
        try {
            val metadataId = MonitorMetadata.getId(monitor, workflowId)
            val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, metadataId).routing(monitor.id)

            val getResponse: GetResponse = client.suspendUntil { get(getRequest, it) }
            return if (getResponse.isExists) {
                val xcp = XContentHelper.createParser(
                    xContentRegistry,
                    LoggingDeprecationHandler.INSTANCE,
                    getResponse.sourceAsBytesRef,
                    XContentType.JSON
                )
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                MonitorMetadata.parse(xcp)
            } else {
                null
            }
        } catch (e: Exception) {
            if (e.message?.contains("no such index") == true) {
                return null
            } else {
                throw AlertingException.wrap(e)
            }
        }
    }

    suspend fun recreateRunContext(metadata: MonitorMetadata, monitor: Monitor): MonitorMetadata {
        try {
            val monitorIndex = if (monitor.monitorType == Monitor.MonitorType.DOC_LEVEL_MONITOR) {
                (monitor.inputs[0] as DocLevelMonitorInput).indices[0]
            } else null
            val runContext = if (monitor.monitorType == Monitor.MonitorType.DOC_LEVEL_MONITOR) {
                createFullRunContext(monitorIndex, metadata.lastRunContext as MutableMap<String, MutableMap<String, Any>>)
            } else null
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

    private suspend fun createNewMetadata(monitor: Monitor, createWithRunContext: Boolean, workflowId: String? = null): MonitorMetadata {
        val monitorIndex = if (monitor.monitorType == Monitor.MonitorType.DOC_LEVEL_MONITOR) {
            (monitor.inputs[0] as DocLevelMonitorInput).indices[0]
        } else null
        val runContext =
            if (monitor.monitorType == Monitor.MonitorType.DOC_LEVEL_MONITOR && createWithRunContext) {
                createFullRunContext(monitorIndex)
            } else emptyMap()
        return MonitorMetadata(
            id = MonitorMetadata.getId(monitor, workflowId),
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
        val lastRunContext = existingRunContext?.toMutableMap() ?: mutableMapOf()
        try {
            if (index == null) return mutableMapOf()
            val getIndexRequest = GetIndexRequest().indices(index)
            val getIndexResponse: GetIndexResponse = client.suspendUntil {
                client.admin().indices().getIndex(getIndexRequest, it)
            }
            val indices = getIndexResponse.indices()

            indices.forEach { indexName ->
                if (!lastRunContext.containsKey(indexName)) {
                    lastRunContext[indexName] = createRunContextForIndex(index)
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
