/*
* Copyright OpenSearch Contributors
* SPDX-License-Identifier: Apache-2.0
*/

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.get.GetIndexRequest
import org.opensearch.action.admin.indices.get.GetIndexResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.MonitorMetadataService
import org.opensearch.alerting.action.MonitorExplainAction
import org.opensearch.alerting.action.MonitorExplainRequest
import org.opensearch.alerting.action.MonitorExplainResponse
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.io.IOException

private val log = LogManager.getLogger(TransportMonitorExplainAction::class.java)

class TransportMonitorExplainAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<MonitorExplainRequest, MonitorExplainResponse>(
    MonitorExplainAction.NAME, transportService, actionFilters, ::MonitorExplainRequest
),
    SecureTransportAction {

    @Volatile
    override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    @Volatile
    private var allowList = ALLOW_LIST.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) { allowList = it }
    }

    override fun doExecute(
        task: Task,
        monitorExplainRequest: MonitorExplainRequest,
        actionListener: ActionListener<MonitorExplainResponse>
    ) {
        val user: User? = readUserFromThreadContext(client)

        val validateBackendRoleMessage: Boolean = validateUserBackendRoles(user, actionListener)
        if (!validateBackendRoleMessage) {
            actionListener.onFailure(
                OpenSearchStatusException(
                    "Do not have permissions to resource",
                    RestStatus.FORBIDDEN
                )
            )
            return
        }

        client.threadPool().threadContext.stashContext().use {
            GlobalScope.launch(Dispatchers.IO + CoroutineName("MonitorExplainAction")) {
                MonitorExplainHandler(client, actionListener, monitorExplainRequest.monitorId, monitorExplainRequest.docDiff, user).start()
            }
        }
    }
    inner class MonitorExplainHandler(
        private val client: Client,
        private val actionListener: ActionListener<MonitorExplainResponse>,
        private val monitorId: String,
        private val docDiffRequested: Boolean,
        private val user: User?
    ) {
        suspend fun start() {
            try {
                val monitor = getMonitor()

                if (!checkUserPermissionsWithResource(
                        user,
                        monitor.user,
                        actionListener,
                        "monitor",
                        monitor.id
                    )
                ) {
                    return
                }

                if (monitor.monitorType == Monitor.MonitorType.DOC_LEVEL_MONITOR) {
                    log.debug("Doc-level monitor hit.")

                    val docLevelMonitorInput = monitor.inputs[0] as DocLevelMonitorInput
                    val index = docLevelMonitorInput.indices[0]

                    val getIndexRequest = GetIndexRequest().indices(index)
                    val getIndexResponse: GetIndexResponse = client.suspendUntil {
                        client.admin().indices().getIndex(getIndexRequest, it)
                    }
                    val indices = getIndexResponse.indices()

                    val monitorMetadata = MonitorMetadataService.getMetadata(monitor)
                        ?: throw IllegalStateException("Monitor created without metadata not explainable!")

                    val lastRunContext = if (monitorMetadata.lastRunContext.isEmpty()) mutableMapOf()
                    else monitorMetadata.lastRunContext.toMutableMap() as MutableMap<String, MutableMap<String, Any>>

                    var seqNoDiffAmount: Long = 0
                    var docDiffAmount: Long = 0

                    indices.forEach { indexName ->
                        val indexLastRunContext = lastRunContext.getOrPut(indexName) {
                            MonitorMetadataService.createRunContextForIndex(indexName)
                        }

                        val indexUpdatedRunContext = MonitorMetadataService.createRunContextForIndex(
                            indexName
                        )

                        val count: Int = indexUpdatedRunContext["shards_count"] as Int
                        for (i: Int in 0 until count) {
                            val shard = i.toString()

                            val maxSeqNo: Long = indexUpdatedRunContext[shard].toString().toLong()
                            val prevSeqNo: Long = indexLastRunContext[shard].toString().toLong()

                            seqNoDiffAmount += maxSeqNo - prevSeqNo

                            if (docDiffRequested) {
                                val boolQueryBuilder = BoolQueryBuilder()
                                boolQueryBuilder.filter(QueryBuilders.rangeQuery("_seq_no").gt(prevSeqNo).lte(maxSeqNo))

                                val request: SearchRequest = SearchRequest()
                                    .indices(indexName)
                                    .preference("_shards:$shard")
                                    .source(
                                        SearchSourceBuilder()
                                            .query(boolQueryBuilder)
                                            .size(0)
                                    )

                                val response: SearchResponse = client.suspendUntil { client.search(request, it) }
                                if (response.status() !== RestStatus.OK) {
                                    throw IOException("Failed to search shard: $shard")
                                }

                                docDiffAmount += response.hits.totalHits?.value ?: 0
                            }
                        }
                    }
                    val docDiffResult: Long? = if (docDiffRequested) docDiffAmount else null
                    actionListener.onResponse(MonitorExplainResponse(monitorId, seqNoDiffAmount, docDiffResult))
                } else {
                    actionListener.onFailure(
                        AlertingException("Not allowed to explain this type of monitor!", RestStatus.FORBIDDEN, IllegalStateException())
                    )
                }
            } catch (e: Exception) {
                actionListener.onFailure(AlertingException.wrap(e))
            }
        }
        private suspend fun getMonitor(): Monitor {
            val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, monitorId)

            val getResponse: GetResponse = client.suspendUntil { get(getRequest, it) }
            if (!getResponse.isExists) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException("Monitor with $monitorId is not found", RestStatus.NOT_FOUND)
                    )
                )
            }
            val xcp = XContentHelper.createParser(
                xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                getResponse.sourceAsBytesRef, XContentType.JSON
            )
            return ScheduledJob.parse(xcp, getResponse.id, getResponse.version) as Monitor
        }
    }
}
