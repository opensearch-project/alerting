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
import org.apache.lucene.search.join.ScoreMode
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.alerting.MonitorMetadataService
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.DeleteMonitorRequest
import org.opensearch.commons.alerting.action.DeleteMonitorResponse
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.DeleteByQueryAction
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

private val log = LogManager.getLogger(TransportDeleteMonitorAction::class.java)

class TransportDeleteMonitorAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<ActionRequest, DeleteMonitorResponse>(
    AlertingActions.DELETE_MONITOR_ACTION_NAME,
    transportService,
    actionFilters,
    ::DeleteMonitorRequest
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: ActionRequest, actionListener: ActionListener<DeleteMonitorResponse>) {
        val transformedRequest = request as? DeleteMonitorRequest
            ?: recreateObject(request) { DeleteMonitorRequest(it) }
        val user = readUserFromThreadContext(client)
        val deleteRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, transformedRequest.monitorId)
            .setRefreshPolicy(transformedRequest.refreshPolicy)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }

        GlobalScope.launch(Dispatchers.IO + CoroutineName("DeleteMonitorAction")) {
            DeleteMonitorHandler(client, actionListener, deleteRequest, user, transformedRequest.monitorId).resolveUserAndStart()
        }
    }

    inner class DeleteMonitorHandler(
        private val client: Client,
        private val actionListener: ActionListener<DeleteMonitorResponse>,
        private val deleteRequest: DeleteRequest,
        private val user: User?,
        private val monitorId: String
    ) {
        suspend fun resolveUserAndStart() {
            try {
                val monitor = getMonitor()

                val canDelete = user == null || !doFilterForUser(user) ||
                    checkUserPermissionsWithResource(user, monitor.user, actionListener, "monitor", monitorId)

                if (monitorIsWorkflowDelegate(monitor.id)) {
                    actionListener.onFailure(
                        AlertingException("Monitor can't be deleted because it is a part of workflow(s)", RestStatus.FORBIDDEN, IllegalStateException())
                    )
                } else if (canDelete) {
                    val deleteResponse = deleteMonitor(monitor)
                    deleteDocLevelMonitorQueriesAndIndices(monitor)
                    deleteMetadata(monitor)
                    actionListener.onResponse(DeleteMonitorResponse(deleteResponse.id, deleteResponse.version))
                } else {
                    actionListener.onFailure(
                        AlertingException("Not allowed to delete this monitor!", RestStatus.FORBIDDEN, IllegalStateException())
                    )
                }
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        /**
         * Checks if the monitor is part of the workflow
         *
         * @param monitorId id of monitor that is checked if it is a workflow delegate
         */
        private suspend fun monitorIsWorkflowDelegate(monitorId: String): Boolean {
            val queryBuilder = QueryBuilders.nestedQuery(
                Workflow.WORKFLOW_DELEGATE_PATH,
                QueryBuilders.boolQuery().must(
                    QueryBuilders.matchQuery(
                        Workflow.WORKFLOW_MONITOR_PATH,
                        monitorId
                    )
                ),
                ScoreMode.None
            )
            try {
                val searchRequest = SearchRequest()
                    .indices(ScheduledJob.SCHEDULED_JOBS_INDEX)
                    .source(SearchSourceBuilder().query(queryBuilder))

                client.threadPool().threadContext.stashContext().use {
                    val searchResponse: SearchResponse = client.suspendUntil { search(searchRequest, it) }
                    if (searchResponse.hits.totalHits?.value == 0L) {
                        return false
                    }

                    val workflowIds = searchResponse.hits.hits.map { it.id }.joinToString()
                    log.info("Monitor $monitorId can't be deleted since it belongs to $workflowIds")
                    return true
                }
            } catch (ex: Exception) {
                log.error("Error getting the monitor workflows", ex)
                throw AlertingException.wrap(ex)
            }
        }

        private suspend fun getMonitor(): Monitor {
            val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, monitorId)

            val getResponse: GetResponse = client.suspendUntil { get(getRequest, it) }
            if (getResponse.isExists == false) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException("Monitor with $monitorId is not found", RestStatus.NOT_FOUND)
                    )
                )
            }
            val xcp = XContentHelper.createParser(
                xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                getResponse.sourceAsBytesRef,
                XContentType.JSON
            )
            return ScheduledJob.parse(xcp, getResponse.id, getResponse.version) as Monitor
        }

        private suspend fun deleteMonitor(monitor: Monitor): DeleteResponse {
            return client.suspendUntil { delete(deleteRequest, it) }
        }

        private suspend fun deleteMetadata(monitor: Monitor) {
            val deleteRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, "${monitor.id}-metadata")
            val deleteResponse: DeleteResponse = client.suspendUntil { delete(deleteRequest, it) }
        }

        private suspend fun deleteDocLevelMonitorQueriesAndIndices(monitor: Monitor) {
            val clusterState = clusterService.state()
            val metadata = MonitorMetadataService.getMetadata(monitor)
            metadata?.sourceToQueryIndexMapping?.forEach { (_, queryIndex) ->

                val indicesExistsResponse: IndicesExistsResponse =
                    client.suspendUntil {
                        client.admin().indices().exists(IndicesExistsRequest(queryIndex), it)
                    }
                if (indicesExistsResponse.isExists == false) {
                    return
                }
                // Check if there's any queries from other monitors in this queryIndex,
                // to avoid unnecessary doc deletion, if we could just delete index completely
                val searchResponse: SearchResponse = client.suspendUntil {
                    search(
                        SearchRequest(queryIndex).source(
                            SearchSourceBuilder()
                                .size(0)
                                .query(
                                    QueryBuilders.boolQuery().mustNot(
                                        QueryBuilders.matchQuery("monitor_id", monitorId)
                                    )
                                )
                        ).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN),
                        it
                    )
                }
                if (searchResponse.hits.totalHits.value == 0L) {
                    val ack: AcknowledgedResponse = client.suspendUntil {
                        client.admin().indices().delete(
                            DeleteIndexRequest(queryIndex).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN),
                            it
                        )
                    }
                    if (ack.isAcknowledged == false) {
                        log.error("Deletion of concrete queryIndex:$queryIndex is not ack'd!")
                    }
                } else {
                    // Delete all queries added by this monitor
                    val response: BulkByScrollResponse = suspendCoroutine { cont ->
                        DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                            .source(queryIndex)
                            .filter(QueryBuilders.matchQuery("monitor_id", monitorId))
                            .refresh(true)
                            .execute(
                                object : ActionListener<BulkByScrollResponse> {
                                    override fun onResponse(response: BulkByScrollResponse) = cont.resume(response)
                                    override fun onFailure(t: Exception) = cont.resumeWithException(t)
                                }
                            )
                    }
                }
            }
        }
    }
}
