/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.apache.logging.log4j.LogManager
import org.apache.lucene.search.join.ScoreMode
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.alerting.MonitorMetadataService
import org.opensearch.alerting.core.lock.LockModel
import org.opensearch.alerting.core.lock.LockService
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.ScheduledJobUtils.Companion.WORKFLOW_DELEGATE_PATH
import org.opensearch.alerting.util.ScheduledJobUtils.Companion.WORKFLOW_MONITOR_PATH
import org.opensearch.alerting.util.use
import org.opensearch.client.Client
import org.opensearch.commons.alerting.action.DeleteMonitorResponse
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.core.action.ActionListener
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.DeleteByQueryAction
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

/**
 * Component used when deleting the monitors
 */
object DeleteMonitorService :
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("WorkflowMetadataService")) {
    private val log = LogManager.getLogger(this.javaClass)

    private lateinit var client: Client
    private lateinit var lockService: LockService

    fun initialize(
        client: Client,
        lockService: LockService
    ) {
        DeleteMonitorService.client = client
        DeleteMonitorService.lockService = lockService
    }

    /**
     * Deletes the monitor, docLevelQueries and monitor metadata
     * @param monitor monitor to be deleted
     * @param refreshPolicy
     */
    suspend fun deleteMonitor(monitor: Monitor, refreshPolicy: RefreshPolicy): DeleteMonitorResponse {
        val deleteResponse = deleteMonitor(monitor.id, refreshPolicy)
        deleteDocLevelMonitorQueriesAndIndices(monitor)
        deleteMetadata(monitor)
        deleteLock(monitor)
        return DeleteMonitorResponse(deleteResponse.id, deleteResponse.version)
    }

    private suspend fun deleteMonitor(monitorId: String, refreshPolicy: RefreshPolicy): DeleteResponse {
        val deleteMonitorRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, monitorId)
            .setRefreshPolicy(refreshPolicy)
        return client.suspendUntil { delete(deleteMonitorRequest, it) }
    }

    private suspend fun deleteMetadata(monitor: Monitor) {
        val deleteRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, "${monitor.id}-metadata")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        try {
            val deleteResponse: DeleteResponse = client.suspendUntil { delete(deleteRequest, it) }
            log.debug("Monitor metadata: ${deleteResponse.id} deletion result: ${deleteResponse.result}")
        } catch (e: Exception) {
            // we only log the error and don't fail the request because if monitor document has been deleted,
            // we cannot retry based on this failure
            log.error("Failed to delete monitor metadata ${deleteRequest.id()}.", e)
        }
    }

    private suspend fun deleteDocLevelMonitorQueriesAndIndices(monitor: Monitor) {
        try {
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
                                        QueryBuilders.matchQuery("monitor_id", monitor.id)
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
                            .filter(QueryBuilders.matchQuery("monitor_id", monitor.id))
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
        } catch (e: Exception) {
            // we only log the error and don't fail the request because if monitor document has been deleted successfully,
            // we cannot retry based on this failure
            log.error("Failed to delete doc level queries from query index.", e)
        }
    }

    private suspend fun deleteLock(monitor: Monitor) {
        client.suspendUntil<Client, Boolean> { lockService.deleteLock(LockModel.generateLockId(monitor.id), it) }
    }

    /**
     * Checks if the monitor is part of the workflow
     *
     * @param monitorId id of monitor that is checked if it is a workflow delegate
     */
    suspend fun monitorIsWorkflowDelegate(monitorId: String): Boolean {
        val queryBuilder = QueryBuilders.nestedQuery(
            WORKFLOW_DELEGATE_PATH,
            QueryBuilders.boolQuery().must(
                QueryBuilders.matchQuery(
                    WORKFLOW_MONITOR_PATH,
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
}
