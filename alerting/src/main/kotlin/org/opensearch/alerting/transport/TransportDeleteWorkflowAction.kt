/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.apache.lucene.search.join.ScoreMode
import org.opensearch.OpenSearchException
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.alerting.DeleteMonitorService
import org.opensearch.alerting.model.MonitorMetadata
import org.opensearch.alerting.model.WorkflowMetadata
import org.opensearch.alerting.opensearchapi.addFilter
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
import org.opensearch.commons.alerting.action.DeleteWorkflowRequest
import org.opensearch.commons.alerting.action.DeleteWorkflowResponse
import org.opensearch.commons.alerting.model.CompositeInput
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.DeleteByQueryAction
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
/**
 * Transport class that deletes the workflow.
 * If the deleteDelegateMonitor flag is set to true, deletes the workflow delegates that are not part of another workflow
 */
class TransportDeleteWorkflowAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
) : HandledTransportAction<ActionRequest, DeleteWorkflowResponse>(
    AlertingActions.DELETE_WORKFLOW_ACTION_NAME, transportService, actionFilters, ::DeleteWorkflowRequest
),
    SecureTransportAction {

    private val log = LogManager.getLogger(javaClass)

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: ActionRequest, actionListener: ActionListener<DeleteWorkflowResponse>) {
        val transformedRequest = request as? DeleteWorkflowRequest
            ?: recreateObject(request) { DeleteWorkflowRequest(it) }

        val user = readUserFromThreadContext(client)
        val deleteRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, transformedRequest.workflowId)
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }

        scope.launch {
            DeleteWorkflowHandler(
                client,
                actionListener,
                deleteRequest,
                transformedRequest.deleteDelegateMonitors,
                user,
                transformedRequest.workflowId
            ).resolveUserAndStart()
        }
    }

    inner class DeleteWorkflowHandler(
        private val client: Client,
        private val actionListener: ActionListener<DeleteWorkflowResponse>,
        private val deleteRequest: DeleteRequest,
        private val deleteDelegateMonitors: Boolean?,
        private val user: User?,
        private val workflowId: String,
    ) {
        suspend fun resolveUserAndStart() {
            try {
                val workflow = getWorkflow()

                val canDelete = user == null ||
                    !doFilterForUser(user) ||
                    checkUserPermissionsWithResource(
                        user,
                        workflow.user,
                        actionListener,
                        "workflow",
                        workflowId
                    )

                if (canDelete) {
                    val delegateMonitorIds = (workflow.inputs[0] as CompositeInput).getMonitorIds()
                    var deletableMonitors = listOf<Monitor>()
                    // User can only delete the delegate monitors only in the case if all monitors can be deleted
                    // Partial monitor deletion is not available
                    if (deleteDelegateMonitors == true) {
                        deletableMonitors = getDeletableDelegates(workflowId, delegateMonitorIds, user)
                        val monitorsDiff = delegateMonitorIds.toMutableList()
                        monitorsDiff.removeAll(deletableMonitors.map { it.id })

                        if (monitorsDiff.isNotEmpty()) {
                            actionListener.onFailure(
                                AlertingException(
                                    "Not allowed to delete ${monitorsDiff.joinToString()} monitors",
                                    RestStatus.FORBIDDEN,
                                    IllegalStateException()
                                )
                            )
                            return
                        }
                    }

                    val deleteResponse = deleteWorkflow(deleteRequest)
                    var deleteWorkflowResponse = DeleteWorkflowResponse(deleteResponse.id, deleteResponse.version)

                    val workflowMetadataId = WorkflowMetadata.getId(workflow.id)

                    val metadataIdsToDelete = mutableListOf(workflowMetadataId)

                    if (deleteDelegateMonitors == true) {
                        val failedMonitorIds = tryDeletingMonitors(deletableMonitors, RefreshPolicy.IMMEDIATE)
                        // Update delete workflow response
                        deleteWorkflowResponse.nonDeletedMonitors = failedMonitorIds
                        // Delete monitors workflow metadata
                        // Monitor metadata will be in monitorId-workflowId-metadata format
                        metadataIdsToDelete.addAll(deletableMonitors.map { MonitorMetadata.getId(it, workflowMetadataId) })
                    }
                    // Delete the monitors workflow metadata
                    val deleteMonitorWorkflowMetadataResponse: BulkByScrollResponse = client.suspendUntil {
                        DeleteByQueryRequestBuilder(this, DeleteByQueryAction.INSTANCE)
                            .source(ScheduledJob.SCHEDULED_JOBS_INDEX)
                            .filter(QueryBuilders.idsQuery().addIds(*metadataIdsToDelete.toTypedArray()))
                            .execute(it)
                    }

                    if (deleteMonitorWorkflowMetadataResponse.isTimedOut) {
                        actionListener.onFailure(
                            AlertingException.wrap(
                                OpenSearchException("Cannot delete monitors workflow metadata due to timeout.")
                            )
                        )
                        return
                    }
                    actionListener.onResponse(deleteWorkflowResponse)
                } else {
                    actionListener.onFailure(
                        AlertingException(
                            "Not allowed to delete this workflow!",
                            RestStatus.FORBIDDEN,
                            IllegalStateException()
                        )
                    )
                }
            } catch (t: Exception) {
                if (t is IndexNotFoundException) {
                    actionListener.onFailure(
                        OpenSearchStatusException(
                            "Workflow not found.",
                            RestStatus.NOT_FOUND
                        )
                    )
                } else {
                    actionListener.onFailure(AlertingException.wrap(t))
                }
            }
        }

        /**
         * Tries to delete the given list of the monitors. Return value contains all the monitorIds which deletion failed
         * @param monitorIds list of monitor ids to be deleted
         * @param refreshPolicy
         * @return list of the monitors that were not deleted
         */
        private suspend fun tryDeletingMonitors(monitors: List<Monitor>, refreshPolicy: RefreshPolicy): List<String> {
            val nonDeletedMonitorIds = mutableListOf<String>()

            for (monitor in monitors) {
                val monitorId = monitor.id
                try {
                    val monitorIsWorkflowDelegate = DeleteMonitorService.monitorIsWorkflowDelegate(monitorId)
                    if (monitorIsWorkflowDelegate) {
                        nonDeletedMonitorIds.add(monitorId)
                    }
                    DeleteMonitorService.deleteMonitor(monitor, refreshPolicy)
                } catch (ex: Exception) {
                    nonDeletedMonitorIds.add(monitorId)
                }
            }
            return nonDeletedMonitorIds
        }

        /**
         * Returns lit of monitor ids belonging only to a given workflow
         * @param workflowIdToBeDeleted Id of the workflow that should be deleted
         * @param monitorIds List of delegate monitor ids (underlying monitor ids)
         */
        private suspend fun getDeletableDelegates(workflowIdToBeDeleted: String, monitorIds: List<String>, user: User?): List<Monitor> {
            // Retrieve monitors belonging to another workflows
            val queryBuilder = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("_id", workflowIdToBeDeleted)).filter(
                QueryBuilders.nestedQuery(
                    Workflow.WORKFLOW_DELEGATE_PATH,
                    QueryBuilders.boolQuery().must(
                        QueryBuilders.termsQuery(
                            Workflow.WORKFLOW_MONITOR_PATH,
                            monitorIds
                        )
                    ),
                    ScoreMode.None
                )
            )

            val searchRequest = SearchRequest()
                .indices(ScheduledJob.SCHEDULED_JOBS_INDEX)
                .source(SearchSourceBuilder().query(queryBuilder))

            val searchResponse: SearchResponse = client.suspendUntil { search(searchRequest, it) }

            val workflows = searchResponse.hits.hits.map { hit ->
                val xcp = XContentHelper.createParser(
                    xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    hit.sourceRef, XContentType.JSON
                ).also { it.nextToken() }
                lateinit var workflow: Workflow
                while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                    xcp.nextToken()
                    when (xcp.currentName()) {
                        "workflow" -> workflow = Workflow.parse(xcp)
                    }
                }
                workflow.copy(id = hit.id, version = hit.version)
            }
            val workflowMonitors = workflows.filter { it.id != workflowIdToBeDeleted }.flatMap { (it.inputs[0] as CompositeInput).getMonitorIds() }.distinct()
            // Monitors that can be deleted -> all workflow delegates - monitors belonging to different workflows
            val deletableMonitorIds = monitorIds.minus(workflowMonitors.toSet())

            val query = QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery("_id", deletableMonitorIds))
            val searchSource = SearchSourceBuilder().query(query)
            val monitorSearchRequest = SearchRequest(ScheduledJob.SCHEDULED_JOBS_INDEX).source(searchSource)

            if (user != null && filterByEnabled) {
                addFilter(user, monitorSearchRequest.source(), "monitor.user.backend_roles.keyword")
            }

            val searchMonitorResponse: SearchResponse = client.suspendUntil { search(monitorSearchRequest, it) }
            if (searchMonitorResponse.isTimedOut) {
                throw OpenSearchException("Cannot determine that the ${ScheduledJob.SCHEDULED_JOBS_INDEX} index is healthy")
            }
            val deletableMonitors = mutableListOf<Monitor>()
            for (hit in searchMonitorResponse.hits) {
                XContentType.JSON.xContent().createParser(
                    xContentRegistry,
                    LoggingDeprecationHandler.INSTANCE, hit.sourceAsString
                ).use { hitsParser ->
                    val monitor = ScheduledJob.parse(hitsParser, hit.id, hit.version) as Monitor
                    deletableMonitors.add(monitor)
                }
            }

            return deletableMonitors
        }

        private suspend fun getWorkflow(): Workflow {
            val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, workflowId)

            val getResponse: GetResponse = client.suspendUntil { get(getRequest, it) }
            if (getResponse.isExists == false) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException("Workflow not found.", RestStatus.NOT_FOUND)
                    )
                )
            }
            val xcp = XContentHelper.createParser(
                xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                getResponse.sourceAsBytesRef, XContentType.JSON
            )
            return ScheduledJob.parse(xcp, getResponse.id, getResponse.version) as Workflow
        }

        private suspend fun deleteWorkflow(deleteRequest: DeleteRequest): DeleteResponse {
            log.debug("Deleting the workflow with id ${deleteRequest.id()}")
            return client.suspendUntil { delete(deleteRequest, it) }
        }

        private suspend fun deleteWorkflowMetadata(workflow: Workflow) {
            val deleteRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, WorkflowMetadata.getId(workflow.id))
            val deleteResponse: DeleteResponse = client.suspendUntil { delete(deleteRequest, it) }
        }
    }
}
