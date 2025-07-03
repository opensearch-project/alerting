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
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.ScheduledJobUtils.Companion.WORKFLOW_DELEGATE_PATH
import org.opensearch.alerting.util.ScheduledJobUtils.Companion.WORKFLOW_MONITOR_PATH
import org.opensearch.alerting.util.use
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.GetMonitorRequest
import org.opensearch.commons.alerting.action.GetMonitorResponse
import org.opensearch.commons.alerting.action.GetMonitorResponse.AssociatedWorkflow
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportGetMonitorAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportGetMonitorAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    val clusterService: ClusterService,
    settings: Settings,
) : HandledTransportAction<ActionRequest, GetMonitorResponse>(
    AlertingActions.GET_MONITOR_ACTION_NAME,
    transportService,
    actionFilters,
    ::GetMonitorRequest
),
    SecureTransportAction {

    @Volatile
    override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: ActionRequest, actionListener: ActionListener<GetMonitorResponse>) {
        val transformedRequest = request as? GetMonitorRequest
            ?: recreateObject(request) {
                GetMonitorRequest(it)
            }

        val user = readUserFromThreadContext(client)

        val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, transformedRequest.monitorId)
            .version(transformedRequest.version)
            .fetchSourceContext(transformedRequest.srcContext)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }

        /*
         * Remove security context before you call elasticsearch api's. By this time, permissions required
         * to call this api are validated.
         * Once system-indices [https://github.com/opendistro-for-elasticsearch/security/issues/666] is done, we
         * might further improve this logic. Also change try to kotlin-use for auto-closable.
         */
        client.threadPool().threadContext.stashContext().use {
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            actionListener.onFailure(
                                AlertingException.wrap(OpenSearchStatusException("Monitor not found.", RestStatus.NOT_FOUND))
                            )
                            return
                        }

                        var monitor: Monitor? = null
                        if (!response.isSourceEmpty) {
                            XContentHelper.createParser(
                                xContentRegistry,
                                LoggingDeprecationHandler.INSTANCE,
                                response.sourceAsBytesRef,
                                XContentType.JSON
                            ).use { xcp ->
                                monitor = ScheduledJob.parse(xcp, response.id, response.version) as Monitor

                                // security is enabled and filterby is enabled
                                if (!checkUserPermissionsWithResource(
                                        user,
                                        monitor?.user,
                                        actionListener,
                                        "monitor",
                                        transformedRequest.monitorId
                                    )
                                ) {
                                    return
                                }
                            }
                        }
                        try {
                            scope.launch {
                                val associatedCompositeMonitors = getAssociatedWorkflows(response.id)
                                actionListener.onResponse(
                                    GetMonitorResponse(
                                        response.id,
                                        response.version,
                                        response.seqNo,
                                        response.primaryTerm,
                                        monitor,
                                        associatedCompositeMonitors
                                    )
                                )
                            }
                        } catch (e: Exception) {
                            log.error("Failed to get associate workflows in get monitor action", e)
                        }
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        }
    }

    private suspend fun getAssociatedWorkflows(id: String): List<AssociatedWorkflow> {
        try {
            val associatedWorkflows = mutableListOf<AssociatedWorkflow>()
            val queryBuilder = QueryBuilders.nestedQuery(
                WORKFLOW_DELEGATE_PATH,
                QueryBuilders.boolQuery().must(
                    QueryBuilders.matchQuery(
                        WORKFLOW_MONITOR_PATH,
                        id
                    )
                ),
                ScoreMode.None
            )
            val searchRequest = SearchRequest()
                .indices(ScheduledJob.SCHEDULED_JOBS_INDEX)
                .source(SearchSourceBuilder().query(queryBuilder).fetchField("_id"))
            val response: SearchResponse = client.suspendUntil { client.search(searchRequest, it) }

            for (hit in response.hits) {
                XContentType.JSON.xContent().createParser(
                    xContentRegistry,
                    LoggingDeprecationHandler.INSTANCE,
                    hit.sourceAsString
                ).use { hitsParser ->
                    val workflow = ScheduledJob.parse(hitsParser, hit.id, hit.version)
                    if (workflow is Workflow) {
                        associatedWorkflows.add(AssociatedWorkflow(hit.id, workflow.name))
                    }
                }
            }
            return associatedWorkflows
        } catch (e: java.lang.Exception) {
            log.error("failed to fetch associated workflows for monitor $id", e)
            return emptyList()
        }
    }
}
