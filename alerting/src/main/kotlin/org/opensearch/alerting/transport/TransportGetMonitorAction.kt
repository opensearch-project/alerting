/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.apache.lucene.search.join.ScoreMode
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.search.SearchAction
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.GetMonitorAction
import org.opensearch.alerting.action.GetMonitorRequest
import org.opensearch.alerting.action.GetMonitorResponse
import org.opensearch.alerting.action.GetMonitorResponse.AssociatedWorkflow
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
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportGetMonitorAction::class.java)

class TransportGetMonitorAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    val clusterService: ClusterService,
    settings: Settings,
) : HandledTransportAction<GetMonitorRequest, GetMonitorResponse>(
    GetMonitorAction.NAME,
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

    override fun doExecute(task: Task, getMonitorRequest: GetMonitorRequest, actionListener: ActionListener<GetMonitorResponse>) {
        val user = readUserFromThreadContext(client)

        val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, getMonitorRequest.monitorId)
            .version(getMonitorRequest.version)
            .fetchSourceContext(getMonitorRequest.srcContext)

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
                                        getMonitorRequest.monitorId
                                    )
                                ) {
                                    return
                                }
                            }
                        }

                        getAssociatedWorkflows(response.id, response, monitor, actionListener)
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        }
    }

    private fun getAssociatedWorkflows(
        id: String,
        getResponse: GetResponse,
        monitor: Monitor?,
        actionListener: ActionListener<GetMonitorResponse>
    ) {
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
        client.execute(
            SearchAction.INSTANCE,
            searchRequest,
            object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    val associatedWorkflows = mutableListOf<AssociatedWorkflow>()
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

                    actionListener.onResponse(
                        GetMonitorResponse(
                            getResponse.id,
                            getResponse.version,
                            getResponse.seqNo,
                            getResponse.primaryTerm,
                            RestStatus.OK,
                            monitor,
                            associatedWorkflows
                        )
                    )
                }

                override fun onFailure(e: java.lang.Exception) {
                    log.error("failed to fetch associated workflows for monitor $id", e)
                    actionListener.onResponse(
                        GetMonitorResponse(
                            getResponse.id,
                            getResponse.version,
                            getResponse.seqNo,
                            getResponse.primaryTerm,
                            RestStatus.OK,
                            monitor,
                            emptyList()
                        )
                    )
                }
            }
        )
    }
}
