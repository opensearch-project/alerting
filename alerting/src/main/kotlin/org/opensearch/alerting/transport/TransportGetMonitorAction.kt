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
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.ScheduledJobUtils.Companion.WORKFLOW_DELEGATE_PATH
import org.opensearch.alerting.util.ScheduledJobUtils.Companion.WORKFLOW_MONITOR_PATH
import org.opensearch.alerting.util.use
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
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.QueryBuilders
import org.opensearch.remote.metadata.client.GetDataObjectRequest
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.remote.metadata.common.SdkClientUtils
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

private val log = LogManager.getLogger(TransportGetMonitorAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportGetMonitorAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    val clusterService: ClusterService,
    settings: Settings,
    val sdkClient: SdkClient,
) : HandledTransportAction<ActionRequest, GetMonitorResponse>(
    AlertingActions.GET_MONITOR_ACTION_NAME,
    transportService,
    actionFilters,
    ::GetMonitorRequest
),
    SecureTransportAction {

    @Volatile
    override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    private val multiTenancyEnabled = AlertingSettings.MULTI_TENANCY_ENABLED.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: ActionRequest, actionListener: ActionListener<GetMonitorResponse>) {
        val transformedRequest = request as? GetMonitorRequest
            ?: recreateObject(request) {
                GetMonitorRequest(it)
            }

        val user = readUserFromThreadContext(client)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }

        val tenantId = client.threadPool().threadContext.getHeader(AlertingPlugin.TENANT_ID_HEADER)
        val getRequest = GetDataObjectRequest.builder()
            .index(ScheduledJob.SCHEDULED_JOBS_INDEX)
            .id(transformedRequest.monitorId)
            .tenantId(tenantId)
            .fetchSourceContext(transformedRequest.srcContext)
            .build()

        client.threadPool().threadContext.stashContext().use {
            sdkClient.getDataObjectAsync(getRequest).whenComplete { response, throwable ->
                if (throwable != null) {
                    val cause = SdkClientUtils.unwrapAndConvertToException(throwable)
                    if (isIndexNotFoundException(cause)) {
                        actionListener.onFailure(
                            AlertingException.wrap(
                                OpenSearchStatusException("Monitor not found.", RestStatus.NOT_FOUND, cause)
                            )
                        )
                    } else {
                        actionListener.onFailure(AlertingException.wrap(cause))
                    }
                    return@whenComplete
                }
                try {
                    val getResponse = response.getResponse()
                    if (getResponse == null || !getResponse.isExists) {
                        actionListener.onFailure(
                            AlertingException.wrap(OpenSearchStatusException("Monitor not found.", RestStatus.NOT_FOUND))
                        )
                        return@whenComplete
                    }
                    var monitor: Monitor? = null
                    if (!getResponse.isSourceEmpty) {
                        XContentHelper.createParser(
                            xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                            getResponse.sourceAsBytesRef, XContentType.JSON
                        ).use { xcp ->
                            monitor = ScheduledJob.parse(xcp, getResponse.id, getResponse.version) as Monitor
                        }
                    }
                    if (!checkUserPermissionsWithResource(user, monitor?.user, actionListener, "monitor", transformedRequest.monitorId)) {
                        return@whenComplete
                    }
                    scope.launch {
                        val associatedCompositeMonitors = getAssociatedWorkflows(getResponse.id)
                        actionListener.onResponse(
                            GetMonitorResponse(
                                getResponse.id, getResponse.version, getResponse.seqNo, getResponse.primaryTerm,
                                monitor, associatedCompositeMonitors
                            )
                        )
                    }
                } catch (e: Exception) {
                    log.error("Failed to parse monitor from SDK response", e)
                    actionListener.onFailure(AlertingException.wrap(e))
                }
            }
        }
    }

    // Checks if the exception is caused by an IndexNotFoundException (directly or nested).
    private fun isIndexNotFoundException(e: Exception): Boolean {
        var cause: Throwable? = e
        while (cause != null) {
            if (cause is IndexNotFoundException) {
                return true
            }
            cause = cause.cause
        }
        return false
    }

    private suspend fun getAssociatedWorkflows(id: String): List<AssociatedWorkflow> {
        // Skip local scheduled-job index search when multi-tenancy is enabled.
        if (multiTenancyEnabled) return emptyList()
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
