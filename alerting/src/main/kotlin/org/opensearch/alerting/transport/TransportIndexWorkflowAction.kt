package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchException
import org.opensearch.OpenSearchStatusException
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.action.admin.cluster.health.ClusterHealthAction
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_MAX_MONITORS
import org.opensearch.alerting.settings.AlertingSettings.Companion.INDEX_TIMEOUT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MAX_ACTION_THROTTLE_VALUE
import org.opensearch.alerting.settings.AlertingSettings.Companion.REQUEST_TIMEOUT
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.isQueryLevelMonitor
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.io.stream.NamedWriteableRegistry
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.IndexWorkflowRequest
import org.opensearch.commons.alerting.action.IndexWorkflowResponse
import org.opensearch.commons.alerting.model.CompositeInput
import org.opensearch.commons.alerting.model.Delegate
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.recreateObject
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.util.stream.Collectors

private val log = LogManager.getLogger(TransportIndexWorkflowAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportIndexWorkflowAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val scheduledJobIndices: ScheduledJobIndices,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
    val namedWriteableRegistry: NamedWriteableRegistry,
) : HandledTransportAction<ActionRequest, IndexWorkflowResponse>(
    AlertingActions.INDEX_WORKFLOW_ACTION_NAME, transportService, actionFilters, ::IndexWorkflowRequest
),
    SecureTransportAction {

    @Volatile
    private var maxMonitors = ALERTING_MAX_MONITORS.get(settings)

    @Volatile
    private var requestTimeout = REQUEST_TIMEOUT.get(settings)

    @Volatile
    private var indexTimeout = INDEX_TIMEOUT.get(settings)

    @Volatile
    private var maxActionThrottle = MAX_ACTION_THROTTLE_VALUE.get(settings)

    @Volatile
    private var allowList = ALLOW_LIST.get(settings)

    @Volatile
    override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_MAX_MONITORS) { maxMonitors = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(REQUEST_TIMEOUT) { requestTimeout = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(INDEX_TIMEOUT) { indexTimeout = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(MAX_ACTION_THROTTLE_VALUE) { maxActionThrottle = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) { allowList = it }
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: ActionRequest, actionListener: ActionListener<IndexWorkflowResponse>) {
        val transformedRequest = request as? IndexWorkflowRequest
            ?: recreateObject(request, namedWriteableRegistry) {
                IndexWorkflowRequest(it)
            }

        val user = readUserFromThreadContext(client)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }

        if (
            user != null &&
            !isAdmin(user) &&
            transformedRequest.rbacRoles != null
        ) {
            if (transformedRequest.rbacRoles?.stream()?.anyMatch { !user.backendRoles.contains(it) } == true) {
                log.debug(
                    "User specified backend roles, ${transformedRequest.rbacRoles}, " +
                        "that they don' have access to. User backend roles: ${user.backendRoles}"
                )
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException(
                            "User specified backend roles that they don't have access to. Contact administrator", RestStatus.FORBIDDEN
                        )
                    )
                )
                return
            } else if (transformedRequest.rbacRoles?.isEmpty() == true) {
                log.debug(
                    "Non-admin user are not allowed to specify an empty set of backend roles. " +
                        "Please don't pass in the parameter or pass in at least one backend role."
                )
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException(
                            "Non-admin user are not allowed to specify an empty set of backend roles.", RestStatus.FORBIDDEN
                        )
                    )
                )
                return
            }
        }
        client.threadPool().threadContext.stashContext().use {
            IndexWorkflowHandler(client, actionListener, transformedRequest, user).resolveUserAndStart()
        }
    }

    inner class IndexWorkflowHandler(
        private val client: Client,
        private val actionListener: ActionListener<IndexWorkflowResponse>,
        private val request: IndexWorkflowRequest,
        private val user: User?
    ) {
        fun resolveUserAndStart() {
            scope.launch {
                try {
                    validateRequest(request)
                } catch (e: Exception) {
                    if (e is IndexNotFoundException) {
                        actionListener.onFailure(
                            OpenSearchStatusException(
                                "Monitors not found",
                                RestStatus.NOT_FOUND
                            )
                        )
                    } else {
                        actionListener.onFailure(e)
                    }
                    return@launch
                }

                if (user == null) {
                    // Security is disabled, add empty user to Monitor. user is null for older versions.
                    request.workflow = request.workflow
                        .copy(user = User("", listOf(), listOf(), listOf()))
                    start()
                } else {
                    request.workflow = request.workflow
                        .copy(user = User(user.name, user.backendRoles, user.roles, user.customAttNames))
                    start()
                }
            }
        }

        fun start() {
            if (!scheduledJobIndices.scheduledJobIndexExists()) {
                scheduledJobIndices.initScheduledJobIndex(object : ActionListener<CreateIndexResponse> {
                    override fun onResponse(response: CreateIndexResponse) {
                        onCreateMappingsResponse(response.isAcknowledged)
                    }

                    override fun onFailure(t: Exception) {
                        // https://github.com/opensearch-project/alerting/issues/646
                        if (ExceptionsHelper.unwrapCause(t) is ResourceAlreadyExistsException) {
                            scope.launch {
                                // Wait for the yellow status
                                val request = ClusterHealthRequest()
                                    .indices(SCHEDULED_JOBS_INDEX)
                                    .waitForYellowStatus()
                                val response: ClusterHealthResponse = client.suspendUntil {
                                    execute(ClusterHealthAction.INSTANCE, request, it)
                                }
                                if (response.isTimedOut) {
                                    actionListener.onFailure(
                                        OpenSearchException("Cannot determine that the $SCHEDULED_JOBS_INDEX index is healthy")
                                    )
                                }
                                // Retry mapping of monitor
                                onCreateMappingsResponse(true)
                            }
                        } else {
                            actionListener.onFailure(AlertingException.wrap(t))
                        }
                    }
                })
            } else if (!IndexUtils.scheduledJobIndexUpdated) {
                IndexUtils.updateIndexMapping(
                    SCHEDULED_JOBS_INDEX,
                    ScheduledJobIndices.scheduledJobMappings(), clusterService.state(), client.admin().indices(),
                    object : ActionListener<AcknowledgedResponse> {
                        override fun onResponse(response: AcknowledgedResponse) {
                            onUpdateMappingsResponse(response)
                        }

                        override fun onFailure(t: Exception) {
                            actionListener.onFailure(AlertingException.wrap(t))
                        }
                    }
                )
            } else {
                prepareWorkflowIndexing()
            }
        }

        /**
         * This function prepares for indexing a new workflow.
         * If this is an update request we can simply update the workflow. Otherwise we first check to see how many monitors already exist,
         * and compare this to the [maxMonitorCount]. Requests that breach this threshold will be rejected.
         */
        private fun prepareWorkflowIndexing() {
            if (request.method == RestRequest.Method.PUT) {
                scope.launch {
                    updateWorkflow()
                }
            } else {
                scope.launch {
                    indexWorkflow()
                }
            }
        }

        private fun onCreateMappingsResponse(isAcknowledged: Boolean) {
            if (isAcknowledged) {
                log.info("Created $SCHEDULED_JOBS_INDEX with mappings.")
                prepareWorkflowIndexing()
                IndexUtils.scheduledJobIndexUpdated()
            } else {
                log.info("Create $SCHEDULED_JOBS_INDEX mappings call not acknowledged.")
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException(
                            "Create $SCHEDULED_JOBS_INDEX mappings call not acknowledged", RestStatus.INTERNAL_SERVER_ERROR
                        )
                    )
                )
            }
        }

        private fun onUpdateMappingsResponse(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Updated  ${ScheduledJob.SCHEDULED_JOBS_INDEX} with mappings.")
                IndexUtils.scheduledJobIndexUpdated()
                prepareWorkflowIndexing()
            } else {
                log.info("Update ${ScheduledJob.SCHEDULED_JOBS_INDEX} mappings call not acknowledged.")
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException(
                            "Updated ${ScheduledJob.SCHEDULED_JOBS_INDEX} mappings call not acknowledged.",
                            RestStatus.INTERNAL_SERVER_ERROR
                        )
                    )
                )
            }
        }

        private suspend fun indexWorkflow() {
            if (user != null) {
                val rbacRoles = if (request.rbacRoles == null) user.backendRoles.toSet()
                else if (!isAdmin(user)) request.rbacRoles?.intersect(user.backendRoles)?.toSet()
                else request.rbacRoles

                request.workflow = request.workflow.copy(
                    user = User(user.name, rbacRoles.orEmpty().toList(), user.roles, user.customAttNames)
                )
                log.debug("Created workflow's backend roles: $rbacRoles")
            }

            val indexRequest = IndexRequest(SCHEDULED_JOBS_INDEX)
                .setRefreshPolicy(request.refreshPolicy)
                .source(request.workflow.toXContentWithUser(jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
                .setIfSeqNo(request.seqNo)
                .setIfPrimaryTerm(request.primaryTerm)
                .timeout(indexTimeout)

            try {
                val indexResponse: IndexResponse = client.suspendUntil { client.index(indexRequest, it) }
                val failureReasons = checkShardsFailure(indexResponse)
                if (failureReasons != null) {
                    log.error("Failed to create workflow: $failureReasons")
                    actionListener.onFailure(
                        AlertingException.wrap(OpenSearchStatusException(failureReasons.toString(), indexResponse.status()))
                    )
                    return
                }
                actionListener.onResponse(
                    IndexWorkflowResponse(
                        indexResponse.id, indexResponse.version, indexResponse.seqNo,
                        indexResponse.primaryTerm, request.workflow
                    )
                )
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun updateWorkflow() {
            val getRequest = GetRequest(SCHEDULED_JOBS_INDEX, request.workflowId)
            try {
                val getResponse: GetResponse = client.suspendUntil { client.get(getRequest, it) }
                if (!getResponse.isExists) {
                    actionListener.onFailure(
                        AlertingException.wrap(
                            OpenSearchStatusException("Workflow with ${request.workflowId} is not found", RestStatus.NOT_FOUND)
                        )
                    )
                    return
                }
                val xcp = XContentHelper.createParser(
                    xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    getResponse.sourceAsBytesRef, XContentType.JSON
                )
                val workflow = ScheduledJob.parse(xcp, getResponse.id, getResponse.version) as Workflow
                onGetResponse(workflow)
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun onGetResponse(currentWorkflow: Workflow) {
            if (!checkUserPermissionsWithResource(user, currentWorkflow.user, actionListener, "workfklow", request.workflowId)) {
                return
            }

            // If both are enabled, use the current existing monitor enabled time, otherwise the next execution will be
            // incorrect.
            if (request.workflow.enabled && currentWorkflow.enabled)
                request.workflow = request.workflow.copy(enabledTime = currentWorkflow.enabledTime)

            /**
             * On update workflow check which backend roles to associate to the workflow.
             * Below are 2 examples of how the logic works
             *
             * Example 1, say we have a Workflow with backend roles [a, b, c, d] associated with it.
             * If I'm User A (non-admin user) and I have backend roles [a, b, c] associated with me and I make a request to update
             * the Workflow's backend roles to [a, b]. This would mean that the roles to remove are [c] and the roles to add are [a, b].
             * The Workflow's backend roles would then be [a, b, d].
             *
             * Example 2, say we have a Workflow with backend roles [a, b, c, d] associated with it.
             * If I'm User A (admin user) and I have backend roles [a, b, c] associated with me and I make a request to update
             * the Workflow's backend roles to [a, b]. This would mean that the roles to remove are [c, d] and the roles to add are [a, b].
             * The Workflow's backend roles would then be [a, b].
             */
            if (user != null) {
                if (request.rbacRoles != null) {
                    if (isAdmin(user)) {
                        request.workflow = request.workflow.copy(
                            user = User(user.name, request.rbacRoles, user.roles, user.customAttNames)
                        )
                    } else {
                        // rolesToRemove: these are the backend roles to remove from the monitor
                        val rolesToRemove = user.backendRoles - request.rbacRoles.orEmpty()
                        // remove the monitor's roles with rolesToRemove and add any roles passed into the request.rbacRoles
                        val updatedRbac = currentWorkflow.user?.backendRoles.orEmpty() - rolesToRemove + request.rbacRoles.orEmpty()
                        request.workflow = request.workflow.copy(
                            user = User(user.name, updatedRbac, user.roles, user.customAttNames)
                        )
                    }
                } else {
                    request.workflow = request.workflow
                        .copy(user = User(user.name, currentWorkflow.user!!.backendRoles, user.roles, user.customAttNames))
                }
                log.debug("Update monitor backend roles to: ${request.workflow.user?.backendRoles}")
            }

            request.workflow = request.workflow.copy(schemaVersion = IndexUtils.scheduledJobIndexSchemaVersion)
            val indexRequest = IndexRequest(SCHEDULED_JOBS_INDEX)
                .setRefreshPolicy(request.refreshPolicy)
                .source(request.workflow.toXContentWithUser(jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
                .id(request.workflowId)
                .setIfSeqNo(request.seqNo)
                .setIfPrimaryTerm(request.primaryTerm)
                .timeout(indexTimeout)

            try {
                val indexResponse: IndexResponse = client.suspendUntil { client.index(indexRequest, it) }
                val failureReasons = checkShardsFailure(indexResponse)
                if (failureReasons != null) {
                    actionListener.onFailure(
                        AlertingException.wrap(OpenSearchStatusException(failureReasons.toString(), indexResponse.status()))
                    )
                    return
                }
                actionListener.onResponse(
                    IndexWorkflowResponse(
                        indexResponse.id, indexResponse.version, indexResponse.seqNo,
                        indexResponse.primaryTerm, request.workflow
                    )
                )
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private fun checkShardsFailure(response: IndexResponse): String? {
            val failureReasons = StringBuilder()
            if (response.shardInfo.failed > 0) {
                response.shardInfo.failures.forEach { entry ->
                    failureReasons.append(entry.reason())
                }
                return failureReasons.toString()
            }
            return null
        }
    }

    suspend fun validateRequest(request: IndexWorkflowRequest) {
        if (request.workflow.inputs.isEmpty())
            throw AlertingException.wrap(IllegalArgumentException("Input list can not be empty."))

        if (request.workflow.inputs[0] !is CompositeInput)
            throw AlertingException.wrap(IllegalArgumentException("When creating a workflow input must be CompositeInput"))

        val compositeInput = request.workflow.inputs[0] as CompositeInput
        val monitorIds = compositeInput.sequence.delegates.stream().map { it.monitorId }.collect(Collectors.toList())

        if (monitorIds.isNullOrEmpty())
            throw AlertingException.wrap(IllegalArgumentException("Delegates list can not be empty."))

        validateDuplicateDelegateMonitorReferenceExists(monitorIds)
        validateSequenceOrdering(compositeInput.sequence.delegates)
        validateChainedMonitorFindings(compositeInput.sequence.delegates)
        val delegateMonitors = getDelegateMonitors(monitorIds)
        validateDelegateMonitorsExist(monitorIds, delegateMonitors)
        validateChainedMonitorFindingsMonitors(compositeInput.sequence.delegates, delegateMonitors)
    }

    private fun validateChainedMonitorFindings(delegates: List<Delegate>) {
        val monitorIdOrderMap: Map<String, Int> = delegates.associate { it.monitorId to it.order }
        delegates.forEach {
            if (it.chainedMonitorFindings != null) {
                if (monitorIdOrderMap.containsKey(it.chainedMonitorFindings!!.monitorId) == false) {
                    throw AlertingException.wrap(
                        IllegalArgumentException(
                            "Chained Findings Monitor ${it.chainedMonitorFindings!!.monitorId} doesn't exist in sequence"
                        )
                    )
                }
                if (it.order <= monitorIdOrderMap[it.chainedMonitorFindings!!.monitorId]!!) {
                    throw AlertingException.wrap(
                        IllegalArgumentException(
                            "Chained Findings Monitor ${it.chainedMonitorFindings!!.monitorId} should be executed before monitor ${it.monitorId}"
                        )
                    )
                }
            }
        }
    }

    private fun validateChainedMonitorFindingsMonitors(delegates: List<Delegate>, monitorDelegates: List<Monitor>) {
        val monitorsById = monitorDelegates.associateBy { it.id }
        delegates.forEach {
            if (it.chainedMonitorFindings != null) {
                val chainedFindingMonitor = monitorsById[it.chainedMonitorFindings!!.monitorId] ?: throw AlertingException.wrap(
                    IllegalArgumentException("Chained finding monitor doesn't exist")
                )

                if (chainedFindingMonitor.isQueryLevelMonitor()) {
                    throw AlertingException.wrap(IllegalArgumentException("Query level monitor can't be part of chained findings"))
                }
            }
        }
    }

    private fun validateSequenceOrdering(delegates: List<Delegate>) {
        val orderSet = delegates.stream().filter { it.order > 0 }.map { it.order }.collect(Collectors.toSet())
        if (orderSet.size != delegates.size) {
            throw AlertingException.wrap(IllegalArgumentException("Sequence ordering of delegate monitor shouldn't contain duplicate order values"))
        }
    }

    private fun validateDuplicateDelegateMonitorReferenceExists(
        monitorIds: MutableList<String>
    ) {
        if (monitorIds.toSet().size != monitorIds.size) {
            throw AlertingException.wrap(IllegalArgumentException("Duplicate delegates not allowed"))
        }
    }

    private fun validateDelegateMonitorsExist(
        monitorIds: List<String>,
        delegateMonitors: List<Monitor>
    ) {
        val reqMonitorIds: MutableList<String> = monitorIds as MutableList<String>
        delegateMonitors.forEach {
            reqMonitorIds.remove(it.id)
        }
        if (reqMonitorIds.isNotEmpty()) {
            throw AlertingException.wrap(IllegalArgumentException(("${reqMonitorIds.joinToString()} are not valid monitor ids")))
        }
    }

    private suspend fun getDelegateMonitors(
        monitorIds: MutableList<String>
    ): List<Monitor> {
        val query = QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery("_id", monitorIds))
        val searchSource = SearchSourceBuilder().query(query)
        val searchRequest = SearchRequest(ScheduledJob.SCHEDULED_JOBS_INDEX).source(searchSource)
        val response: SearchResponse = client.suspendUntil { client.search(searchRequest, it) }
        var monitors = mutableListOf<Monitor>()
        if (response.isTimedOut) {
            return monitors
        }
        for (hit in response.hits) {
            XContentType.JSON.xContent().createParser(
                xContentRegistry,
                LoggingDeprecationHandler.INSTANCE, hit.sourceAsString
            ).use { hitsParser ->
                val monitor = ScheduledJob.parse(hitsParser, hit.id, hit.version)
                monitors.add(monitor as Monitor)
            }
        }
        return monitors
    }
}
