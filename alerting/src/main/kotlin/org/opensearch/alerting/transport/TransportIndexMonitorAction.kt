/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchException
import org.opensearch.OpenSearchSecurityException
import org.opensearch.OpenSearchStatusException
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.ActionRequest
import org.opensearch.action.admin.cluster.health.ClusterHealthAction
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.MonitorMetadataService
import org.opensearch.alerting.PPLUtils.appendCustomCondition
import org.opensearch.alerting.PPLUtils.appendDataRowsLimit
import org.opensearch.alerting.PPLUtils.customConditionIsValid
import org.opensearch.alerting.PPLUtils.executePplQuery
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.service.DeleteMonitorService
import org.opensearch.alerting.service.ExternalSchedulerService
import org.opensearch.alerting.service.SchedulerRoutingResolver
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_MAX_MONITORS
import org.opensearch.alerting.settings.AlertingSettings.Companion.INDEX_TIMEOUT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MAX_ACTION_THROTTLE_VALUE
import org.opensearch.alerting.settings.AlertingSettings.Companion.MAX_PPL_TRIGGERS_PER_MONITOR
import org.opensearch.alerting.settings.AlertingSettings.Companion.MAX_TRIGGERS_PER_MONITOR
import org.opensearch.alerting.settings.AlertingSettings.Companion.MULTI_TENANT_TRIGGER_EVAL_ENABLED
import org.opensearch.alerting.settings.AlertingSettings.Companion.NOTIFICATION_MESSAGE_SOURCE_MAX_LENGTH
import org.opensearch.alerting.settings.AlertingSettings.Companion.NOTIFICATION_SUBJECT_SOURCE_MAX_LENGTH
import org.opensearch.alerting.settings.AlertingSettings.Companion.PPL_MAX_QUERY_LENGTH
import org.opensearch.alerting.settings.AlertingSettings.Companion.PPL_QUERY_RESULTS_MAX_DATAROWS
import org.opensearch.alerting.settings.AlertingSettings.Companion.REQUEST_TIMEOUT
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST
import org.opensearch.alerting.util.ArnValidator
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.addUserBackendRolesFilter
import org.opensearch.alerting.util.await
import org.opensearch.alerting.util.getRoleFilterEnabled
import org.opensearch.alerting.util.isADMonitor
import org.opensearch.alerting.util.isClusterMetricsMonitor
import org.opensearch.alerting.util.isUnsupportedMultiTenantMonitorType
import org.opensearch.alerting.util.use
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.util.FeatureFlags
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.IndexMonitorRequest
import org.opensearch.commons.alerting.action.IndexMonitorResponse
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelMonitorInput.Companion.DOC_LEVEL_INPUT_FIELD
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.Monitor.MonitorType
import org.opensearch.commons.alerting.model.MonitorMetadata
import org.opensearch.commons.alerting.model.PPLInput
import org.opensearch.commons.alerting.model.PPLTrigger
import org.opensearch.commons.alerting.model.ScheduleJobPayload
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.alerting.model.remote.monitors.RemoteDocLevelMonitorInput
import org.opensearch.commons.alerting.model.remote.monitors.RemoteDocLevelMonitorInput.Companion.REMOTE_DOC_LEVEL_MONITOR_INPUT_FIELD
import org.opensearch.commons.alerting.model.userErrorMessage
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.alerting.util.isMonitorOfStandardType
import org.opensearch.commons.alerting.util.isPPLMonitor
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.TenantContext
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.DeleteByQueryAction
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder
import org.opensearch.remote.metadata.client.GetDataObjectRequest
import org.opensearch.remote.metadata.client.PutDataObjectRequest
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.rest.RestRequest
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.node.NodeClient
import java.io.IOException
import java.time.Duration
import java.util.Locale

private val log = LogManager.getLogger(TransportIndexMonitorAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportIndexMonitorAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val scheduledJobIndices: ScheduledJobIndices,
    val docLevelMonitorQueries: DocLevelMonitorQueries,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
    val namedWriteableRegistry: NamedWriteableRegistry,
    val sdkClient: SdkClient,
) : HandledTransportAction<ActionRequest, IndexMonitorResponse>(
    AlertingActions.INDEX_MONITOR_ACTION_NAME, transportService, actionFilters, ::IndexMonitorRequest
),
    SecureTransportAction {

    @Volatile private var maxMonitors = ALERTING_MAX_MONITORS.get(settings)
    @Volatile private var maxTriggersPerMonitor = MAX_TRIGGERS_PER_MONITOR.get(settings)
    @Volatile private var multiTenantTriggerEvalEnabled = MULTI_TENANT_TRIGGER_EVAL_ENABLED.get(settings)
    @Volatile private var requestTimeout = REQUEST_TIMEOUT.get(settings)
    @Volatile private var indexTimeout = INDEX_TIMEOUT.get(settings)
    @Volatile private var maxActionThrottle = MAX_ACTION_THROTTLE_VALUE.get(settings)
    @Volatile private var allowList = ALLOW_LIST.get(settings)

    // PPL Alerting related settings
    @Volatile private var maxQueryLength = PPL_MAX_QUERY_LENGTH.get(settings)
    @Volatile private var maxQueryResults = PPL_QUERY_RESULTS_MAX_DATAROWS.get(settings)
    @Volatile private var maxPPLTriggers = MAX_PPL_TRIGGERS_PER_MONITOR.get(settings)
    @Volatile private var notificationSubjectMaxLength = NOTIFICATION_SUBJECT_SOURCE_MAX_LENGTH.get(settings)
    @Volatile private var notificationMessageMaxLength = NOTIFICATION_MESSAGE_SOURCE_MAX_LENGTH.get(settings)

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    @Volatile private var externalSchedulerEnabled = AlertingSettings.EXTERNAL_SCHEDULER_ENABLED.get(settings)
    @Volatile private var externalSchedulerAccountId = AlertingSettings.EXTERNAL_SCHEDULER_ACCOUNT_ID.get(settings)
    @Volatile private var jobQueueName = AlertingSettings.JOB_QUEUE_NAME.get(settings)
    @Volatile private var externalSchedulerRoleName = AlertingSettings.EXTERNAL_SCHEDULER_ROLE_NAME.get(settings)
    @Volatile private var externalSchedulerExecutionRoleName = AlertingSettings.EXTERNAL_SCHEDULER_EXECUTION_ROLE_NAME.get(settings)

    private val multiTenancyEnabled = AlertingSettings.MULTI_TENANCY_ENABLED.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_MAX_MONITORS) { maxMonitors = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(MAX_TRIGGERS_PER_MONITOR) { maxTriggersPerMonitor = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(MULTI_TENANT_TRIGGER_EVAL_ENABLED) {
            multiTenantTriggerEvalEnabled = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(REQUEST_TIMEOUT) { requestTimeout = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(INDEX_TIMEOUT) { indexTimeout = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(MAX_ACTION_THROTTLE_VALUE) { maxActionThrottle = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) { allowList = it }

        clusterService.clusterSettings.addSettingsUpdateConsumer(PPL_MAX_QUERY_LENGTH) { maxQueryLength = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(PPL_QUERY_RESULTS_MAX_DATAROWS) { maxQueryResults = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(MAX_PPL_TRIGGERS_PER_MONITOR) { maxPPLTriggers = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(NOTIFICATION_SUBJECT_SOURCE_MAX_LENGTH) {
            notificationSubjectMaxLength = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(NOTIFICATION_MESSAGE_SOURCE_MAX_LENGTH) {
            notificationMessageMaxLength = it
        }

        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.EXTERNAL_SCHEDULER_ENABLED) {
            externalSchedulerEnabled = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.EXTERNAL_SCHEDULER_ACCOUNT_ID) {
            externalSchedulerAccountId = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.JOB_QUEUE_NAME) {
            jobQueueName = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.EXTERNAL_SCHEDULER_ROLE_NAME) {
            externalSchedulerRoleName = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.EXTERNAL_SCHEDULER_EXECUTION_ROLE_NAME) {
            externalSchedulerExecutionRoleName = it
        }

        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: ActionRequest, actionListener: ActionListener<IndexMonitorResponse>) {
        val transformedRequest = request as? IndexMonitorRequest
            ?: recreateObject(request, namedWriteableRegistry) {
                IndexMonitorRequest(it)
            }

        if (multiTenancyEnabled && transformedRequest.monitor.isUnsupportedMultiTenantMonitorType()) {
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException(
                        "${transformedRequest.monitor.monitorType} monitors are not allowed when multi-tenancy is enabled.",
                        RestStatus.METHOD_NOT_ALLOWED
                    )
                )
            )
            return
        }

        // Block non-PPL monitors on pluggable dataformat domains
        if (FeatureFlags.isEnabled(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG) &&
            !transformedRequest.monitor.isPPLMonitor() &&
            !transformedRequest.monitor.isClusterMetricsMonitor()
        ) {
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException(
                        "Monitor creation/update failed. ${transformedRequest.monitor.monitorType} type and other DSL-based monitors " +
                            "are not supported on this domain type. This domain supports PPL as the query language for " +
                            "alert monitors. It also supports cluster metrics monitors. Please create one of these monitor types instead.",
                        RestStatus.FORBIDDEN
                    )
                )
            )
            return
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

        if (isADMonitor(transformedRequest.monitor)) {
            // check if user has access to any anomaly detector for AD monitor
            checkAnomalyDetectorAndExecute(client, actionListener, transformedRequest, user)
        } else if (transformedRequest.monitor.monitorType == MonitorType.PPL_MONITOR.value) {
            checkPPLQueryAndExecute(actionListener, transformedRequest, user)
        } else {
            checkIndicesAndExecute(client, actionListener, transformedRequest, user)
        }
    }

    /**
     *  Check if user has permissions to read the configured indices on the monitor and
     *  then create monitor.
     */
    fun checkIndicesAndExecute(
        client: Client,
        actionListener: ActionListener<IndexMonitorResponse>,
        request: IndexMonitorRequest,
        user: User?,
    ) {
        val indices = mutableListOf<String>()
        // todo: for doc level alerting: check if index is present before monitor is created.
        val searchInputs = request.monitor.inputs.filter {
            it.name() == SearchInput.SEARCH_FIELD ||
                it.name() == DOC_LEVEL_INPUT_FIELD ||
                it.name() == REMOTE_DOC_LEVEL_MONITOR_INPUT_FIELD
        }
        searchInputs.forEach {
            val inputIndices = if (it.name() == SearchInput.SEARCH_FIELD) (it as SearchInput).indices
            else if (it.name() == DOC_LEVEL_INPUT_FIELD) (it as DocLevelMonitorInput).indices
            else (it as RemoteDocLevelMonitorInput).docLevelMonitorInput.indices
            indices.addAll(inputIndices)
        }
        val updatedIndices = indices.map { index ->
            if (IndexUtils.isAlias(index, clusterService.state()) || IndexUtils.isDataStream(index, clusterService.state())) {
                val metadata = clusterService.state().metadata.indicesLookup[index]?.writeIndex
                metadata?.index?.name ?: index
            } else {
                index
            }
        }
        val searchRequest = SearchRequest().indices(*updatedIndices.toTypedArray())
            .source(SearchSourceBuilder.searchSource().size(1).query(QueryBuilders.matchAllQuery()))
        client.search(
            searchRequest,
            object : ActionListener<SearchResponse> {
                override fun onResponse(searchResponse: SearchResponse) {
                    // User has read access to configured indices in the monitor, now create monitor with out user context.
                    val tenantId = client.threadPool().threadContext.getHeader(AlertingPlugin.TENANT_ID_HEADER)
                    val schedulerAccountId = client.threadPool().threadContext
                        .getTransient<String>(ExternalSchedulerService.SCHEDULER_ACCOUNT_ID_KEY)
                    client.threadPool().threadContext.stashContext().use {
                        IndexMonitorHandler(client, actionListener, request, user, tenantId, schedulerAccountId).resolveUserAndStart()
                    }
                }

                //  Due to below issue with security plugin, we get security_exception when invalid index name is mentioned.
                //  https://github.com/opendistro-for-elasticsearch/security/issues/718
                override fun onFailure(t: Exception) {
                    actionListener.onFailure(
                        AlertingException.wrap(
                            when (t is OpenSearchSecurityException) {
                                true -> OpenSearchStatusException(
                                    "User doesn't have read permissions for one or more configured index " +
                                        "$indices",
                                    RestStatus.FORBIDDEN
                                )
                                false -> t
                            }
                        )
                    )
                }
            }
        )
    }

    private fun checkPPLQueryAndExecute(
        actionListener: ActionListener<IndexMonitorResponse>,
        indexMonitorRequest: IndexMonitorRequest,
        user: User?
    ) {
        val pplMonitor = indexMonitorRequest.monitor

        // run basic validations against the PPL Monitor
        val pplMonitorValid = validatePPLMonitor(pplMonitor, actionListener)
        if (!pplMonitorValid) {
            return
        }

        // validate the PPL query syntax and that user has permissions to
        // the indices being queried. if it does, proceed to index the
        // PPL Monitor
        validatePPLQuery(pplMonitor, actionListener) {
            val tenantId = client.threadPool().threadContext.getHeader(AlertingPlugin.TENANT_ID_HEADER)
            val schedulerAccountId = client.threadPool().threadContext
                .getTransient<String>(ExternalSchedulerService.SCHEDULER_ACCOUNT_ID_KEY)

            // user permissions to indices have already been checked if we made it to the onResponse(),
            // proceed without the context of the user, otherwise,
            // we would get permissions errors trying to search the alerting-config
            // index as the user. pass the user object itself so backend
            // roles can be matched and checked downstream
            client.threadPool().threadContext.stashContext().use {
                val pplMonitor = indexMonitorRequest.monitor
                if (user == null) {
                    indexMonitorRequest.monitor = pplMonitor
                        .copy(user = User("", listOf(), listOf(), mapOf()))
                } else {
                    indexMonitorRequest.monitor = pplMonitor
                        .copy(user = User(user.name, user.backendRoles, user.roles, user.customAttributes))
                }
                IndexMonitorHandler(
                    client,
                    actionListener,
                    indexMonitorRequest,
                    user,
                    tenantId,
                    schedulerAccountId
                ).resolveUserAndStart()
            }
        }
    }

    private fun validatePPLMonitor(pplMonitor: Monitor, actionListener: ActionListener<IndexMonitorResponse>): Boolean {
        if (pplMonitor.triggers.size > maxPPLTriggers) {
            actionListener.onFailure(
                AlertingException.wrap(
                    IllegalArgumentException(
                        "PPL Monitor ${pplMonitor.id} has too many triggers. Max allowed triggers is $maxPPLTriggers."
                    )
                )
            )
            return false
        }

        pplMonitor.triggers.forEach { trigger ->
            val pplTrigger = trigger as PPLTrigger
            if (pplTrigger.conditionType == PPLTrigger.ConditionType.NUMBER_OF_RESULTS &&
                pplTrigger.numResultsValue!! > maxQueryResults
            ) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        IllegalArgumentException(
                            "Trigger ${trigger.id} checks for number of results threshold of ${trigger.numResultsValue}, " +
                                "but PPL Alerting is configured only to retrieve $maxQueryResults query results maximum. " +
                                "Please lower the number of results value to one below this maximum value, or adjust the cluster " +
                                "setting: ${PPL_QUERY_RESULTS_MAX_DATAROWS.key}"
                        )
                    )
                )
                return false
            }

            pplTrigger.actions.forEach { action ->
                if ((action.subjectTemplate?.idOrCode?.length ?: 0) > notificationSubjectMaxLength) {
                    actionListener.onFailure(
                        AlertingException.wrap(
                            IllegalArgumentException(
                                "Notification subject source cannot exceed length: $notificationSubjectMaxLength"
                            )
                        )
                    )
                    return false
                }

                if (action.messageTemplate.idOrCode.length > notificationMessageMaxLength) {
                    actionListener.onFailure(
                        AlertingException.wrap(
                            IllegalArgumentException(
                                "Notification message source cannot exceed length: $notificationMessageMaxLength"
                            )
                        )
                    )
                    return false
                }
            }
        }

        val query = (pplMonitor.inputs[0] as PPLInput).query

        // ensure the query length doesn't exceed the limit
        if (query.length > maxQueryLength) {
            actionListener.onFailure(
                AlertingException.wrap(
                    IllegalArgumentException(
                        "PPL Query length must be at most $maxQueryLength but was ${query.length}"
                    )
                )
            )
            return false
        }

        return true
    }

    private fun validatePPLQuery(
        pplMonitor: Monitor,
        actionListener: ActionListener<IndexMonitorResponse>,
        onSuccess: () -> Unit
    ) {
        // first attempt to run the monitor query and all possible
        // extensions of it (from custom conditions)
        val baseQuery = (pplMonitor.inputs[0] as PPLInput).query

        val limitedQueryToExecute = appendDataRowsLimit(baseQuery, maxQueryResults)

        // now PPL explain the base query as is.
        // if there are any PPL syntax, index not found, insufficient index permissions, or other errors,
        // this will throw an exception from the SQL/PPL plugin
        executePplQuery(
            limitedQueryToExecute,
            true,
            client as NodeClient,
            object : ActionListener<TransportPPLQueryResponse> {
                override fun onResponse(response: TransportPPLQueryResponse) {
                    // Base query is valid. Now validate custom conditions.
                    val customTriggers = pplMonitor.triggers
                        .filterIsInstance<PPLTrigger>()
                        .filter { it.conditionType == PPLTrigger.ConditionType.CUSTOM }

                    validatePPLCustomConditions(baseQuery, customTriggers, 0, actionListener, onSuccess)
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(
                        AlertingException.wrap(
                            IllegalArgumentException("PPL Query validation failed: ${e.userErrorMessage()}")
                        )
                    )
                }
            }
        )
    }

    private fun validatePPLCustomConditions(
        baseQuery: String,
        customTriggers: List<PPLTrigger>,
        recursionIndex: Int,
        actionListener: ActionListener<IndexMonitorResponse>,
        onSuccess: () -> Unit
    ) {
        // base case: the recursion index iterates over the list of custom triggers. if
        // we have scanned all custom triggers and none of them had issues, all custom
        // triggers are valid, proceed with indexing the PPL Monitor
        if (recursionIndex >= customTriggers.size) {
            onSuccess()
            return
        }

        val customTrigger = customTriggers[recursionIndex]
        val customCondition = customTrigger.customCondition!!

        // validate the custom condition is a where statement and
        // not some other valid PPL statement
        if (!customConditionIsValid(customCondition)) {
            actionListener.onFailure(
                AlertingException.wrap(
                    IllegalArgumentException(
                        "Custom condition for trigger ${customTrigger.name} is invalid, " +
                            "custom condition must be a valid PPL where statement."
                    )
                )
            )
            return
        }

        val queryWithCustomCondition = appendCustomCondition(baseQuery, customCondition)
        val limitedQueryWithCustomCondition = appendDataRowsLimit(queryWithCustomCondition, maxQueryResults)

        // if the custom condition is invalid, this will throw an exception
        // from the SQL/PPL plugin and onFailure will be called
        executePplQuery(
            limitedQueryWithCustomCondition,
            true,
            client as NodeClient,
            object : ActionListener<TransportPPLQueryResponse> {
                override fun onResponse(response: TransportPPLQueryResponse) {
                    validatePPLCustomConditions(baseQuery, customTriggers, recursionIndex + 1, actionListener, onSuccess)
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(
                        AlertingException.wrap(
                            IllegalArgumentException("PPL Query validation failed: ${e.userErrorMessage()}")
                        )
                    )
                }
            }
        )
    }

    /**
     * It's no reasonable to create AD monitor if the user has no access to any detector. Otherwise
     * the monitor will not get any anomaly result. So we will check user has access to at least 1
     * anomaly detector if they need to create AD monitor.
     * As anomaly detector index is system index, common user has no permission to query. So we need
     * to send REST API call to AD REST API.
     */
    fun checkAnomalyDetectorAndExecute(
        client: Client,
        actionListener: ActionListener<IndexMonitorResponse>,
        request: IndexMonitorRequest,
        user: User?,
    ) {
        val tenantId = client.threadPool().threadContext.getHeader(AlertingPlugin.TENANT_ID_HEADER)
        val schedulerAccountId = client.threadPool().threadContext
            .getTransient<String>(ExternalSchedulerService.SCHEDULER_ACCOUNT_ID_KEY)
        client.threadPool().threadContext.stashContext().use {
            IndexMonitorHandler(client, actionListener, request, user, tenantId, schedulerAccountId).resolveUserAndStartForAD()
        }
    }

    inner class IndexMonitorHandler(
        private val client: Client,
        private val actionListener: ActionListener<IndexMonitorResponse>,
        private val request: IndexMonitorRequest,
        private val user: User?,
        private val tenantId: String?,
        private val schedulerAccountId: String?,
    ) {

        fun resolveUserAndStart() {
            if (user == null) {
                // Security is disabled, add empty user to Monitor. user is null for older versions.
                request.monitor = request.monitor
                    .copy(user = User("", listOf(), listOf(), mapOf()))
                start()
            } else {
                request.monitor = request.monitor
                    .copy(user = User(user.name, user.backendRoles, user.roles, user.customAttributes))
                start()
            }
        }

        fun resolveUserAndStartForAD() {
            if (user == null) {
                // Security is disabled, add empty user to Monitor. user is null for older versions.
                request.monitor = request.monitor
                    .copy(user = User("", listOf(), listOf(), mapOf()))
                start()
            } else {
                try {
                    request.monitor = request.monitor
                        .copy(user = User(user.name, user.backendRoles, user.roles, user.customAttributes))
                    val searchSourceBuilder = SearchSourceBuilder().size(0)
                    if (getRoleFilterEnabled(clusterService, settings, "plugins.anomaly_detection.filter_by_backend_roles")) {
                        addUserBackendRolesFilter(user, searchSourceBuilder)
                    }
                    val searchRequest = SearchRequest().indices(".opendistro-anomaly-detectors").source(searchSourceBuilder)
                    client.search(
                        searchRequest,
                        object : ActionListener<SearchResponse> {
                            override fun onResponse(response: SearchResponse?) {
                                val totalHits = response?.hits?.totalHits?.value
                                if (totalHits != null && totalHits > 0L) {
                                    start()
                                } else {
                                    actionListener.onFailure(
                                        AlertingException.wrap(
                                            OpenSearchStatusException("User has no available detectors", RestStatus.NOT_FOUND)
                                        )
                                    )
                                }
                            }

                            override fun onFailure(t: Exception) {
                                actionListener.onFailure(AlertingException.wrap(t))
                            }
                        }
                    )
                } catch (ex: IOException) {
                    actionListener.onFailure(AlertingException.wrap(ex))
                }
            }
        }

        fun start() {
            // When multi-tenancy is enabled, monitors are stored in remote metadata —
            // skip local scheduled-job index creation and mapping updates.
            if (multiTenancyEnabled) {
                prepareMonitorIndexing()
                return
            }
            if (!scheduledJobIndices.scheduledJobIndexExists()) {
                scheduledJobIndices.initScheduledJobIndex(object : ActionListener<CreateIndexResponse> {
                    override fun onResponse(response: CreateIndexResponse) {
                        onCreateMappingsResponse(response.isAcknowledged)
                    }
                    override fun onFailure(t: Exception) {
                        // https://github.com/opensearch-project/alerting/issues/646
                        if (ExceptionsHelper.unwrapCause(t) is ResourceAlreadyExistsException) {
                            scope.launch(TenantContext(tenantId)) {
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
                prepareMonitorIndexing()
            }
        }

        /**
         * This function prepares for indexing a new monitor.
         * If this is an update request we can simply update the monitor. Otherwise we first check to see how many monitors already exist,
         * and compare this to the [maxMonitorCount]. Requests that breach this threshold will be rejected.
         */
        private fun prepareMonitorIndexing() {

            // Below check needs to be async operations and needs to be refactored issue#269
            // checkForDisallowedDestinations(allowList)

            try {
                validateActionThrottle(request.monitor, maxActionThrottle, TimeValue.timeValueMinutes(1))
                validateTriggerCount(request.monitor)
                ArnValidator.validateTargetArn(request.monitor.target)
            } catch (e: RuntimeException) {
                actionListener.onFailure(AlertingException.wrap(e))
                return
            }

            if (request.method == RestRequest.Method.PUT) {
                scope.launch(TenantContext(tenantId)) {
                    updateMonitor()
                }
            } else if (multiTenancyEnabled) {
                // Skip local scheduled-job index search for monitor count when multi-tenancy is enabled.
                scope.launch(TenantContext(tenantId)) {
                    indexMonitor()
                }
            } else {
                val query = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("${Monitor.MONITOR_TYPE}.type", Monitor.MONITOR_TYPE))
                val searchSource = SearchSourceBuilder().query(query).timeout(requestTimeout)
                val searchRequest = SearchRequest(SCHEDULED_JOBS_INDEX).source(searchSource)

                client.search(
                    searchRequest,
                    object : ActionListener<SearchResponse> {
                        override fun onResponse(searchResponse: SearchResponse) {
                            onSearchResponse(searchResponse)
                        }

                        override fun onFailure(t: Exception) {
                            actionListener.onFailure(AlertingException.wrap(t))
                        }
                    }
                )
            }
        }

        private fun validateTriggerCount(monitor: Monitor) {
            require(monitor.triggers.size <= maxTriggersPerMonitor) {
                "The current cluster settings only allow up to $maxTriggersPerMonitor triggers per monitor."
            }
            if (multiTenantTriggerEvalEnabled &&
                Monitor.MonitorType.valueOf(monitor.monitorType.uppercase(Locale.ROOT)) ==
                Monitor.MonitorType.BUCKET_LEVEL_MONITOR
            ) {
                require(monitor.triggers.size <= 1) {
                    "Bucket-level monitors only support 1 trigger when remote trigger evaluation is enabled."
                }
            }
        }

        private fun validateActionThrottle(monitor: Monitor, maxValue: TimeValue, minValue: TimeValue) {
            monitor.triggers.forEach { trigger ->
                trigger.actions.forEach { action ->
                    if (action.throttle != null) {
                        require(
                            TimeValue(Duration.of(action.throttle!!.value.toLong(), action.throttle!!.unit).toMillis())
                                .compareTo(maxValue) <= 0,
                            { "Can only set throttle period less than or equal to $maxValue" }
                        )
                        require(
                            TimeValue(Duration.of(action.throttle!!.value.toLong(), action.throttle!!.unit).toMillis())
                                .compareTo(minValue) >= 0,
                            { "Can only set throttle period greater than or equal to $minValue" }
                        )
                    }
                }
            }
        }

        /**
         * After searching for all existing monitors we validate the system can support another monitor to be created.
         */
        private fun onSearchResponse(response: SearchResponse) {
            val totalHits = response.hits.totalHits?.value
            if (totalHits != null && totalHits >= maxMonitors) {
                log.info("This request would create more than the allowed monitors [$maxMonitors].")
                actionListener.onFailure(
                    AlertingException.wrap(
                        IllegalArgumentException(
                            "This request would create more than the allowed monitors [$maxMonitors]."
                        )
                    )
                )
            } else {
                scope.launch(TenantContext(tenantId)) {
                    indexMonitor()
                }
            }
        }

        private fun onCreateMappingsResponse(isAcknowledged: Boolean) {
            if (isAcknowledged) {
                log.info("Created $SCHEDULED_JOBS_INDEX with mappings.")
                prepareMonitorIndexing()
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
                prepareMonitorIndexing()
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

        private suspend fun indexMonitor() {
            if (user != null) {
                // Use the backend roles which is an intersection of the requested backend roles and the user's backend roles.
                // Admins can pass in any backend role. Also if no backend role is passed in, all the user's backend roles are used.
                val rbacRoles = if (request.rbacRoles == null) user.backendRoles.toSet()
                else if (!isAdmin(user)) request.rbacRoles?.intersect(user.backendRoles)?.toSet()
                else request.rbacRoles

                request.monitor = request.monitor.copy(
                    user = User(user.name, rbacRoles.orEmpty().toList(), user.roles, user.customAttributes)
                )
                log.debug("Created monitor's backend roles: $rbacRoles")
            }

            log.info("Creating new monitor: ${request.monitor.name}, type: ${request.monitor.monitorType}")

            if (!tenantId.isNullOrEmpty()) {
                val updatedMetadata = (request.monitor.metadata.orEmpty()) +
                    (AlertingPlugin.TENANT_ID_METADATA_KEY to tenantId)
                request.monitor = request.monitor.copy(metadata = updatedMetadata)
            }

            val monitorObj = ToXContentObject { builder, params ->
                request.monitor.toXContentWithUser(builder, ToXContent.MapParams(mapOf("with_type" to "true")))
            }
            val putRequest = PutDataObjectRequest.builder()
                .index(SCHEDULED_JOBS_INDEX)
                .tenantId(tenantId)
                .dataObject(monitorObj)
                .build()

            try {
                val putResponse = sdkClient.putDataObjectAsync(putRequest).await()
                if (putResponse.isFailed) {
                    actionListener.onFailure(
                        AlertingException.wrap(
                            OpenSearchStatusException(
                                "Failed to create monitor: ${putResponse.cause()?.message}",
                                putResponse.status() ?: RestStatus.INTERNAL_SERVER_ERROR
                            )
                        )
                    )
                    return
                }
                val indexResponse = putResponse.indexResponse()
                    ?: throw OpenSearchStatusException("No index response from SDK", RestStatus.INTERNAL_SERVER_ERROR)
                request.monitor = request.monitor.copy(id = indexResponse.id)

                if (!multiTenancyEnabled) {
                    var metadata: MonitorMetadata?
                    try {
                        var (monitorMetadata: MonitorMetadata, created: Boolean) =
                            MonitorMetadataService.getOrCreateMetadata(request.monitor)
                        if (created == false) {
                            log.warn("Metadata doc id:${monitorMetadata.id} exists, but it shouldn't!")
                        }
                        metadata = monitorMetadata
                    } catch (t: Exception) {
                        log.error("failed to create metadata for monitor ${indexResponse.id}. deleting monitor")
                        cleanupMonitorAfterPartialFailure(request.monitor, indexResponse)
                        throw t
                    }
                    try {
                        if (
                            request.monitor.isMonitorOfStandardType() &&
                            Monitor.MonitorType.valueOf(request.monitor.monitorType.uppercase(Locale.ROOT)) ==
                            Monitor.MonitorType.DOC_LEVEL_MONITOR
                        ) {
                            indexDocLevelMonitorQueries(request.monitor, indexResponse.id, metadata, request.refreshPolicy)
                        }
                        // When inserting queries in queryIndex we could update sourceToQueryIndexMapping
                        MonitorMetadataService.upsertMetadata(metadata, updating = true)
                    } catch (t: Exception) {
                        log.error("failed to index doc level queries monitor ${indexResponse.id}. deleting monitor", t)
                        cleanupMonitorAfterPartialFailure(request.monitor, indexResponse)
                        throw t
                    }
                }

                // Create external schedule and update monitor with the schedule ARN
                if (externalSchedulerEnabled) {
                    try {
                        val scheduleArn = createExternalSchedule(request.monitor)
                        val updatedMetadata = (request.monitor.metadata.orEmpty()) +
                            (ExternalSchedulerService.SCHEDULE_ARN_METADATA_KEY to scheduleArn)
                        request.monitor = request.monitor.copy(metadata = updatedMetadata)
                        updateMonitorMetadata(request.monitor, tenantId)
                    } catch (t: Exception) {
                        log.error("Failed to create EB schedule for monitor ${indexResponse.id}. Rolling back.", t)
                        cleanupMonitorAfterPartialFailure(request.monitor, indexResponse)
                        throw t
                    }
                }

                actionListener.onResponse(
                    IndexMonitorResponse(
                        indexResponse.id, indexResponse.version, indexResponse.seqNo,
                        indexResponse.primaryTerm, request.monitor
                    )
                )
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun cleanupMonitorAfterPartialFailure(monitor: Monitor, indexMonitorResponse: IndexResponse) {
            // we simply log the success (debug log) or failure (error log) when we try clean up partially failed monitor creation request
            try {
                DeleteMonitorService.deleteMonitor(
                    monitor = monitor,
                    RefreshPolicy.IMMEDIATE
                )
                log.debug(
                    "Cleaned up monitor related resources after monitor creation request partial failure. " +
                        "Monitor id : ${indexMonitorResponse.id}"
                )
            } catch (e: Exception) {
                log.error("Failed to clean up monitor after monitor creation request partial failure", e)
            }
        }

        @Suppress("UNCHECKED_CAST")
        private suspend fun indexDocLevelMonitorQueries(
            monitor: Monitor,
            monitorId: String,
            monitorMetadata: MonitorMetadata,
            refreshPolicy: RefreshPolicy
        ) {
            val queryIndex = monitor.dataSources.queryIndex
            if (!docLevelMonitorQueries.docLevelQueryIndexExists(monitor.dataSources)) {
                docLevelMonitorQueries.initDocLevelQueryIndex(monitor.dataSources)
                log.info("Central Percolation index $queryIndex created")
            }
            docLevelMonitorQueries.indexDocLevelQueries(
                monitor,
                monitorId,
                monitorMetadata,
                refreshPolicy,
                indexTimeout
            )
            log.debug("Queries inserted into Percolate index $queryIndex")
        }

        private suspend fun updateMonitor() {
            val getRequest = GetDataObjectRequest.builder()
                .index(SCHEDULED_JOBS_INDEX)
                .id(request.monitorId)
                .tenantId(tenantId)
                .build()
            try {
                val response = sdkClient.getDataObjectAsync(getRequest).await()
                val getResponse = response.getResponse()
                if (getResponse == null || !getResponse.isExists) {
                    actionListener.onFailure(
                        AlertingException.wrap(
                            OpenSearchStatusException("Monitor with ${request.monitorId} is not found", RestStatus.NOT_FOUND)
                        )
                    )
                    return
                }
                val xcp = XContentHelper.createParser(
                    xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    getResponse.sourceAsBytesRef, XContentType.JSON
                )
                val monitor = ScheduledJob.parse(xcp, getResponse.id, getResponse.version) as Monitor
                onGetResponse(monitor)
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun onGetResponse(currentMonitor: Monitor) {
            if (!checkUserPermissionsWithResource(user, currentMonitor.user, actionListener, "monitor", request.monitorId)) {
                return
            }

            // If both are enabled, use the current existing monitor enabled time, otherwise the next execution will be
            // incorrect.
            if (request.monitor.enabled && currentMonitor.enabled)
                request.monitor = request.monitor.copy(enabledTime = currentMonitor.enabledTime)

            /**
             * On update monitor check which backend roles to associate to the monitor.
             * Below are 2 examples of how the logic works
             *
             * Example 1, say we have a Monitor with backend roles [a, b, c, d] associated with it.
             * If I'm User A (non-admin user) and I have backend roles [a, b, c] associated with me and I make a request to update
             * the Monitor's backend roles to [a, b]. This would mean that the roles to remove are [c] and the roles to add are [a, b].
             * The Monitor's backend roles would then be [a, b, d].
             *
             * Example 2, say we have a Monitor with backend roles [a, b, c, d] associated with it.
             * If I'm User A (admin user) and I have backend roles [a, b, c] associated with me and I make a request to update
             * the Monitor's backend roles to [a, b]. This would mean that the roles to remove are [c, d] and the roles to add are [a, b].
             * The Monitor's backend roles would then be [a, b].
             */
            if (user != null) {
                if (request.rbacRoles != null) {
                    if (isAdmin(user)) {
                        request.monitor = request.monitor.copy(
                            user = User(user.name, request.rbacRoles, user.roles, user.customAttributes)
                        )
                    } else {
                        // rolesToRemove: these are the backend roles to remove from the monitor
                        val rolesToRemove = user.backendRoles - request.rbacRoles.orEmpty()
                        // remove the monitor's roles with rolesToRemove and add any roles passed into the request.rbacRoles
                        val updatedRbac = currentMonitor.user?.backendRoles.orEmpty() - rolesToRemove + request.rbacRoles.orEmpty()
                        request.monitor = request.monitor.copy(
                            user = User(user.name, updatedRbac, user.roles, user.customAttributes)
                        )
                    }
                } else {
                    request.monitor = request.monitor
                        .copy(user = User(user.name, currentMonitor.user!!.backendRoles, user.roles, user.customAttributes))
                }
                log.debug("Update monitor backend roles to: ${request.monitor.user?.backendRoles}")
            }

            request.monitor = request.monitor.copy(schemaVersion = IndexUtils.scheduledJobIndexSchemaVersion)

            log.info("Updating monitor, ${currentMonitor.id}")

            val monitorObj = ToXContentObject { builder, params ->
                request.monitor.toXContentWithUser(builder, ToXContent.MapParams(mapOf("with_type" to "true")))
            }
            val putRequest = PutDataObjectRequest.builder()
                .index(SCHEDULED_JOBS_INDEX)
                .id(request.monitorId)
                .tenantId(tenantId)
                .ifSeqNo(request.seqNo)
                .ifPrimaryTerm(request.primaryTerm)
                .overwriteIfExists(true)
                .dataObject(monitorObj)
                .build()

            try {
                val putResponse = sdkClient.putDataObjectAsync(putRequest).await()
                if (putResponse.isFailed) {
                    actionListener.onFailure(
                        AlertingException.wrap(
                            OpenSearchStatusException(
                                "Failed to update monitor: ${putResponse.cause()?.message}",
                                putResponse.status() ?: RestStatus.INTERNAL_SERVER_ERROR
                            )
                        )
                    )
                    return
                }
                val indexResponse = putResponse.indexResponse()
                    ?: throw OpenSearchStatusException("No index response from SDK", RestStatus.INTERNAL_SERVER_ERROR)
                var isDocLevelMonitorRestarted = false
                // Force re-creation of last run context if monitor is of type standard doc-level/threat-intel
                // And monitor is re-enabled
                if (request.monitor.enabled && !currentMonitor.enabled &&
                    request.monitor.monitorType.endsWith(Monitor.MonitorType.DOC_LEVEL_MONITOR.value)
                ) {
                    isDocLevelMonitorRestarted = true
                }

                if (!multiTenancyEnabled) {
                    var updatedMetadata: MonitorMetadata
                    val (metadata, created) = MonitorMetadataService.getOrCreateMetadata(
                        request.monitor,
                        forceCreateLastRunContext = isDocLevelMonitorRestarted
                    )

                    // Recreate runContext if metadata exists
                    // Delete and insert all queries from/to queryIndex

                    val isDocLevelMonitor = currentMonitor.isMonitorOfStandardType() &&
                        Monitor.MonitorType.valueOf(currentMonitor.monitorType.uppercase(Locale.ROOT)) ==
                        Monitor.MonitorType.DOC_LEVEL_MONITOR
                    if (!created && isDocLevelMonitor) {
                        updatedMetadata = MonitorMetadataService.recreateRunContext(metadata, currentMonitor)
                        if (docLevelMonitorQueries.docLevelQueryIndexExists(currentMonitor.dataSources)) {
                            client.suspendUntil<Client, BulkByScrollResponse> {
                                DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                                    .source(currentMonitor.dataSources.queryIndex)
                                    .filter(QueryBuilders.matchQuery("monitor_id", currentMonitor.id))
                                    .execute(it)
                            }
                        }
                        indexDocLevelMonitorQueries(
                            request.monitor,
                            currentMonitor.id,
                            updatedMetadata,
                            request.refreshPolicy
                        )
                        MonitorMetadataService.upsertMetadata(updatedMetadata, updating = true)
                    }
                }
                // Update external schedule with latest monitor config
                if (externalSchedulerEnabled) {
                    try {
                        updateExternalSchedule(request.monitor, currentMonitor, tenantId)
                    } catch (t: Exception) {
                        log.error("Failed to update EB schedule for monitor ${request.monitorId}", t)
                        actionListener.onFailure(AlertingException.wrap(t))
                        return
                    }
                }
                actionListener.onResponse(
                    IndexMonitorResponse(
                        indexResponse.id, indexResponse.version, indexResponse.seqNo,
                        indexResponse.primaryTerm, request.monitor
                    )
                )
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private fun checkShardsFailure(response: IndexResponse): String? {
            val failureReasons = StringBuilder()
            if (response.shardInfo.failed > 0) {
                response.shardInfo.failures.forEach {
                        entry ->
                    failureReasons.append(entry.reason())
                }
                return failureReasons.toString()
            }
            return null
        }

        /**
         * Creates an external schedule and returns the schedule ARN.
         */
        private fun createExternalSchedule(monitor: Monitor): String {
            val routing = resolveRouting(schedulerAccountId)
            val targetInput = buildScheduleJobPayloadJson(monitor)
            ExternalSchedulerService.createSchedule(monitor, routing, targetInput)
            return ExternalSchedulerService.buildScheduleArn(routing, monitor.id)
        }

        /**
         * Reads the schedule ARN from the existing monitor's metadata to determine
         * the target account, then updates the external schedule with the latest monitor config.
         */
        private fun updateExternalSchedule(monitor: Monitor, currentMonitor: Monitor, tenantId: String?) {
            val scheduleArn = currentMonitor.metadata?.get(ExternalSchedulerService.SCHEDULE_ARN_METADATA_KEY)
            val accountIdOverride = scheduleArn?.let {
                ExternalSchedulerService.parseScheduleArn(it).accountId
            }
            val routing = resolveRouting(accountIdOverride)
            val targetInput = buildScheduleJobPayloadJson(monitor)
            ExternalSchedulerService.updateSchedule(monitor, routing, targetInput)
        }

        /**
         * Builds a JSON string matching [ScheduleJobPayload] schema for the EB schedule target input.
         * Uses the EB placeholder for job_start_time which the scheduler replaces at invocation time
         * with a real ISO-8601 timestamp that [ScheduleJobPayload.parse] can deserialize.
         */
        private fun buildScheduleJobPayloadJson(monitor: Monitor): String {
            val monitorConfigBuilder = XContentFactory.jsonBuilder()
            monitor.toXContentWithUser(monitorConfigBuilder, ToXContent.EMPTY_PARAMS)
            val payload = ScheduleJobPayload(
                monitorId = monitor.id,
                jobStartTime = ExternalSchedulerService.EB_SCHEDULED_TIME_PLACEHOLDER,
                monitorConfig = monitorConfigBuilder.toString()
            )
            val builder = XContentFactory.jsonBuilder()
            payload.toXContent(builder, ToXContent.EMPTY_PARAMS)
            return builder.toString()
        }

        private suspend fun updateMonitorMetadata(monitor: Monitor, tenantId: String?) {
            val monitorObj = ToXContentObject { builder, params ->
                monitor.toXContentWithUser(builder, ToXContent.MapParams(mapOf("with_type" to "true")))
            }
            val putRequest = PutDataObjectRequest.builder()
                .index(SCHEDULED_JOBS_INDEX)
                .id(monitor.id)
                .tenantId(tenantId)
                .overwriteIfExists(true)
                .dataObject(monitorObj)
                .build()
            sdkClient.putDataObjectAsync(putRequest).await()
        }

        private fun resolveRouting(accountIdOverride: String?): SchedulerRoutingResolver.Routing = SchedulerRoutingResolver.resolve(
            settingsAccountId = externalSchedulerAccountId,
            settingsQueueName = jobQueueName,
            settingsRoleName = externalSchedulerRoleName,
            settingsExecutionRoleName = externalSchedulerExecutionRoleName,
            threadContextAccountIdOverride = accountIdOverride
        )
    }
}
