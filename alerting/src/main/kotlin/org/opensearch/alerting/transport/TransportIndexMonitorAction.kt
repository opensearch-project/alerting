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
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.service.DeleteMonitorService
import org.opensearch.alerting.service.ExternalSchedulerService
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_MAX_MONITORS
import org.opensearch.alerting.settings.AlertingSettings.Companion.INDEX_TIMEOUT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MAX_ACTION_THROTTLE_VALUE
import org.opensearch.alerting.settings.AlertingSettings.Companion.MAX_TRIGGERS_PER_MONITOR
import org.opensearch.alerting.settings.AlertingSettings.Companion.REQUEST_TIMEOUT
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.MonitorPayloadBuilder
import org.opensearch.alerting.util.addUserBackendRolesFilter
import org.opensearch.alerting.util.await
import org.opensearch.alerting.util.getRoleFilterEnabled
import org.opensearch.alerting.util.isADMonitor
import org.opensearch.alerting.util.isUnsupportedMultiTenantMonitorType
import org.opensearch.alerting.util.use
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.IndexMonitorRequest
import org.opensearch.commons.alerting.action.IndexMonitorResponse
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelMonitorInput.Companion.DOC_LEVEL_INPUT_FIELD
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorMetadata
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.alerting.model.remote.monitors.RemoteDocLevelMonitorInput
import org.opensearch.commons.alerting.model.remote.monitors.RemoteDocLevelMonitorInput.Companion.REMOTE_DOC_LEVEL_MONITOR_INPUT_FIELD
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.alerting.util.isMonitorOfStandardType
import org.opensearch.commons.authuser.User
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
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
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
    @Volatile private var requestTimeout = REQUEST_TIMEOUT.get(settings)
    @Volatile private var indexTimeout = INDEX_TIMEOUT.get(settings)
    @Volatile private var maxActionThrottle = MAX_ACTION_THROTTLE_VALUE.get(settings)
    @Volatile private var allowList = ALLOW_LIST.get(settings)
    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    private val multiTenancyEnabled = AlertingSettings.MULTI_TENANCY_ENABLED.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_MAX_MONITORS) { maxMonitors = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(MAX_TRIGGERS_PER_MONITOR) { maxTriggersPerMonitor = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(REQUEST_TIMEOUT) { requestTimeout = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(INDEX_TIMEOUT) { indexTimeout = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(MAX_ACTION_THROTTLE_VALUE) { maxActionThrottle = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) { allowList = it }
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

        if (!isADMonitor(transformedRequest.monitor)) {
            checkIndicesAndExecute(client, actionListener, transformedRequest, user)
        } else {
            // check if user has access to any anomaly detector for AD monitor
            checkAnomalyDetectorAndExecute(client, actionListener, transformedRequest, user)
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
                    client.threadPool().threadContext.stashContext().use {
                        IndexMonitorHandler(client, actionListener, request, user).resolveUserAndStart()
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
        client.threadPool().threadContext.stashContext().use {
            IndexMonitorHandler(client, actionListener, request, user).resolveUserAndStartForAD()
        }
    }

    inner class IndexMonitorHandler(
        private val client: Client,
        private val actionListener: ActionListener<IndexMonitorResponse>,
        private val request: IndexMonitorRequest,
        private val user: User?,
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
            } catch (e: RuntimeException) {
                actionListener.onFailure(AlertingException.wrap(e))
                return
            }

            if (request.method == RestRequest.Method.PUT) {
                scope.launch {
                    updateMonitor()
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
                scope.launch {
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

            val tenantId = client.threadPool().threadContext.getHeader(AlertingPlugin.TENANT_ID_HEADER)
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
                var metadata: MonitorMetadata?
                try { // delete monitor if metadata creation fails, log the right error and re-throw the error to fail listener
                    request.monitor = request.monitor.copy(id = indexResponse.id)
                    var (monitorMetadata: MonitorMetadata, created: Boolean) = MonitorMetadataService.getOrCreateMetadata(request.monitor)
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

                // OSSA-606: Create EB schedule for monitor execution (OASIS path)
                try {
                    createExternalSchedule(request.monitor, tenantId)
                } catch (t: Exception) {
                    log.error("Failed to create EB schedule for monitor ${indexResponse.id}. Rolling back.", t)
                    cleanupMonitorAfterPartialFailure(request.monitor, indexResponse)
                    throw t
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
            val tenantId = client.threadPool().threadContext.getHeader(AlertingPlugin.TENANT_ID_HEADER)
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

            val tenantId = client.threadPool().threadContext.getHeader(AlertingPlugin.TENANT_ID_HEADER)
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

                var updatedMetadata: MonitorMetadata
                val (metadata, created) = MonitorMetadataService.getOrCreateMetadata(
                    request.monitor,
                    forceCreateLastRunContext = isDocLevelMonitorRestarted
                )

                // Recreate runContext if metadata exists
                // Delete and insert all queries from/to queryIndex

                if (!created &&
                    currentMonitor.isMonitorOfStandardType() &&
                    Monitor.MonitorType.valueOf(currentMonitor.monitorType.uppercase(Locale.ROOT)) == Monitor.MonitorType.DOC_LEVEL_MONITOR
                ) {
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
                // OSSA-606: Update EB schedule with latest monitor config (OASIS path)
                try {
                    updateExternalSchedule(request.monitor, tenantId)
                } catch (t: Exception) {
                    log.error("Failed to update EB schedule for monitor ${request.monitorId}", t)
                    // Update flow: EB failure is non-fatal — monitor metadata is already persisted.
                    // The schedule will be reconciled on next update or by auditor.
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
         * Reads EB cell info from ThreadContext and creates an external schedule.
         * No-op when OASIS ActionFilter is not present (open-source standalone mode).
         */
        private fun createExternalSchedule(monitor: Monitor, tenantId: String?) {
            val threadContext = client.threadPool().threadContext
            val ebCellAccountId = threadContext.getTransient<String>(ExternalSchedulerService.EB_CELL_ACCOUNT_ID_KEY) ?: return
            val ebCellRegion = threadContext.getTransient<String>(ExternalSchedulerService.EB_CELL_REGION_KEY) ?: return
            val queueArn = threadContext.getTransient<String>(ExternalSchedulerService.EB_CELL_QUEUE_ARN_KEY) ?: return
            val roleArn = threadContext.getTransient<String>(ExternalSchedulerService.EB_CELL_ROLE_ARN_KEY) ?: return

            val targetInput = MonitorPayloadBuilder.buildTargetInput(
                monitor = monitor,
                appId = threadContext.getHeader("x-app-id")
                    ?: throw OpenSearchStatusException("x-app-id header missing", RestStatus.BAD_REQUEST),
                tenantId = tenantId
                    ?: throw OpenSearchStatusException("Tenant ID missing", RestStatus.BAD_REQUEST),
                workspaceId = threadContext.getHeader("x-workspace-id")
                    ?: throw OpenSearchStatusException("x-workspace-id header missing", RestStatus.BAD_REQUEST),
                collectionEndpoint = threadContext.getHeader("x-collection-endpoint")
                    ?: throw OpenSearchStatusException("x-collection-endpoint header missing", RestStatus.BAD_REQUEST)
            )

            ExternalSchedulerService.createSchedule(
                monitor, ebCellAccountId, ebCellRegion, queueArn, roleArn, targetInput
            )
        }

        /**
         * Reads EB cell info from ThreadContext and updates the external schedule.
         * Per LLD 6.2: always refreshes Target.Input with latest monitor config.
         */
        private fun updateExternalSchedule(monitor: Monitor, tenantId: String?) {
            val threadContext = client.threadPool().threadContext
            val ebCellAccountId = threadContext.getTransient<String>(ExternalSchedulerService.EB_CELL_ACCOUNT_ID_KEY) ?: return
            val ebCellRegion = threadContext.getTransient<String>(ExternalSchedulerService.EB_CELL_REGION_KEY) ?: return
            val queueArn = threadContext.getTransient<String>(ExternalSchedulerService.EB_CELL_QUEUE_ARN_KEY) ?: return
            val roleArn = threadContext.getTransient<String>(ExternalSchedulerService.EB_CELL_ROLE_ARN_KEY) ?: return

            val targetInput = MonitorPayloadBuilder.buildTargetInput(
                monitor = monitor,
                appId = threadContext.getHeader("x-app-id")
                    ?: throw OpenSearchStatusException("x-app-id header missing", RestStatus.BAD_REQUEST),
                tenantId = tenantId
                    ?: throw OpenSearchStatusException("Tenant ID missing", RestStatus.BAD_REQUEST),
                workspaceId = threadContext.getHeader("x-workspace-id")
                    ?: throw OpenSearchStatusException("x-workspace-id header missing", RestStatus.BAD_REQUEST),
                collectionEndpoint = threadContext.getHeader("x-collection-endpoint")
                    ?: throw OpenSearchStatusException("x-collection-endpoint header missing", RestStatus.BAD_REQUEST)
            )

            ExternalSchedulerService.updateSchedule(
                monitor, ebCellAccountId, ebCellRegion, queueArn, roleArn, targetInput
            )
        }
    }
}
