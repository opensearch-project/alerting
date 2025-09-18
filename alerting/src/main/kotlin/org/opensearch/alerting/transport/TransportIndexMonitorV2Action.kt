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
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.alerting.PPLMonitorRunner.appendCustomCondition
import org.opensearch.alerting.PPLMonitorRunner.executePplQuery
import org.opensearch.alerting.PPLMonitorRunner.findEvalResultVar
import org.opensearch.alerting.PPLMonitorRunner.findEvalResultVarIdxInSchema
import org.opensearch.alerting.actionv2.IndexMonitorV2Action
import org.opensearch.alerting.actionv2.IndexMonitorV2Request
import org.opensearch.alerting.actionv2.IndexMonitorV2Response
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.core.modelv2.MonitorV2
import org.opensearch.alerting.core.modelv2.PPLMonitor
import org.opensearch.alerting.core.modelv2.PPLTrigger.ConditionType
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_MAX_MONITORS
import org.opensearch.alerting.settings.AlertingSettings.Companion.INDEX_TIMEOUT
import org.opensearch.alerting.settings.AlertingSettings.Companion.REQUEST_TIMEOUT
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.use
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.commons.alerting.model.userErrorMessage
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.authuser.User
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestRequest
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.node.NodeClient

private val log = LogManager.getLogger(TransportIndexMonitorV2Action::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportIndexMonitorV2Action @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val scheduledJobIndices: ScheduledJobIndices,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
    val namedWriteableRegistry: NamedWriteableRegistry,
) : HandledTransportAction<IndexMonitorV2Request, IndexMonitorV2Response>(
    IndexMonitorV2Action.NAME, transportService, actionFilters, ::IndexMonitorV2Request
),
    SecureTransportAction {

    // TODO: add monitor v2 versions of these settings
    @Volatile private var maxMonitors = ALERTING_MAX_MONITORS.get(settings)
    @Volatile private var requestTimeout = REQUEST_TIMEOUT.get(settings)
    @Volatile private var indexTimeout = INDEX_TIMEOUT.get(settings)
//    @Volatile private var maxActionThrottle = MAX_ACTION_THROTTLE_VALUE.get(settings)
    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    override fun doExecute(
        task: Task,
        indexMonitorV2Request: IndexMonitorV2Request,
        actionListener: ActionListener<IndexMonitorV2Response>
    ) {
        // validate the MonitorV2 based on its type
        when (indexMonitorV2Request.monitorV2) {
            is PPLMonitor -> validateMonitorPplQuery(
                indexMonitorV2Request.monitorV2 as PPLMonitor,
                object : ActionListener<Unit> { // validationListener
                    override fun onResponse(response: Unit) {
                        checkUserAndIndicesAccess(client, actionListener, indexMonitorV2Request)
                    }

                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(e)
                    }
                }
            )
            else -> actionListener.onFailure(
                AlertingException.wrap(
                    IllegalStateException(
                        "unexpected MonitorV2 type: ${indexMonitorV2Request.monitorV2.javaClass.name}"
                    )
                )
            )
        }
    }

    // validates the PPL Monitor query by submitting it to SQL/PPL plugin
    private fun validateMonitorPplQuery(pplMonitor: PPLMonitor, validationListener: ActionListener<Unit>) {
        scope.launch {
            try {
                val nodeClient = client as NodeClient

                // first attempt to run the base query
                // if there are any PPL syntax errors, this will throw an exception
                executePplQuery(pplMonitor.query, nodeClient)

                // now scan all the triggers with custom conditions, and ensure each query constructed
                // from the base query + custom condition is valid
                val allCustomTriggersValid = true
                for (pplTrigger in pplMonitor.triggers) {
                    if (pplTrigger.conditionType == ConditionType.NUMBER_OF_RESULTS) {
                        continue
                    }

                    val evalResultVar = findEvalResultVar(pplTrigger.customCondition!!)

                    val queryWithCustomCondition = appendCustomCondition(pplMonitor.query, pplTrigger.customCondition!!)

                    val executePplQueryResponse = executePplQuery(queryWithCustomCondition, nodeClient)

                    val evalResultVarIdx = findEvalResultVarIdxInSchema(executePplQueryResponse, evalResultVar)

                    val resultVarType = executePplQueryResponse
                        .getJSONArray("schema")
                        .getJSONObject(evalResultVarIdx)
                        .getString("type")

                    // custom conditions must evaluate to a boolean result, otherwise it's invalid
                    if (resultVarType != "boolean") {
                        validationListener.onFailure(
                            AlertingException.wrap(
                                IllegalArgumentException(
                                    "Custom condition in trigger ${pplTrigger.name} is invalid because it does not " +
                                        "evaluate to a boolean, but instead to type: $resultVarType"
                                )
                            )
                        )
                        return@launch
                    }
                }

                validationListener.onResponse(Unit)
            } catch (e: Exception) {
                validationListener.onFailure(
                    AlertingException.wrap(
                        IllegalArgumentException("Invalid PPL Query in PPL Monitor: ${e.userErrorMessage()}")
                    )
                )
            }
        }
    }

    private fun checkUserAndIndicesAccess(
        client: Client,
        actionListener: ActionListener<IndexMonitorV2Response>,
        indexMonitorV2Request: IndexMonitorV2Request
    ) {
        /* check initial user permissions */
        val headers = client.threadPool().threadContext.headers
        log.info("Headers in transport layer: $headers")

        val user = readUserFromThreadContext(client)

        log.info("user in checkUserAndIndicesAccess: $user")

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }

        if (
            user != null &&
            !isAdmin(user) &&
            indexMonitorV2Request.rbacRoles != null
        ) {
            if (indexMonitorV2Request.rbacRoles.stream().anyMatch { !user.backendRoles.contains(it) }) {
                log.debug(
                    "User specified backend roles, ${indexMonitorV2Request.rbacRoles}, " +
                        "that they don't have access to. User backend roles: ${user.backendRoles}"
                )
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException(
                            "User specified backend roles that they don't have access to. Contact administrator", RestStatus.FORBIDDEN
                        )
                    )
                )
                return
            } else if (indexMonitorV2Request.rbacRoles.isEmpty()) {
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

        /* check user access to indices */
        when (indexMonitorV2Request.monitorV2) {
            is PPLMonitor -> {
                checkPplQueryIndices(indexMonitorV2Request, client, actionListener, user)
            }
        }
    }

    private fun checkPplQueryIndices(
        indexMonitorV2Request: IndexMonitorV2Request,
        client: Client,
        actionListener: ActionListener<IndexMonitorV2Response>,
        user: User?
    ) {
        log.info("user in checkPplQueryIndices: $user")
        val pplMonitor = indexMonitorV2Request.monitorV2 as PPLMonitor
        val pplQuery = pplMonitor.query
        val indices = getIndicesFromPplQuery(pplQuery)

        val searchRequest = SearchRequest().indices(*indices.toTypedArray())
            .source(SearchSourceBuilder.searchSource().size(1).query(QueryBuilders.matchAllQuery()))
        client.search(
            searchRequest,
            object : ActionListener<SearchResponse> {
                override fun onResponse(searchResponse: SearchResponse) {
                    // User has read access to configured indices in the monitor, now create monitor without user context.
                    client.threadPool().threadContext.stashContext().use {
                        if (user == null) {
                            // Security is disabled, add empty user to Monitor. user is null for older versions.
                            indexMonitorV2Request.monitorV2 = pplMonitor
                                .copy(user = User("", listOf(), listOf(), listOf()))
                            checkScheduledJobIndex(indexMonitorV2Request, actionListener, user)
                        } else {
                            indexMonitorV2Request.monitorV2 = pplMonitor
                                .copy(user = User(user.name, user.backendRoles, user.roles, user.customAttNames))
                            checkScheduledJobIndex(indexMonitorV2Request, actionListener, user)
                        }
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

    private fun checkScheduledJobIndex(
        indexMonitorRequest: IndexMonitorV2Request,
        actionListener: ActionListener<IndexMonitorV2Response>,
        user: User?
    ) {
        log.info("user in checkScheduledJobIndex: $user")
        /* check to see if alerting-config index (scheduled job index) is created and updated before indexing MonitorV2 into it */
        if (!scheduledJobIndices.scheduledJobIndexExists()) { // if alerting-config index doesn't exist, send request to create it
            scheduledJobIndices.initScheduledJobIndex(object : ActionListener<CreateIndexResponse> {
                override fun onResponse(response: CreateIndexResponse) {
                    onCreateMappingsResponse(response.isAcknowledged, indexMonitorRequest, actionListener, user)
                }

                override fun onFailure(e: Exception) {
                    if (ExceptionsHelper.unwrapCause(e) is ResourceAlreadyExistsException) {
                        scope.launch {
                            // Wait for the yellow status
                            val clusterHealthRequest = ClusterHealthRequest()
                                .indices(SCHEDULED_JOBS_INDEX)
                                .waitForYellowStatus()
                            val response: ClusterHealthResponse = client.suspendUntil {
                                execute(ClusterHealthAction.INSTANCE, clusterHealthRequest, it)
                            }
                            if (response.isTimedOut) {
                                actionListener.onFailure(
                                    OpenSearchException("Cannot determine that the $SCHEDULED_JOBS_INDEX index is healthy")
                                )
                            }
                            // Retry mapping of monitor
                            onCreateMappingsResponse(true, indexMonitorRequest, actionListener, user)
                        }
                    } else {
                        actionListener.onFailure(AlertingException.wrap(e))
                    }
                }
            })
        } else if (!IndexUtils.scheduledJobIndexUpdated) {
            IndexUtils.updateIndexMapping(
                SCHEDULED_JOBS_INDEX,
                ScheduledJobIndices.scheduledJobMappings(), clusterService.state(), client.admin().indices(),
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(response: AcknowledgedResponse) {
                        onUpdateMappingsResponse(response, indexMonitorRequest, actionListener, user)
                    }
                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        } else {
            prepareMonitorIndexing(indexMonitorRequest, actionListener, user)
        }
    }

    private fun onCreateMappingsResponse(
        isAcknowledged: Boolean,
        request: IndexMonitorV2Request,
        actionListener: ActionListener<IndexMonitorV2Response>,
        user: User?
    ) {
        log.info("user in onCreateMappingsResponse: $user")
        if (isAcknowledged) {
            log.info("Created $SCHEDULED_JOBS_INDEX with mappings.")
            prepareMonitorIndexing(request, actionListener, user)
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

    private fun onUpdateMappingsResponse(
        response: AcknowledgedResponse,
        indexMonitorRequest: IndexMonitorV2Request,
        actionListener: ActionListener<IndexMonitorV2Response>,
        user: User?
    ) {
        log.info("user in onUpdateMappingsResponse: $user")
        if (response.isAcknowledged) {
            log.info("Updated  $SCHEDULED_JOBS_INDEX with mappings.")
            IndexUtils.scheduledJobIndexUpdated()
            prepareMonitorIndexing(indexMonitorRequest, actionListener, user)
        } else {
            log.info("Update $SCHEDULED_JOBS_INDEX mappings call not acknowledged.")
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException(
                        "Updated $SCHEDULED_JOBS_INDEX mappings call not acknowledged.",
                        RestStatus.INTERNAL_SERVER_ERROR
                    )
                )
            )
        }
    }

    /**
     * This function prepares for indexing a new monitor.
     * If this is an update request we can simply update the monitor. Otherwise we first check to see how many monitors already exist,
     * and compare this to the [maxMonitorCount]. Requests that breach this threshold will be rejected.
     */
    private fun prepareMonitorIndexing(
        indexMonitorRequest: IndexMonitorV2Request,
        actionListener: ActionListener<IndexMonitorV2Response>,
        user: User?
    ) {
        log.info("user in prepareMonitorIndexing: $user")
        if (indexMonitorRequest.method == RestRequest.Method.PUT) { // update monitor case
            scope.launch {
                updateMonitor(indexMonitorRequest, actionListener, user)
            }
        } else { // create monitor case
            val query = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("${Monitor.MONITOR_TYPE}.type", Monitor.MONITOR_TYPE))
            val searchSource = SearchSourceBuilder().query(query).timeout(requestTimeout)
            val searchRequest = SearchRequest(SCHEDULED_JOBS_INDEX).source(searchSource)

            client.search(
                searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(searchResponse: SearchResponse) {
                        onMonitorCountSearchResponse(searchResponse, indexMonitorRequest, actionListener, user)
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        }
    }

    /* Functions for Update Monitor flow */

    private suspend fun updateMonitor(
        indexMonitorRequest: IndexMonitorV2Request,
        actionListener: ActionListener<IndexMonitorV2Response>,
        user: User?
    ) {
        val getRequest = GetRequest(SCHEDULED_JOBS_INDEX, indexMonitorRequest.monitorId)
        try {
            val getResponse: GetResponse = client.suspendUntil { client.get(getRequest, it) }
            if (!getResponse.isExists) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException("MonitorV2 with ${indexMonitorRequest.monitorId} is not found", RestStatus.NOT_FOUND)
                    )
                )
                return
            }
            val xcp = XContentHelper.createParser(
                xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                getResponse.sourceAsBytesRef, XContentType.JSON
            )
            val monitorV2 = ScheduledJob.parse(xcp, getResponse.id, getResponse.version) as MonitorV2
            onGetMonitorResponseForUpdate(monitorV2, indexMonitorRequest, actionListener, user)
        } catch (e: ClassCastException) {
            // if ScheduledJob parsed the object and could not cast it to MonitorV2, we must
            // have gotten a Monitor V1 from the given ID
            actionListener.onFailure(
                AlertingException.wrap(
                    IllegalArgumentException(
                        "The ID given corresponds to a V1 Monitor, please pass in the ID of a V2 Monitor"
                    )
                )
            )
        } catch (e: Exception) {
            actionListener.onFailure(AlertingException.wrap(e))
        }
    }

    private suspend fun onGetMonitorResponseForUpdate(
        existingMonitorV2: MonitorV2,
        indexMonitorRequest: IndexMonitorV2Request,
        actionListener: ActionListener<IndexMonitorV2Response>,
        user: User?
    ) {
        if (
            !checkUserPermissionsWithResource(
                user,
                existingMonitorV2.user,
                actionListener,
                "monitor",
                indexMonitorRequest.monitorId
            )
        ) {
            return
        }

        var newMonitorV2: MonitorV2
        val currentMonitorV2: MonitorV2 // this is the same as existingMonitorV2, but will be cast to a specific MonitorV2 type

        when (indexMonitorRequest.monitorV2) {
            is PPLMonitor -> {
                newMonitorV2 = indexMonitorRequest.monitorV2 as PPLMonitor
                currentMonitorV2 = existingMonitorV2 as PPLMonitor
            }
            else -> throw IllegalStateException("received unsupported monitor type to index: ${indexMonitorRequest.monitorV2.javaClass}")
        }

        // If both are enabled, use the current existing monitor enabled time, otherwise the next execution will be
        // incorrect.
        if (newMonitorV2.enabled && currentMonitorV2.enabled) {
            newMonitorV2 = newMonitorV2.copy(enabledTime = currentMonitorV2.enabledTime)
        }

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
            if (indexMonitorRequest.rbacRoles != null) {
                if (isAdmin(user)) {
                    newMonitorV2 = newMonitorV2.copy(
                        user = User(user.name, indexMonitorRequest.rbacRoles, user.roles, user.customAttNames)
                    )
                } else {
                    // rolesToRemove: these are the backend roles to remove from the monitor
                    val rolesToRemove = user.backendRoles - indexMonitorRequest.rbacRoles.orEmpty()
                    // remove the monitor's roles with rolesToRemove and add any roles passed into the request.rbacRoles
                    val updatedRbac = currentMonitorV2.user?.backendRoles.orEmpty() - rolesToRemove + indexMonitorRequest.rbacRoles
                    newMonitorV2 = newMonitorV2.copy(
                        user = User(user.name, updatedRbac, user.roles, user.customAttNames)
                    )
                }
            } else {
                newMonitorV2 = newMonitorV2
                    .copy(user = User(user.name, currentMonitorV2.user!!.backendRoles, user.roles, user.customAttNames))
            }
            log.debug("Update monitor backend roles to: ${newMonitorV2.user?.backendRoles}")
        }

        newMonitorV2 = newMonitorV2.copy(schemaVersion = IndexUtils.scheduledJobIndexSchemaVersion)
        val indexRequest = IndexRequest(SCHEDULED_JOBS_INDEX)
            .setRefreshPolicy(indexMonitorRequest.refreshPolicy)
            .source(newMonitorV2.toXContentWithUser(jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
            .id(indexMonitorRequest.monitorId)
            .routing(indexMonitorRequest.monitorId)
            .setIfSeqNo(indexMonitorRequest.seqNo)
            .setIfPrimaryTerm(indexMonitorRequest.primaryTerm)
            .timeout(indexTimeout)

        log.info(
            "Updating monitor, ${currentMonitorV2.id}, from: ${currentMonitorV2.toXContentWithUser(
                jsonBuilder(),
                ToXContent.MapParams(mapOf("with_type" to "true"))
            )} \n to: ${newMonitorV2.toXContentWithUser(jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true")))}"
        )

        try {
            val indexResponse: IndexResponse = client.suspendUntil { client.index(indexRequest, it) }
            val failureReasons = IndexUtils.checkShardsFailure(indexResponse)
            if (failureReasons != null) {
                actionListener.onFailure(
                    AlertingException.wrap(OpenSearchStatusException(failureReasons.toString(), indexResponse.status()))
                )
                return
            }

            actionListener.onResponse(
                IndexMonitorV2Response(
                    indexResponse.id, indexResponse.version, indexResponse.seqNo,
                    indexResponse.primaryTerm, newMonitorV2
                )
            )
        } catch (e: Exception) {
            actionListener.onFailure(AlertingException.wrap(e))
        }
    }

    /* Functions for Create Monitor flow */

    /**
     * After searching for all existing monitors we validate the system can support another monitor to be created.
     */
    private fun onMonitorCountSearchResponse(
        monitorCountSearchResponse: SearchResponse,
        indexMonitorRequest: IndexMonitorV2Request,
        actionListener: ActionListener<IndexMonitorV2Response>,
        user: User?
    ) {
        log.info("user in onMonitorCountSearchResponse: $user")
        val totalHits = monitorCountSearchResponse.hits.totalHits?.value
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
                indexMonitor(indexMonitorRequest, actionListener, user)
            }
        }
    }

    private suspend fun indexMonitor(
        indexMonitorRequest: IndexMonitorV2Request,
        actionListener: ActionListener<IndexMonitorV2Response>,
        user: User?
    ) {
        log.info("user in indexMonitor: $user")
        var monitorV2 = when (indexMonitorRequest.monitorV2) {
            is PPLMonitor -> indexMonitorRequest.monitorV2 as PPLMonitor
            else -> throw IllegalArgumentException("received unsupported monitor type to index: ${indexMonitorRequest.monitorV2.javaClass}")
        }

        if (user != null) {
            // Use the backend roles which is an intersection of the requested backend roles and the user's backend roles.
            // Admins can pass in any backend role. Also if no backend role is passed in, all the user's backend roles are used.
            val rbacRoles = if (indexMonitorRequest.rbacRoles == null) user.backendRoles.toSet()
            else if (!isAdmin(user)) indexMonitorRequest.rbacRoles.intersect(user.backendRoles).toSet()
            else indexMonitorRequest.rbacRoles

            monitorV2 = when (monitorV2) {
                is PPLMonitor -> monitorV2.copy(
                    user = User(user.name, rbacRoles.toList(), user.roles, user.customAttNames)
                )
                else -> throw IllegalArgumentException(
                    "received unsupported monitor type when resolving backend roles: ${indexMonitorRequest.monitorV2.javaClass}"
                )
            }
            log.debug("Created monitor's backend roles: $rbacRoles")
        }

        // TODO: only works because monitorV2 is always of type PPLMonitor, not extensible to other potential MonitorV2 types
        val indexRequest = IndexRequest(SCHEDULED_JOBS_INDEX)
            .setRefreshPolicy(indexMonitorRequest.refreshPolicy)
            .source(monitorV2.toXContentWithUser(jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
            .routing(indexMonitorRequest.monitorId)
            .setIfSeqNo(indexMonitorRequest.seqNo)
            .setIfPrimaryTerm(indexMonitorRequest.primaryTerm)
            .timeout(indexTimeout)

        log.info(
            "Creating new monitorV2: ${monitorV2.toXContentWithUser(
                jsonBuilder(),
                ToXContent.MapParams(mapOf("with_type" to "true"))
            )}"
        )

        try {
            val indexResponse: IndexResponse = client.suspendUntil { client.index(indexRequest, it) }
            val failureReasons = IndexUtils.checkShardsFailure(indexResponse)
            if (failureReasons != null) {
                log.info(failureReasons.toString())
                actionListener.onFailure(
                    AlertingException.wrap(OpenSearchStatusException(failureReasons.toString(), indexResponse.status()))
                )
                return
            }

            actionListener.onResponse(
                IndexMonitorV2Response(
                    indexResponse.id, indexResponse.version, indexResponse.seqNo,
                    indexResponse.primaryTerm, monitorV2
                )
            )
        } catch (t: Exception) {
            actionListener.onFailure(AlertingException.wrap(t))
        }
    }

    /* Utils */
    private fun getIndicesFromPplQuery(pplQuery: String): List<String> {
        // captures comma-separated concrete indices, index patterns, and index aliases
        val indicesRegex = """(?i)source(?:\s*)=(?:\s*)([-\w.*'+]+(?:\*)?(?:\s*,\s*[-\w.*'+]+\*?)*)\s*\|*""".toRegex()

        // use find() instead of findAll() because a PPL query only ever has one source statement
        // the only capture group specified in the regex captures the comma separated list of indices/index patterns
        val indices = indicesRegex.find(pplQuery)?.groupValues?.get(1)?.split(",")?.map { it.trim() }
            ?: throw IllegalStateException(
                "Could not find indices that PPL Monitor query searches even " +
                    "after validating the query through SQL/PPL plugin"
            )

        return indices
    }
}
