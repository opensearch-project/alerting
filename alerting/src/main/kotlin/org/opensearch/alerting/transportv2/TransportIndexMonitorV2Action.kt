/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transportv2

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchException
import org.opensearch.OpenSearchStatusException
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.admin.cluster.health.ClusterHealthAction
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.alerting.AlertingV2Utils.validateMonitorV2
import org.opensearch.alerting.PPLUtils.appendCustomCondition
import org.opensearch.alerting.PPLUtils.executePplQuery
import org.opensearch.alerting.PPLUtils.findEvalResultVar
import org.opensearch.alerting.PPLUtils.findEvalResultVarIdxInSchema
import org.opensearch.alerting.PPLUtils.getIndicesFromPplQuery
import org.opensearch.alerting.actionv2.IndexMonitorV2Action
import org.opensearch.alerting.actionv2.IndexMonitorV2Request
import org.opensearch.alerting.actionv2.IndexMonitorV2Response
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.core.settings.AlertingV2Settings.Companion.ALERTING_V2_ENABLED
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.modelv2.MonitorV2.Companion.MONITOR_V2_TYPE
import org.opensearch.alerting.modelv2.PPLSQLMonitor
import org.opensearch.alerting.modelv2.PPLSQLTrigger.ConditionType
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_V2_MAX_EXPIRE_DURATION
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_V2_MAX_LOOK_BACK_WINDOW
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_V2_MAX_MONITORS
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_V2_MAX_QUERY_LENGTH
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_V2_MAX_THROTTLE_DURATION
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_V2_QUERY_RESULTS_MAX_DATAROWS
import org.opensearch.alerting.settings.AlertingSettings.Companion.INDEX_TIMEOUT
import org.opensearch.alerting.settings.AlertingSettings.Companion.NOTIFICATION_MESSAGE_SOURCE_MAX_LENGTH
import org.opensearch.alerting.settings.AlertingSettings.Companion.NOTIFICATION_SUBJECT_SOURCE_MAX_LENGTH
import org.opensearch.alerting.settings.AlertingSettings.Companion.REQUEST_TIMEOUT
import org.opensearch.alerting.transport.SecureTransportAction
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.use
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
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

private val log = LogManager.getLogger(TransportIndexMonitorV2Action::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportIndexMonitorV2Action @Inject constructor(
    val transportService: TransportService,
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

    // adjustable limits (via settings)
    @Volatile private var alertingV2Enabled = ALERTING_V2_ENABLED.get(settings)
    @Volatile private var maxMonitors = ALERTING_V2_MAX_MONITORS.get(settings)
    @Volatile private var maxThrottleDuration = ALERTING_V2_MAX_THROTTLE_DURATION.get(settings)
    @Volatile private var maxExpireDuration = ALERTING_V2_MAX_EXPIRE_DURATION.get(settings)
    @Volatile private var maxLookBackWindow = ALERTING_V2_MAX_LOOK_BACK_WINDOW.get(settings)
    @Volatile private var maxQueryLength = ALERTING_V2_MAX_QUERY_LENGTH.get(settings)
    @Volatile private var maxQueryResults = ALERTING_V2_QUERY_RESULTS_MAX_DATAROWS.get(settings)
    @Volatile private var notificationSubjectMaxLength = NOTIFICATION_SUBJECT_SOURCE_MAX_LENGTH.get(settings)
    @Volatile private var notificationMessageMaxLength = NOTIFICATION_MESSAGE_SOURCE_MAX_LENGTH.get(settings)
    @Volatile private var requestTimeout = REQUEST_TIMEOUT.get(settings)
    @Volatile private var indexTimeout = INDEX_TIMEOUT.get(settings)
    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_V2_ENABLED) { alertingV2Enabled = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_V2_MAX_MONITORS) { maxMonitors = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_V2_MAX_THROTTLE_DURATION) { maxThrottleDuration = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_V2_MAX_EXPIRE_DURATION) { maxExpireDuration = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_V2_MAX_LOOK_BACK_WINDOW) { maxLookBackWindow = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_V2_MAX_QUERY_LENGTH) { maxQueryLength = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_V2_QUERY_RESULTS_MAX_DATAROWS) { maxQueryResults = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(NOTIFICATION_SUBJECT_SOURCE_MAX_LENGTH) {
            notificationSubjectMaxLength = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(NOTIFICATION_MESSAGE_SOURCE_MAX_LENGTH) {
            notificationMessageMaxLength = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(REQUEST_TIMEOUT) { requestTimeout = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(INDEX_TIMEOUT) { indexTimeout = it }
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(
        task: Task,
        indexMonitorV2Request: IndexMonitorV2Request,
        actionListener: ActionListener<IndexMonitorV2Response>
    ) {
        if (!alertingV2Enabled) {
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException(
                        "Alerting V2 is currently disabled, please enable it with the " +
                            "cluster setting: ${ALERTING_V2_ENABLED.key}",
                        RestStatus.FORBIDDEN
                    ),
                )
            )
            return
        }

        // read the user from thread context immediately, before
        // downstream flows spin up new threads with fresh context
        val user = readUserFromThreadContext(client)

        // validate the MonitorV2 based on its type
        when (indexMonitorV2Request.monitorV2) {
            is PPLSQLMonitor -> validatePplSqlMonitorUserPermissionsAndQuery(
                indexMonitorV2Request,
                user,
                object : ActionListener<Unit> { // validationListener
                    override fun onResponse(response: Unit) {
                        // user permissions to indices have already been checked
                        // proceed without the context of the user, otherwise,
                        // we would get permissions errors trying to search the alerting-config
                        // index as the user. pass the user object itself so backend
                        // roles can be matched and checked downstream
                        client.threadPool().threadContext.stashContext().use {
                            val pplSqlMonitor = indexMonitorV2Request.monitorV2 as PPLSQLMonitor
                            if (user == null) {
                                indexMonitorV2Request.monitorV2 = pplSqlMonitor
                                    .copy(user = User("", listOf(), listOf(), mapOf()))
                            } else {
                                indexMonitorV2Request.monitorV2 = pplSqlMonitor
                                    .copy(user = User(user.name, user.backendRoles, user.roles, user.customAttributes))
                            }
                            checkScheduledJobIndex(indexMonitorV2Request, actionListener, user)
                        }
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

    // validates the PPL Monitor, its query, and user's permissions to the indices it queries by submitting it to SQL/PPL plugin
    private fun validatePplSqlMonitorUserPermissionsAndQuery(
        indexMonitorV2Request: IndexMonitorV2Request,
        user: User?,
        validationListener: ActionListener<Unit>
    ) {
        client.threadPool().threadContext.stashContext().use {
            scope.launch {
                val singleThreadContext = newSingleThreadContext("IndexMonitorV2ActionThread")
                withContext(singleThreadContext) {
                    it.restore()

                    val pplSqlMonitor = indexMonitorV2Request.monitorV2 as PPLSQLMonitor

                    val pplQueryValid = validatePplSqlQuery(pplSqlMonitor, validationListener)
                    if (!pplQueryValid) {
                        return@withContext
                    }

                    // run basic validations against the PPL/SQL Monitor
                    val pplSqlMonitorValid = validatePplSqlMonitor(pplSqlMonitor, validationListener)
                    if (!pplSqlMonitorValid) {
                        return@withContext
                    }

                    // check the user for basic permissions
                    val userHasPermissions = checkUser(user, indexMonitorV2Request, validationListener)
                    if (!userHasPermissions) {
                        return@withContext
                    }

                    // check that given timestamp field is valid
                    val timestampFieldValid = checkPplQueryIndicesForTimestampField(pplSqlMonitor, validationListener)
                    if (!timestampFieldValid) {
                        return@withContext
                    }

                    validationListener.onResponse(Unit)
                }
            }
        }
    }

    private suspend fun validatePplSqlQuery(
        pplSqlMonitor: PPLSQLMonitor,
        validationListener: ActionListener<Unit>
    ): Boolean {
        // first attempt to run the monitor query and all possible
        // extensions of it (from custom conditions)
        try {
            // first run the base query as is.
            // if there are any PPL syntax or index not found or other errors,
            // this will throw an exception
            executePplQuery(pplSqlMonitor.query, clusterService.state().nodes.localNode, transportService)

            // now scan all the triggers with custom conditions, and ensure each query constructed
            // from the base query + custom condition is valid
            for (pplTrigger in pplSqlMonitor.triggers) {
                if (pplTrigger.conditionType != ConditionType.CUSTOM) {
                    continue
                }

                val evalResultVar = findEvalResultVar(pplTrigger.customCondition!!)

                val queryWithCustomCondition = appendCustomCondition(pplSqlMonitor.query, pplTrigger.customCondition!!)

                val executePplQueryResponse = executePplQuery(
                    queryWithCustomCondition,
                    clusterService.state().nodes.localNode,
                    transportService
                )

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
                    return false
                }
            }
        } catch (e: Exception) {
            validationListener.onFailure(
                AlertingException.wrap(
                    IllegalArgumentException("Validation error for PPL Query in PPL Monitor: ${e.userErrorMessage()}")
                )
            )
            return false
        }

        return true
    }

    private fun validatePplSqlMonitor(pplSqlMonitor: PPLSQLMonitor, validationListener: ActionListener<Unit>): Boolean {
        // ensure the trigger throttle and expire durations are valid
        pplSqlMonitor.triggers.forEach { trigger ->
            trigger.throttleDuration?.let { throttleDuration ->
                if (throttleDuration > maxThrottleDuration) {
                    validationListener.onFailure(
                        AlertingException.wrap(
                            IllegalArgumentException(
                                "Throttle duration must be at most $maxThrottleDuration but was $throttleDuration"
                            )
                        )
                    )
                    return false
                }
            }

            if (trigger.expireDuration > maxExpireDuration) {
                validationListener.onFailure(
                    AlertingException.wrap(
                        IllegalArgumentException(
                            "Expire duration must be at most $maxExpireDuration but was ${trigger.expireDuration}"
                        )
                    )
                )
                return false
            }

            if (trigger.conditionType == ConditionType.NUMBER_OF_RESULTS &&
                trigger.numResultsValue!! > maxQueryResults
            ) {
                validationListener.onFailure(
                    AlertingException.wrap(
                        IllegalArgumentException(
                            "Trigger ${trigger.id} checks for number of results threshold of ${trigger.numResultsValue}, " +
                                "but Alerting V2 is configured only to retrieve $maxQueryResults query results maximum. " +
                                "Please lower the number of results value to one below this maximum value, or adjust the cluster " +
                                "setting: $ALERTING_V2_QUERY_RESULTS_MAX_DATAROWS.key}"
                        )
                    )
                )
                return false
            }

            trigger.actions.forEach { action ->
                if (action.subjectTemplate?.idOrCode?.length!! > notificationSubjectMaxLength) {
                    validationListener.onFailure(
                        AlertingException.wrap(
                            IllegalArgumentException(
                                "Notification subject source cannot exceed length: $notificationSubjectMaxLength"
                            )
                        )
                    )
                    return false
                }

                if (action.messageTemplate.idOrCode.length > notificationMessageMaxLength) {
                    validationListener.onFailure(
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

        // ensure the query length doesn't exceed the limit
        if (pplSqlMonitor.query.length > maxQueryLength) {
            validationListener.onFailure(
                AlertingException.wrap(
                    IllegalArgumentException(
                        "PPL Query length must be at most $maxQueryLength but was ${pplSqlMonitor.query.length}"
                    )
                )
            )
            return false
        }

        // ensure the look back window doesn't exceed the limit
        pplSqlMonitor.lookBackWindow?.let {
            if (pplSqlMonitor.lookBackWindow > maxLookBackWindow) {
                validationListener.onFailure(
                    AlertingException.wrap(
                        IllegalArgumentException(
                            "Look back window must be at most $maxLookBackWindow minutes but was ${pplSqlMonitor.lookBackWindow}"
                        )
                    )
                )
                return false
            }
        }

        return true
    }

    private fun checkUser(
        user: User?,
        indexMonitorV2Request: IndexMonitorV2Request,
        validationListener: ActionListener<Unit>
    ): Boolean {
        /* check initial user permissions */
        if (!validateUserBackendRoles(user, validationListener)) {
            return false
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
                validationListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException(
                            "User specified backend roles that they don't have access to. Contact administrator", RestStatus.FORBIDDEN
                        )
                    )
                )
                return false
            } else if (indexMonitorV2Request.rbacRoles.isEmpty()) {
                log.debug(
                    "Non-admin user are not allowed to specify an empty set of backend roles. " +
                        "Please don't pass in the parameter or pass in at least one backend role."
                )
                validationListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException(
                            "Non-admin user are not allowed to specify an empty set of backend roles.", RestStatus.FORBIDDEN
                        )
                    )
                )
                return false
            }
        }

        return true
    }

    // if look back window is specified, all the indices that the PPL query searches
    // must contain the timestamp field specified in the PPL Monitor, and they must
    // all be of OpenSearch data type "date"
    private suspend fun checkPplQueryIndicesForTimestampField(
        pplSqlMonitor: PPLSQLMonitor,
        validationListener: ActionListener<Unit>
    ): Boolean {
        if (pplSqlMonitor.lookBackWindow == null) {
            // if no look back window was specified, no need
            // to check for timestamp field in PPL query indices
            return true
        }

        val pplQuery = pplSqlMonitor.query
        val timestampField = pplSqlMonitor.timestampField

        try {
            val indices = getIndicesFromPplQuery(pplQuery)
            val getMappingsRequest = GetMappingsRequest().indices(*indices.toTypedArray())
            val getMappingsResponse = client.suspendUntil { admin().indices().getMappings(getMappingsRequest, it) }

            val metadataMap = getMappingsResponse.mappings

            for (index in metadataMap.keys) {
                val metadata = metadataMap[index]!!.sourceAsMap["properties"] as Map<String, Any>
                if (!metadata.keys.contains(timestampField)) {
                    validationListener.onFailure(
                        AlertingException.wrap(
                            IllegalArgumentException("Query index $index don't contain given timestamp field: $timestampField")
                        )
                    )
                    return false
                }
                val typeInfo = metadata[timestampField] as Map<String, String>
                val type = typeInfo["type"]
                val dateType = "date"
                val dateNanosType = "date_nanos"
                if (type != dateType && type != dateNanosType) {
                    validationListener.onFailure(
                        AlertingException.wrap(
                            IllegalArgumentException(
                                "Timestamp field: $timestampField is present in index $index " +
                                    "but is type $type instead of $dateType or $dateNanosType"
                            )
                        )
                    )
                    return false
                }
            }
        } catch (e: Exception) {
            log.error("failed to read query indices' fields when checking for timestamp field: $timestampField")
            validationListener.onFailure(
                AlertingException.wrap(
                    IllegalArgumentException("failed to read query indices' fields when checking for timestamp field: $timestampField", e)
                )
            )
            return false
        }

        return true
    }

    private fun checkScheduledJobIndex(
        indexMonitorRequest: IndexMonitorV2Request,
        actionListener: ActionListener<IndexMonitorV2Response>,
        user: User?
    ) {
        // user permissions to indices have already been checked
        // proceed without the context of the user, otherwise,
        // we would get permissions errors trying to search the alerting-config
        // index as the user
        client.threadPool().threadContext.stashContext().use {
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
    }

    private fun onCreateMappingsResponse(
        isAcknowledged: Boolean,
        request: IndexMonitorV2Request,
        actionListener: ActionListener<IndexMonitorV2Response>,
        user: User?
    ) {
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

    private fun prepareMonitorIndexing(
        indexMonitorRequest: IndexMonitorV2Request,
        actionListener: ActionListener<IndexMonitorV2Response>,
        user: User?
    ) {
        if (indexMonitorRequest.method == RestRequest.Method.PUT) { // update monitor case
            scope.launch {
                updateMonitor(indexMonitorRequest, actionListener, user)
            }
        } else { // create monitor case
            val query = QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery(MONITOR_V2_TYPE))
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
            val scheduledJob = ScheduledJob.parse(xcp, getResponse.id, getResponse.version)

            validateMonitorV2(scheduledJob)?.let {
                actionListener.onFailure(AlertingException.wrap(it))
                return
            }

            val monitorV2 = scheduledJob as MonitorV2

            onGetMonitorResponseForUpdate(monitorV2, indexMonitorRequest, actionListener, user)
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
        log.info("user: $user")
        log.info("monitor user: ${existingMonitorV2.user}")
        if (
            !checkUserPermissionsWithResource(
                user,
                existingMonitorV2.user,
                actionListener,
                "monitor_v2",
                indexMonitorRequest.monitorId
            )
        ) {
            return
        }

        var newMonitorV2 = indexMonitorRequest.monitorV2

        // If both are enabled, use the current existing monitor enabled time,
        // otherwise the next execution will be incorrect.
        if (newMonitorV2.enabled && existingMonitorV2.enabled) {
            newMonitorV2 = newMonitorV2.makeCopy(enabledTime = existingMonitorV2.enabledTime)
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
                    newMonitorV2 = newMonitorV2.makeCopy(
                        user = User(user.name, indexMonitorRequest.rbacRoles, user.roles, user.customAttributes)
                    )
                } else {
                    // rolesToRemove: these are the backend roles to remove from the monitor
                    val rolesToRemove = user.backendRoles - indexMonitorRequest.rbacRoles
                    // remove the monitor's roles with rolesToRemove and add any roles passed into the request.rbacRoles
                    val updatedRbac = existingMonitorV2.user?.backendRoles.orEmpty() - rolesToRemove + indexMonitorRequest.rbacRoles
                    newMonitorV2 = newMonitorV2.makeCopy(
                        user = User(user.name, updatedRbac, user.roles, user.customAttributes)
                    )
                }
            } else {
                newMonitorV2 = newMonitorV2
                    .makeCopy(user = User(user.name, existingMonitorV2.user!!.backendRoles, user.roles, user.customAttributes))
            }
            log.info("Update monitor backend roles to: ${newMonitorV2.user?.backendRoles}")
        }

        newMonitorV2 = newMonitorV2.makeCopy(schemaVersion = IndexUtils.scheduledJobIndexSchemaVersion)
        val indexRequest = IndexRequest(SCHEDULED_JOBS_INDEX)
            .setRefreshPolicy(indexMonitorRequest.refreshPolicy)
            .source(newMonitorV2.toXContentWithUser(jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
            .id(indexMonitorRequest.monitorId)
            .routing(indexMonitorRequest.monitorId)
            .setIfSeqNo(indexMonitorRequest.seqNo)
            .setIfPrimaryTerm(indexMonitorRequest.primaryTerm)
            .timeout(indexTimeout)

        log.info(
            "Updating monitor, ${existingMonitorV2.id}, from: ${existingMonitorV2.toXContentWithUser(
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
        var monitorV2 = indexMonitorRequest.monitorV2

        if (user != null) {
            // Use the backend roles which is an intersection of the requested backend roles and the user's backend roles.
            // Admins can pass in any backend role. Also if no backend role is passed in, all the user's backend roles are used.
            val rbacRoles = if (indexMonitorRequest.rbacRoles == null) user.backendRoles.toSet()
            else if (!isAdmin(user)) indexMonitorRequest.rbacRoles.intersect(user.backendRoles).toSet()
            else indexMonitorRequest.rbacRoles

            monitorV2 = monitorV2.makeCopy(
                user = User(user.name, rbacRoles.toList(), user.roles, user.customAttributes)
            )

            log.debug("Created monitor's backend roles: $rbacRoles")
        }

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
}
