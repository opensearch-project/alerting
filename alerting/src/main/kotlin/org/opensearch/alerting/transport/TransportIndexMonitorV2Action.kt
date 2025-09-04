package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchException
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
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_MAX_MONITORS
import org.opensearch.alerting.settings.AlertingSettings.Companion.INDEX_TIMEOUT
import org.opensearch.alerting.settings.AlertingSettings.Companion.REQUEST_TIMEOUT
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.IndexMonitorV2Request
import org.opensearch.commons.alerting.action.IndexMonitorV2Response
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorV2
import org.opensearch.commons.alerting.model.PPLMonitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.commons.alerting.util.AlertingException
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
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val scheduledJobIndices: ScheduledJobIndices,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
    val namedWriteableRegistry: NamedWriteableRegistry,
) : HandledTransportAction<IndexMonitorV2Request, IndexMonitorV2Response>(
    AlertingActions.INDEX_MONITOR_V2_ACTION_NAME, transportService, actionFilters, ::IndexMonitorV2Request
),
    SecureTransportAction {

    // TODO: add monitor v2 versions of these settings
    @Volatile private var maxMonitors = ALERTING_MAX_MONITORS.get(settings)
    @Volatile private var requestTimeout = REQUEST_TIMEOUT.get(settings)
    @Volatile private var indexTimeout = INDEX_TIMEOUT.get(settings)
//    @Volatile private var maxActionThrottle = MAX_ACTION_THROTTLE_VALUE.get(settings)
    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    override fun doExecute(task: Task, indexMonitorRequest: IndexMonitorV2Request, actionListener: ActionListener<IndexMonitorV2Response>) {
        /* check to see if alerting-config index (scheduled job index) is created and updated before indexing MonitorV2 into it */
        if (!scheduledJobIndices.scheduledJobIndexExists()) { // if alerting-config index doesn't exist, send request to create it
            scheduledJobIndices.initScheduledJobIndex(object : ActionListener<CreateIndexResponse> {
                override fun onResponse(response: CreateIndexResponse) {
                    onCreateMappingsResponse(response.isAcknowledged, indexMonitorRequest, actionListener)
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
                            onCreateMappingsResponse(true, indexMonitorRequest, actionListener)
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
                        onUpdateMappingsResponse(response, indexMonitorRequest, actionListener)
                    }
                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        } else {
            prepareMonitorIndexing(indexMonitorRequest, actionListener)
        }
    }

    private fun onCreateMappingsResponse(
        isAcknowledged: Boolean,
        request: IndexMonitorV2Request,
        actionListener: ActionListener<IndexMonitorV2Response>
    ) {
        if (isAcknowledged) {
            log.info("Created $SCHEDULED_JOBS_INDEX with mappings.")
            prepareMonitorIndexing(request, actionListener)
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
        actionListener: ActionListener<IndexMonitorV2Response>
    ) {
        if (response.isAcknowledged) {
            log.info("Updated  $SCHEDULED_JOBS_INDEX with mappings.")
            IndexUtils.scheduledJobIndexUpdated()
            prepareMonitorIndexing(indexMonitorRequest, actionListener)
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
    private fun prepareMonitorIndexing(indexMonitorRequest: IndexMonitorV2Request, actionListener: ActionListener<IndexMonitorV2Response>) {

        // Below check needs to be async operations and needs to be refactored issue#269
        // checkForDisallowedDestinations(allowList)

        // TODO: checks for throttling/suppression, should not be needed here, done in common utils when parsing PPLTriggers
//        try {
//            validateActionThrottle(request.monitor, maxActionThrottle, TimeValue.timeValueMinutes(1))
//        } catch (e: RuntimeException) {
//            actionListener.onFailure(AlertingException.wrap(e))
//            return
//        }

        if (indexMonitorRequest.method == RestRequest.Method.PUT) { // update monitor case
            scope.launch {
                updateMonitor(indexMonitorRequest, actionListener)
            }
        } else { // create monitor case
            val query = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("${Monitor.MONITOR_TYPE}.type", Monitor.MONITOR_TYPE))
            val searchSource = SearchSourceBuilder().query(query).timeout(requestTimeout)
            val searchRequest = SearchRequest(SCHEDULED_JOBS_INDEX).source(searchSource)

            client.search(
                searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(searchResponse: SearchResponse) {
                        onMonitorCountSearchResponse(searchResponse, indexMonitorRequest, actionListener)
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        }
    }

    /* Functions for Update Monitor flow */

    private suspend fun updateMonitor(indexMonitorRequest: IndexMonitorV2Request, actionListener: ActionListener<IndexMonitorV2Response>) {
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
            onGetMonitorResponseForUpdate(monitorV2, indexMonitorRequest, actionListener)
        } catch (t: Exception) {
            actionListener.onFailure(AlertingException.wrap(t))
        }
    }

    private suspend fun onGetMonitorResponseForUpdate(
        currentMonitorV2: MonitorV2,
        indexMonitorRequest: IndexMonitorV2Request,
        actionListener: ActionListener<IndexMonitorV2Response>
    ) {
        var newMonitorV2 = when (indexMonitorRequest.monitorV2) {
            is PPLMonitor -> indexMonitorRequest.monitorV2 as PPLMonitor
            else -> throw IllegalArgumentException("received unsupported monitor type to index: ${indexMonitorRequest.monitorV2.javaClass}")
        }

        if (currentMonitorV2 !is PPLMonitor) {
            throw IllegalStateException(
                "During update, existing monitor ${currentMonitorV2.id} had unexpected type ${currentMonitorV2::class.java}"
            )
        }

        if (newMonitorV2.enabled && currentMonitorV2.enabled) {
            newMonitorV2 = newMonitorV2.copy(enabledTime = currentMonitorV2.enabledTime)
        }

        newMonitorV2 = newMonitorV2.copy(schemaVersion = IndexUtils.scheduledJobIndexSchemaVersion)
        val indexRequest = IndexRequest(SCHEDULED_JOBS_INDEX)
            .setRefreshPolicy(indexMonitorRequest.refreshPolicy)
            .source(newMonitorV2.toXContent(jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
            .id(indexMonitorRequest.monitorId)
            .setIfSeqNo(indexMonitorRequest.seqNo)
            .setIfPrimaryTerm(indexMonitorRequest.primaryTerm)
            .timeout(indexTimeout)

        log.info(
            "Updating monitor, ${currentMonitorV2.id}, from: ${currentMonitorV2.toXContent(
                jsonBuilder(),
                ToXContent.MapParams(mapOf("with_type" to "true"))
            )} \n to: ${newMonitorV2.toXContent(jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true")))}"
        )

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
        actionListener: ActionListener<IndexMonitorV2Response>
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
                indexMonitor(indexMonitorRequest, actionListener)
            }
        }
    }

    private suspend fun indexMonitor(indexMonitorRequest: IndexMonitorV2Request, actionListener: ActionListener<IndexMonitorV2Response>) {
        // TODO: user permissions for PPL alerting
//        if (user != null) {
//            // Use the backend roles which is an intersection of the requested backend roles and the user's backend roles.
//            // Admins can pass in any backend role. Also if no backend role is passed in, all the user's backend roles are used.
//            val rbacRoles = if (request.rbacRoles == null) user.backendRoles.toSet()
//            else if (!isAdmin(user)) request.rbacRoles?.intersect(user.backendRoles)?.toSet()
//            else request.rbacRoles
//
//            request.monitor = request.monitor.copy(
//                user = User(user.name, rbacRoles.orEmpty().toList(), user.roles, user.customAttNames)
//            )
//            log.debug("Created monitor's backend roles: $rbacRoles")
//        }
        var monitorV2 = when (indexMonitorRequest.monitorV2) {
            is PPLMonitor -> indexMonitorRequest.monitorV2 as PPLMonitor
            else -> throw IllegalArgumentException("received unsupported monitor type to index: ${indexMonitorRequest.monitorV2.javaClass}")
        }

        val indexRequest = IndexRequest(SCHEDULED_JOBS_INDEX)
            .setRefreshPolicy(indexMonitorRequest.refreshPolicy)
            .source(monitorV2.toXContent(jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
            .setIfSeqNo(indexMonitorRequest.seqNo)
            .setIfPrimaryTerm(indexMonitorRequest.primaryTerm)
            .timeout(indexTimeout)

        log.info(
            "Creating new monitorV2: ${monitorV2.toXContent(
                jsonBuilder(),
                ToXContent.MapParams(mapOf("with_type" to "true"))
            )}"
        )

        try {
            val indexResponse: IndexResponse = client.suspendUntil { client.index(indexRequest, it) }
            val failureReasons = checkShardsFailure(indexResponse)
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

    // TODO: copied from V1 TransportIndexMonitorAction, abstract this out into a util function
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
}
