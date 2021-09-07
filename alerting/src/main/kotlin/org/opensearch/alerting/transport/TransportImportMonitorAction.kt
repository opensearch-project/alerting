/*
 *   Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.ImportMonitorAction
import org.opensearch.alerting.action.ImportMonitorRequest
import org.opensearch.alerting.action.ImportMonitorResponse
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.core.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_MAX_MONITORS
import org.opensearch.alerting.settings.AlertingSettings.Companion.FILTER_BY_BACKEND_ROLES
import org.opensearch.alerting.settings.AlertingSettings.Companion.INDEX_TIMEOUT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MAX_ACTION_THROTTLE_VALUE
import org.opensearch.alerting.settings.AlertingSettings.Companion.REQUEST_TIMEOUT
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.checkFilterByUserBackendRoles
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.time.Duration

private val log = LogManager.getLogger(TransportImportMonitorAction::class.java)

class TransportImportMonitorAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val scheduledJobIndices: ScheduledJobIndices,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<ImportMonitorRequest, ImportMonitorResponse>(
    ImportMonitorAction.NAME, transportService, actionFilters, ::ImportMonitorRequest
) {

    @Volatile private var maxMonitors = ALERTING_MAX_MONITORS.get(settings)
    @Volatile private var requestTimeout = REQUEST_TIMEOUT.get(settings)
    @Volatile private var indexTimeout = INDEX_TIMEOUT.get(settings)
    @Volatile private var maxActionThrottle = MAX_ACTION_THROTTLE_VALUE.get(settings)
    @Volatile private var allowList = ALLOW_LIST.get(settings)
    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_MAX_MONITORS) { maxMonitors = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(REQUEST_TIMEOUT) { requestTimeout = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(INDEX_TIMEOUT) { indexTimeout = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(MAX_ACTION_THROTTLE_VALUE) { maxActionThrottle = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) { allowList = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) { filterByEnabled = it }
    }

    override fun doExecute(task: Task, request: ImportMonitorRequest, actionListener: ActionListener<ImportMonitorResponse>) {

        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
        log.debug("User and roles string from thread context: $userStr")
        val user: User? = User.parse(userStr)

        if (!checkFilterByUserBackendRoles(filterByEnabled, user, actionListener)) {
            return
        }

        checkIndicesAndExecute(client, actionListener, request, user)
    }

    /**
     *  Check if user has permissions to read the configured indices on each monitor and
     *  then create monitor.
     */
    fun checkIndicesAndExecute(
        client: Client,
        actionListener: ActionListener<ImportMonitorResponse>,
        request: ImportMonitorRequest,
        user: User?
    ) {
        // TODO: Check search permissions for all indices

        client.threadPool().threadContext.stashContext().use {
            BulkIndexMonitorHandler(client, actionListener, request, user).resolveUserAndStart()
        }
    }

    inner class BulkIndexMonitorHandler(
        private val client: Client,
        private val actionListener: ActionListener<ImportMonitorResponse>,
        private val request: ImportMonitorRequest,
        private val user: User?
    ) {

        fun resolveUserAndStart() {
            if (user == null) {
                // Security is disabled, add empty user to Monitor. user is null for older versions.
                for (monitorIndex in request.monitors.indices) {
                    request.monitors[monitorIndex] = request.monitors[monitorIndex]
                        .copy(user = User("", listOf(), listOf(), listOf()))
                }
            } else {
                for (monitorIndex in request.monitors.indices) {
                    request.monitors[monitorIndex] = request.monitors[monitorIndex]
                        .copy(user = User(user.name, user.backendRoles, user.roles, user.customAttNames))
                }
            }

            start()
        }

        fun start() {
            if (!scheduledJobIndices.scheduledJobIndexExists()) {
                scheduledJobIndices.initScheduledJobIndex(object : ActionListener<CreateIndexResponse> {
                    override fun onResponse(response: CreateIndexResponse) {
                        onCreateMappingsResponse(response)
                    }
                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                })
            }
            else {
                prepareMonitorIndexing()
            }
        }

        /**
         * This function prepares for indexing a new monitor.
         * We first check to see how many monitors already exist, and compare this to the [maxMonitorCount].
         * Requests that breach this threshold will be rejected.
         */
        private fun prepareMonitorIndexing() {
            for (monitor in request.monitors) {
                try {
                    validateActionThrottle(monitor, maxActionThrottle, TimeValue.timeValueMinutes(1))
                } catch (e: RuntimeException) {
                    actionListener.onFailure(AlertingException.wrap(e))
                    return
                }
            }

            val query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("${Monitor.MONITOR_TYPE}.type", Monitor.MONITOR_TYPE))
            val searchSource = SearchSourceBuilder()
                .query(query)
                .timeout(requestTimeout)
            val searchRequest = SearchRequest(SCHEDULED_JOBS_INDEX)
                .source(searchSource)
            client.search(searchRequest, object : ActionListener<SearchResponse> {
                override fun onResponse(searchResponse: SearchResponse) {
                    onSearchResponse(searchResponse)
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(AlertingException.wrap(t))
                }
            })
        }

        private fun validateActionThrottle(monitor: Monitor, maxValue: TimeValue, minValue: TimeValue) {
            monitor.triggers.forEach { trigger ->
                trigger.actions.forEach { action ->
                    if (action.throttle != null) {
                        require(TimeValue(Duration.of(action.throttle.value.toLong(), action.throttle.unit).toMillis())
                            .compareTo(maxValue) <= 0, { "Can only set throttle period less than or equal to $maxValue" })
                        require(TimeValue(Duration.of(action.throttle.value.toLong(), action.throttle.unit).toMillis())
                            .compareTo(minValue) >= 0, { "Can only set throttle period greater than or equal to $minValue" })
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
                log.error("This request would create more than the allowed monitors [$maxMonitors].")
                actionListener.onFailure(
                    AlertingException.wrap(IllegalArgumentException(
                        "This request would create more than the allowed monitors [$maxMonitors]."))
                )
            } else {
                indexMonitor()
            }
        }

        private fun onCreateMappingsResponse(response: CreateIndexResponse) {
            if (response.isAcknowledged) {
                log.info("Created $SCHEDULED_JOBS_INDEX with mappings.")
                prepareMonitorIndexing()
            } else {
                log.error("Create $SCHEDULED_JOBS_INDEX mappings call not acknowledged.")
                actionListener.onFailure(AlertingException.wrap(OpenSearchStatusException(
                    "Create $SCHEDULED_JOBS_INDEX mappings call not acknowledged", RestStatus.INTERNAL_SERVER_ERROR))
                )
            }
        }

        private fun indexMonitor() {
            val bulkRequest = BulkRequest()

            for (monitorIndex in request.monitors.indices) {
                request.monitors[monitorIndex] = request.monitors[monitorIndex]
                    .copy(schemaVersion = IndexUtils.scheduledJobIndexSchemaVersion)

                bulkRequest.add(
                    IndexRequest(SCHEDULED_JOBS_INDEX)
                        .source(request.monitors[monitorIndex]
                            .toXContent(jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
                        .timeout(indexTimeout)
                )
            }

            client.bulk(bulkRequest, object : ActionListener<BulkResponse> {
                override fun onResponse(response: BulkResponse) {
                    // TODO: All monitor-creation failures happen in RestHandler, so failures need to passed here later.

                    var successful = 0
                    var failed = 0

                    for (bulkResponseItem in response.items) {
                        if (bulkResponseItem.isFailed) {
                            failed += 1
                        } else {
                            successful += 1
                        }
                    }

                    actionListener.onResponse(
                        ImportMonitorResponse(request.monitors.size, successful, failed)
                    )
                }
                override fun onFailure(t: Exception) {
                    actionListener.onFailure(AlertingException.wrap(t))
                }
            })
        }
    }
}
