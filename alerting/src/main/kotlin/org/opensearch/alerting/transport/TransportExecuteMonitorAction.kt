/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchSecurityException
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.MonitorMetadataService
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.action.ExecuteMonitorAction
import org.opensearch.alerting.action.ExecuteMonitorRequest
import org.opensearch.alerting.action.ExecuteMonitorResponse
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.isClusterMetricsMonitor
import org.opensearch.alerting.util.isUnsupportedMultiTenantMonitorType
import org.opensearch.alerting.util.use
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.FeatureFlags
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelMonitorInput.Companion.DOC_LEVEL_INPUT_FIELD
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.alerting.model.remote.monitors.RemoteDocLevelMonitorInput
import org.opensearch.commons.alerting.model.remote.monitors.RemoteDocLevelMonitorInput.Companion.REMOTE_DOC_LEVEL_MONITOR_INPUT_FIELD
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.alerting.util.isMonitorOfStandardType
import org.opensearch.commons.alerting.util.isPPLMonitor
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.TenantContext
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.query.QueryBuilders
import org.opensearch.remote.metadata.client.GetDataObjectRequest
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.remote.metadata.common.SdkClientUtils
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.time.Instant
import java.util.Locale

private val log = LogManager.getLogger(TransportExecuteMonitorAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportExecuteMonitorAction @Inject constructor(
    private val transportService: TransportService,
    private val client: Client,
    private val clusterService: ClusterService,
    private val runner: MonitorRunnerService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    private val docLevelMonitorQueries: DocLevelMonitorQueries,
    private val settings: Settings,
    private val sdkClient: SdkClient
) : HandledTransportAction<ExecuteMonitorRequest, ExecuteMonitorResponse> (
    ExecuteMonitorAction.NAME, transportService, actionFilters, ::ExecuteMonitorRequest
),
    SecureTransportAction {
    @Volatile private var indexTimeout = AlertingSettings.INDEX_TIMEOUT.get(settings)

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    @Volatile
    override var filterByAccessStrategy = AlertingSettings.FILTER_BY_BACKEND_ROLES_ACCESS_STRATEGY.get(settings)

    private val multiTenancyEnabled = AlertingSettings.MULTI_TENANCY_ENABLED.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, execMonitorRequest: ExecuteMonitorRequest, actionListener: ActionListener<ExecuteMonitorResponse>) {

        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
        log.debug("User and roles string from thread context: $userStr")
        val user: User? = User.parse(userStr)

        val tenantId = client.threadPool().threadContext.getHeader(AlertingPlugin.TENANT_ID_HEADER)

        if (execMonitorRequest.monitorId != null && execMonitorRequest.monitor == null) {
            val monitorId = execMonitorRequest.monitorId
            // Existing monitor referenced by ID. Its inputs and data sources were already validated against
            // the caller's permissions at creation time, and an explicit permission check is performed below,
            // so it is safe to stash the context here.
            client.threadPool().threadContext.stashContext().use {
                executeExistingMonitor(execMonitorRequest, monitorId, user, tenantId, actionListener)
            }
        } else {
            // Inline monitor definition supplied in the request body. Validate the caller's access to the
            // configured input indices and data sources using the caller's OWN security context BEFORE
            // stashing it, so the execute path performs the same validation as the monitor creation path.
            val monitor = when (user?.name.isNullOrEmpty()) {
                true -> execMonitorRequest.monitor as Monitor
                false -> (execMonitorRequest.monitor as Monitor).copy(user = user)
            }

            if (multiTenancyEnabled && monitor.isUnsupportedMultiTenantMonitorType()) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException(
                            "${monitor.monitorType} monitors are not allowed when multi-tenancy is enabled.",
                            RestStatus.METHOD_NOT_ALLOWED
                        )
                    )
                )
                return
            }

            // Block non-PPL monitors on pluggable dataformat domains
            if (FeatureFlags.isEnabled(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG) &&
                !monitor.isPPLMonitor() && !monitor.isClusterMetricsMonitor()
            ) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException(
                            "Monitor execution failed. ${monitor.monitorType} type and other DSL-based monitors " +
                                "are not supported on this domain type. This domain supports PPL as the query language for " +
                                "alert monitors. It also supports cluster metrics monitors. Please create one of these monitor " +
                                "types instead.",
                            RestStatus.FORBIDDEN
                        )
                    )
                )
                return
            }

            checkIndicesAndExecute(monitor, execMonitorRequest, tenantId, actionListener)
        }
    }

    /**
     * Verifies that the caller has read access to the inline monitor's configured input indices using the
     * caller's own security context, and only then stashes the context and executes the monitor. This mirrors
     * [TransportIndexMonitorAction.checkIndicesAndExecute] so the execute path applies the same permission
     * check as the monitor creation path.
     */
    private fun checkIndicesAndExecute(
        monitor: Monitor,
        execMonitorRequest: ExecuteMonitorRequest,
        tenantId: String?,
        actionListener: ActionListener<ExecuteMonitorResponse>,
    ) {
        val indices = mutableListOf<String>()
        val searchInputs = monitor.inputs.filter {
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

        // No configured input indices to validate; safe to stash and execute.
        if (indices.isEmpty()) {
            client.threadPool().threadContext.stashContext().use {
                executeInlineMonitor(monitor, execMonitorRequest, tenantId, actionListener)
            }
            return
        }

        val updatedIndices = indices.map { index ->
            if (IndexUtils.isAlias(index, clusterService.state()) || IndexUtils.isDataStream(index, clusterService.state())) {
                val metadata = clusterService.state().metadata.indicesLookup[index]?.writeIndex
                metadata?.index?.name ?: index
            } else {
                index
            }
        }

        // Test search executed with the caller's security context (context not yet stashed).
        val searchRequest = SearchRequest().indices(*updatedIndices.toTypedArray())
            .source(SearchSourceBuilder.searchSource().size(1).query(QueryBuilders.matchAllQuery()))
        client.search(
            searchRequest,
            object : ActionListener<SearchResponse> {
                override fun onResponse(searchResponse: SearchResponse) {
                    // Caller has read access to the configured indices; now stash context and execute.
                    client.threadPool().threadContext.stashContext().use {
                        executeInlineMonitor(monitor, execMonitorRequest, tenantId, actionListener)
                    }
                }

                //  Due to below issue with security plugin, we get security_exception when invalid index name is mentioned.
                //  https://github.com/opendistro-for-elasticsearch/security/issues/718
                override fun onFailure(t: Exception) {
                    actionListener.onFailure(
                        AlertingException.wrap(
                            when (t is OpenSearchSecurityException) {
                                true -> OpenSearchStatusException(
                                    "User doesn't have read permissions for one or more configured index $indices",
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

    private fun executeExistingMonitor(
        execMonitorRequest: ExecuteMonitorRequest,
        monitorId: String,
        user: User?,
        tenantId: String?,
        actionListener: ActionListener<ExecuteMonitorResponse>,
    ) {
        val getRequest = GetDataObjectRequest.builder()
            .index(ScheduledJob.SCHEDULED_JOBS_INDEX)
            .id(monitorId)
            .tenantId(tenantId)
            .build()
        sdkClient.getDataObjectAsync(getRequest).whenComplete { response, throwable ->
            if (throwable != null) {
                actionListener.onFailure(AlertingException.wrap(SdkClientUtils.unwrapAndConvertToException(throwable)))
                return@whenComplete
            }
            try {
                val getResponse = response.getResponse()
                if (getResponse == null || !getResponse.isExists) {
                    actionListener.onFailure(
                        AlertingException.wrap(
                            OpenSearchStatusException(
                                "Can't find monitor with id: $monitorId",
                                RestStatus.NOT_FOUND
                            )
                        )
                    )
                    return@whenComplete
                }
                if (getResponse.isSourceEmpty) {
                    actionListener.onFailure(
                        AlertingException.wrap(
                            OpenSearchStatusException(
                                "Monitor source is empty for id: $monitorId",
                                RestStatus.NOT_FOUND
                            )
                        )
                    )
                    return@whenComplete
                }
                XContentHelper.createParser(
                    xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    getResponse.sourceAsBytesRef, XContentType.JSON
                ).use { xcp ->
                    val monitor = ScheduledJob.parse(xcp, getResponse.id, getResponse.version) as Monitor

                    if (multiTenancyEnabled && monitor.isUnsupportedMultiTenantMonitorType()) {
                        actionListener.onFailure(
                            AlertingException.wrap(
                                OpenSearchStatusException(
                                    "${monitor.monitorType} monitors are not allowed when multi-tenancy is enabled.",
                                    RestStatus.METHOD_NOT_ALLOWED
                                )
                            )
                        )
                        return@whenComplete
                    }

                    // RBAC check: verify calling user has permissions to this monitor
                    if (!checkUserPermissionsWithResource(
                            user, monitor.user, actionListener,
                            "monitor", monitorId
                        )
                    ) {
                        return@whenComplete
                    }

                    launchExecuteMonitor(monitor, execMonitorRequest, tenantId, actionListener)
                }
            } catch (e: Exception) {
                log.error("Failed to get monitor $monitorId for execution", e)
                actionListener.onFailure(AlertingException.wrap(e))
            }
        }
    }

    private fun executeInlineMonitor(
        monitor: Monitor,
        execMonitorRequest: ExecuteMonitorRequest,
        tenantId: String?,
        actionListener: ActionListener<ExecuteMonitorResponse>,
    ) {
        if (
            monitor.isMonitorOfStandardType() &&
            Monitor.MonitorType.valueOf(monitor.monitorType.uppercase(Locale.ROOT)) == Monitor.MonitorType.DOC_LEVEL_MONITOR
        ) {
            try {
                scope.launch(TenantContext(tenantId)) {
                    if (!docLevelMonitorQueries.docLevelQueryIndexExists(monitor.dataSources)) {
                        docLevelMonitorQueries.initDocLevelQueryIndex(monitor.dataSources)
                        log.info("Central Percolation index ${ScheduledJob.DOC_LEVEL_QUERIES_INDEX} created")
                    }
                    val (metadata, _) = MonitorMetadataService.getOrCreateMetadata(monitor, skipIndex = true)
                    docLevelMonitorQueries.indexDocLevelQueries(
                        monitor,
                        monitor.id,
                        metadata,
                        WriteRequest.RefreshPolicy.IMMEDIATE,
                        indexTimeout
                    )
                    log.info("Queries inserted into Percolate index ${ScheduledJob.DOC_LEVEL_QUERIES_INDEX}")
                    launchExecuteMonitor(monitor, execMonitorRequest, tenantId, actionListener)
                }
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        } else {
            launchExecuteMonitor(monitor, execMonitorRequest, tenantId, actionListener)
        }
    }

    private fun launchExecuteMonitor(
        monitor: Monitor,
        execMonitorRequest: ExecuteMonitorRequest,
        tenantId: String?,
        actionListener: ActionListener<ExecuteMonitorResponse>,
    ) {
        // Launch the coroutine with the clients threadContext. This is needed to preserve authentication information
        // stored on the threadContext set by the security plugin when using the Alerting plugin with the Security plugin.
        // runner.launch(ElasticThreadContextElement(client.threadPool().threadContext)) {
        runner.launch(TenantContext(tenantId)) {
            val (periodStart, periodEnd) = if (execMonitorRequest.requestStart != null) {
                Pair(
                    Instant.ofEpochMilli(execMonitorRequest.requestStart.millis),
                    Instant.ofEpochMilli(execMonitorRequest.requestEnd.millis)
                )
            } else {
                monitor.schedule.getPeriodEndingAt(Instant.ofEpochMilli(execMonitorRequest.requestEnd.millis))
            }
            try {
                log.info(
                    "Executing monitor from API - id: ${monitor.id}, type: ${monitor.monitorType}, " +
                        "periodStart: $periodStart, periodEnd: $periodEnd, dryrun: ${execMonitorRequest.dryrun}"
                )
                val monitorRunResult = runner.runJob(
                    monitor,
                    periodStart,
                    periodEnd,
                    execMonitorRequest.dryrun,
                    transportService
                )
                withContext(Dispatchers.IO) {
                    actionListener.onResponse(ExecuteMonitorResponse(monitorRunResult))
                }
            } catch (e: Exception) {
                log.error("Unexpected error running monitor", e)
                withContext(Dispatchers.IO) {
                    actionListener.onFailure(AlertingException.wrap(e))
                }
            }
        }
    }
}
