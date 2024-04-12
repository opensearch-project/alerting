/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.action.ActionRequest
import org.opensearch.alerting.action.ExecuteMonitorAction
import org.opensearch.alerting.action.ExecuteWorkflowAction
import org.opensearch.alerting.action.GetDestinationsAction
import org.opensearch.alerting.action.GetEmailAccountAction
import org.opensearch.alerting.action.GetEmailGroupAction
import org.opensearch.alerting.action.GetRemoteIndexesAction
import org.opensearch.alerting.action.SearchEmailAccountAction
import org.opensearch.alerting.action.SearchEmailGroupAction
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.core.JobSweeper
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.core.action.node.ScheduledJobsStatsAction
import org.opensearch.alerting.core.action.node.ScheduledJobsStatsTransportAction
import org.opensearch.alerting.core.lock.LockService
import org.opensearch.alerting.core.resthandler.RestScheduledJobStatsHandler
import org.opensearch.alerting.core.schedule.JobScheduler
import org.opensearch.alerting.core.settings.LegacyOpenDistroScheduledJobSettings
import org.opensearch.alerting.core.settings.ScheduledJobSettings
import org.opensearch.alerting.resthandler.RestAcknowledgeAlertAction
import org.opensearch.alerting.resthandler.RestAcknowledgeChainedAlertAction
import org.opensearch.alerting.resthandler.RestDeleteMonitorAction
import org.opensearch.alerting.resthandler.RestDeleteWorkflowAction
import org.opensearch.alerting.resthandler.RestExecuteMonitorAction
import org.opensearch.alerting.resthandler.RestExecuteWorkflowAction
import org.opensearch.alerting.resthandler.RestGetAlertsAction
import org.opensearch.alerting.resthandler.RestGetDestinationsAction
import org.opensearch.alerting.resthandler.RestGetEmailAccountAction
import org.opensearch.alerting.resthandler.RestGetEmailGroupAction
import org.opensearch.alerting.resthandler.RestGetFindingsAction
import org.opensearch.alerting.resthandler.RestGetMonitorAction
import org.opensearch.alerting.resthandler.RestGetRemoteIndexesAction
import org.opensearch.alerting.resthandler.RestGetWorkflowAction
import org.opensearch.alerting.resthandler.RestGetWorkflowAlertsAction
import org.opensearch.alerting.resthandler.RestIndexMonitorAction
import org.opensearch.alerting.resthandler.RestIndexWorkflowAction
import org.opensearch.alerting.resthandler.RestSearchEmailAccountAction
import org.opensearch.alerting.resthandler.RestSearchEmailGroupAction
import org.opensearch.alerting.resthandler.RestSearchMonitorAction
import org.opensearch.alerting.script.TriggerScript
import org.opensearch.alerting.service.DeleteMonitorService
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.AlertingSettings.Companion.DOC_LEVEL_MONITOR_SHARD_FETCH_SIZE
import org.opensearch.alerting.settings.DestinationSettings
import org.opensearch.alerting.settings.LegacyOpenDistroAlertingSettings
import org.opensearch.alerting.settings.LegacyOpenDistroDestinationSettings
import org.opensearch.alerting.transport.TransportAcknowledgeAlertAction
import org.opensearch.alerting.transport.TransportAcknowledgeChainedAlertAction
import org.opensearch.alerting.transport.TransportDeleteMonitorAction
import org.opensearch.alerting.transport.TransportDeleteWorkflowAction
import org.opensearch.alerting.transport.TransportExecuteMonitorAction
import org.opensearch.alerting.transport.TransportExecuteWorkflowAction
import org.opensearch.alerting.transport.TransportGetAlertsAction
import org.opensearch.alerting.transport.TransportGetDestinationsAction
import org.opensearch.alerting.transport.TransportGetEmailAccountAction
import org.opensearch.alerting.transport.TransportGetEmailGroupAction
import org.opensearch.alerting.transport.TransportGetFindingsSearchAction
import org.opensearch.alerting.transport.TransportGetMonitorAction
import org.opensearch.alerting.transport.TransportGetRemoteIndexesAction
import org.opensearch.alerting.transport.TransportGetWorkflowAction
import org.opensearch.alerting.transport.TransportGetWorkflowAlertsAction
import org.opensearch.alerting.transport.TransportIndexMonitorAction
import org.opensearch.alerting.transport.TransportIndexWorkflowAction
import org.opensearch.alerting.transport.TransportSearchEmailAccountAction
import org.opensearch.alerting.transport.TransportSearchEmailGroupAction
import org.opensearch.alerting.transport.TransportSearchMonitorAction
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.alerting.util.destinationmigration.DestinationMigrationCoordinator
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.node.DiscoveryNodes
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.IndexScopedSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.settings.SettingsFilter
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.aggregation.bucketselectorext.BucketSelectorExtAggregationBuilder
import org.opensearch.commons.alerting.model.BucketLevelTrigger
import org.opensearch.commons.alerting.model.ChainedAlertTrigger
import org.opensearch.commons.alerting.model.ClusterMetricsInput
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocumentLevelTrigger
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.QueryLevelTrigger
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.env.Environment
import org.opensearch.env.NodeEnvironment
import org.opensearch.index.IndexModule
import org.opensearch.monitor.jvm.JvmStats
import org.opensearch.painless.spi.Allowlist
import org.opensearch.painless.spi.AllowlistLoader
import org.opensearch.painless.spi.PainlessExtension
import org.opensearch.percolator.PercolatorPluginExt
import org.opensearch.plugins.ActionPlugin
import org.opensearch.plugins.ReloadablePlugin
import org.opensearch.plugins.ScriptPlugin
import org.opensearch.plugins.SearchPlugin
import org.opensearch.repositories.RepositoriesService
import org.opensearch.rest.RestController
import org.opensearch.rest.RestHandler
import org.opensearch.script.ScriptContext
import org.opensearch.script.ScriptService
import org.opensearch.threadpool.ThreadPool
import org.opensearch.watcher.ResourceWatcherService
import java.util.function.Supplier

/**
 * Entry point of the OpenDistro for Elasticsearch alerting plugin
 * This class initializes the [RestGetMonitorAction], [RestDeleteMonitorAction], [RestIndexMonitorAction] rest handlers.
 * It also adds [Monitor.XCONTENT_REGISTRY], [SearchInput.XCONTENT_REGISTRY], [QueryLevelTrigger.XCONTENT_REGISTRY],
 * [BucketLevelTrigger.XCONTENT_REGISTRY], [ClusterMetricsInput.XCONTENT_REGISTRY] to the [NamedXContentRegistry] so that we are able to deserialize the custom named objects.
 */
internal class AlertingPlugin : PainlessExtension, ActionPlugin, ScriptPlugin, ReloadablePlugin, SearchPlugin, PercolatorPluginExt() {

    override fun getContextAllowlists(): Map<ScriptContext<*>, List<Allowlist>> {
        val whitelist = AllowlistLoader.loadFromResourceFiles(javaClass, "org.opensearch.alerting.txt")
        return mapOf(TriggerScript.CONTEXT to listOf(whitelist))
    }

    companion object {
        @JvmField val OPEN_SEARCH_DASHBOARDS_USER_AGENT = "OpenSearch-Dashboards"
        @JvmField val UI_METADATA_EXCLUDE = arrayOf("monitor.${Monitor.UI_METADATA_FIELD}")
        @JvmField val MONITOR_BASE_URI = "/_plugins/_alerting/monitors"
        @JvmField val WORKFLOW_BASE_URI = "/_plugins/_alerting/workflows"
        @JvmField val REMOTE_BASE_URI = "/_plugins/_alerting/remote"
        @JvmField val DESTINATION_BASE_URI = "/_plugins/_alerting/destinations"
        @JvmField val LEGACY_OPENDISTRO_MONITOR_BASE_URI = "/_opendistro/_alerting/monitors"
        @JvmField val LEGACY_OPENDISTRO_DESTINATION_BASE_URI = "/_opendistro/_alerting/destinations"
        @JvmField val EMAIL_ACCOUNT_BASE_URI = "$DESTINATION_BASE_URI/email_accounts"
        @JvmField val EMAIL_GROUP_BASE_URI = "$DESTINATION_BASE_URI/email_groups"
        @JvmField val LEGACY_OPENDISTRO_EMAIL_ACCOUNT_BASE_URI = "$LEGACY_OPENDISTRO_DESTINATION_BASE_URI/email_accounts"
        @JvmField val LEGACY_OPENDISTRO_EMAIL_GROUP_BASE_URI = "$LEGACY_OPENDISTRO_DESTINATION_BASE_URI/email_groups"
        @JvmField val FINDING_BASE_URI = "/_plugins/_alerting/findings"

        @JvmField val ALERTING_JOB_TYPES = listOf("monitor", "workflow")
    }

    lateinit var runner: MonitorRunnerService
    lateinit var scheduler: JobScheduler
    lateinit var sweeper: JobSweeper
    lateinit var scheduledJobIndices: ScheduledJobIndices
    lateinit var docLevelMonitorQueries: DocLevelMonitorQueries
    lateinit var threadPool: ThreadPool
    lateinit var alertIndices: AlertIndices
    lateinit var clusterService: ClusterService
    lateinit var destinationMigrationCoordinator: DestinationMigrationCoordinator

    override fun getRestHandlers(
        settings: Settings,
        restController: RestController,
        clusterSettings: ClusterSettings,
        indexScopedSettings: IndexScopedSettings,
        settingsFilter: SettingsFilter,
        indexNameExpressionResolver: IndexNameExpressionResolver?,
        nodesInCluster: Supplier<DiscoveryNodes>
    ): List<RestHandler> {
        return listOf(
            RestGetMonitorAction(),
            RestDeleteMonitorAction(),
            RestIndexMonitorAction(),
            RestIndexWorkflowAction(),
            RestSearchMonitorAction(settings, clusterService),
            RestExecuteMonitorAction(),
            RestExecuteWorkflowAction(),
            RestAcknowledgeAlertAction(),
            RestAcknowledgeChainedAlertAction(),
            RestScheduledJobStatsHandler("_alerting"),
            RestSearchEmailAccountAction(),
            RestGetEmailAccountAction(),
            RestSearchEmailGroupAction(),
            RestGetEmailGroupAction(),
            RestGetDestinationsAction(),
            RestGetAlertsAction(),
            RestGetWorkflowAlertsAction(),
            RestGetFindingsAction(),
            RestGetWorkflowAction(),
            RestDeleteWorkflowAction(),
            RestGetRemoteIndexesAction(),
        )
    }

    override fun getActions(): List<ActionPlugin.ActionHandler<out ActionRequest, out ActionResponse>> {
        return listOf(
            ActionPlugin.ActionHandler(ScheduledJobsStatsAction.INSTANCE, ScheduledJobsStatsTransportAction::class.java),
            ActionPlugin.ActionHandler(AlertingActions.INDEX_MONITOR_ACTION_TYPE, TransportIndexMonitorAction::class.java),
            ActionPlugin.ActionHandler(AlertingActions.GET_MONITOR_ACTION_TYPE, TransportGetMonitorAction::class.java),
            ActionPlugin.ActionHandler(ExecuteMonitorAction.INSTANCE, TransportExecuteMonitorAction::class.java),
            ActionPlugin.ActionHandler(AlertingActions.SEARCH_MONITORS_ACTION_TYPE, TransportSearchMonitorAction::class.java),
            ActionPlugin.ActionHandler(AlertingActions.DELETE_MONITOR_ACTION_TYPE, TransportDeleteMonitorAction::class.java),
            ActionPlugin.ActionHandler(AlertingActions.ACKNOWLEDGE_ALERTS_ACTION_TYPE, TransportAcknowledgeAlertAction::class.java),
            ActionPlugin.ActionHandler(
                AlertingActions.ACKNOWLEDGE_CHAINED_ALERTS_ACTION_TYPE, TransportAcknowledgeChainedAlertAction::class.java
            ),
            ActionPlugin.ActionHandler(GetEmailAccountAction.INSTANCE, TransportGetEmailAccountAction::class.java),
            ActionPlugin.ActionHandler(SearchEmailAccountAction.INSTANCE, TransportSearchEmailAccountAction::class.java),
            ActionPlugin.ActionHandler(GetEmailGroupAction.INSTANCE, TransportGetEmailGroupAction::class.java),
            ActionPlugin.ActionHandler(SearchEmailGroupAction.INSTANCE, TransportSearchEmailGroupAction::class.java),
            ActionPlugin.ActionHandler(GetDestinationsAction.INSTANCE, TransportGetDestinationsAction::class.java),
            ActionPlugin.ActionHandler(AlertingActions.GET_ALERTS_ACTION_TYPE, TransportGetAlertsAction::class.java),
            ActionPlugin.ActionHandler(AlertingActions.GET_WORKFLOW_ALERTS_ACTION_TYPE, TransportGetWorkflowAlertsAction::class.java),
            ActionPlugin.ActionHandler(AlertingActions.GET_FINDINGS_ACTION_TYPE, TransportGetFindingsSearchAction::class.java),
            ActionPlugin.ActionHandler(AlertingActions.INDEX_WORKFLOW_ACTION_TYPE, TransportIndexWorkflowAction::class.java),
            ActionPlugin.ActionHandler(AlertingActions.GET_WORKFLOW_ACTION_TYPE, TransportGetWorkflowAction::class.java),
            ActionPlugin.ActionHandler(AlertingActions.DELETE_WORKFLOW_ACTION_TYPE, TransportDeleteWorkflowAction::class.java),
            ActionPlugin.ActionHandler(ExecuteWorkflowAction.INSTANCE, TransportExecuteWorkflowAction::class.java),
            ActionPlugin.ActionHandler(GetRemoteIndexesAction.INSTANCE, TransportGetRemoteIndexesAction::class.java),
        )
    }

    override fun getNamedXContent(): List<NamedXContentRegistry.Entry> {
        return listOf(
            Monitor.XCONTENT_REGISTRY,
            SearchInput.XCONTENT_REGISTRY,
            DocLevelMonitorInput.XCONTENT_REGISTRY,
            QueryLevelTrigger.XCONTENT_REGISTRY,
            BucketLevelTrigger.XCONTENT_REGISTRY,
            ClusterMetricsInput.XCONTENT_REGISTRY,
            DocumentLevelTrigger.XCONTENT_REGISTRY,
            ChainedAlertTrigger.XCONTENT_REGISTRY,
            Workflow.XCONTENT_REGISTRY
        )
    }

    override fun createComponents(
        client: Client,
        clusterService: ClusterService,
        threadPool: ThreadPool,
        resourceWatcherService: ResourceWatcherService,
        scriptService: ScriptService,
        xContentRegistry: NamedXContentRegistry,
        environment: Environment,
        nodeEnvironment: NodeEnvironment,
        namedWriteableRegistry: NamedWriteableRegistry,
        indexNameExpressionResolver: IndexNameExpressionResolver,
        repositoriesServiceSupplier: Supplier<RepositoriesService>
    ): Collection<Any> {
        // Need to figure out how to use the OpenSearch DI classes rather than handwiring things here.
        val settings = environment.settings()
        val lockService = LockService(client, clusterService)
        alertIndices = AlertIndices(settings, client, threadPool, clusterService)
        runner = MonitorRunnerService
            .registerClusterService(clusterService)
            .registerClient(client)
            .registerNamedXContentRegistry(xContentRegistry)
            .registerindexNameExpressionResolver(indexNameExpressionResolver)
            .registerScriptService(scriptService)
            .registerSettings(settings)
            .registerThreadPool(threadPool)
            .registerAlertIndices(alertIndices)
            .registerInputService(InputService(client, scriptService, namedWriteableRegistry, xContentRegistry, clusterService, settings))
            .registerTriggerService(TriggerService(scriptService))
            .registerAlertService(AlertService(client, xContentRegistry, alertIndices))
            .registerDocLevelMonitorQueries(DocLevelMonitorQueries(client, clusterService))
            .registerJvmStats(JvmStats.jvmStats())
            .registerWorkflowService(WorkflowService(client, xContentRegistry))
            .registerLockService(lockService)
            .registerConsumers()
            .registerDestinationSettings()
        scheduledJobIndices = ScheduledJobIndices(client.admin(), clusterService)
        docLevelMonitorQueries = DocLevelMonitorQueries(client, clusterService)
        scheduler = JobScheduler(threadPool, runner)
        sweeper = JobSweeper(environment.settings(), client, clusterService, threadPool, xContentRegistry, scheduler, ALERTING_JOB_TYPES)
        destinationMigrationCoordinator = DestinationMigrationCoordinator(client, clusterService, threadPool, scheduledJobIndices)
        this.threadPool = threadPool
        this.clusterService = clusterService

        MonitorMetadataService.initialize(
            client,
            clusterService,
            xContentRegistry,
            settings
        )

        WorkflowMetadataService.initialize(
            client,
            clusterService,
            xContentRegistry,
            settings
        )

        DeleteMonitorService.initialize(client, lockService)

        return listOf(sweeper, scheduler, runner, scheduledJobIndices, docLevelMonitorQueries, destinationMigrationCoordinator, lockService)
    }

    override fun getSettings(): List<Setting<*>> {
        return listOf(
            ScheduledJobSettings.REQUEST_TIMEOUT,
            ScheduledJobSettings.SWEEP_BACKOFF_MILLIS,
            ScheduledJobSettings.SWEEP_BACKOFF_RETRY_COUNT,
            ScheduledJobSettings.SWEEP_PERIOD,
            ScheduledJobSettings.SWEEP_PAGE_SIZE,
            ScheduledJobSettings.SWEEPER_ENABLED,
            LegacyOpenDistroScheduledJobSettings.REQUEST_TIMEOUT,
            LegacyOpenDistroScheduledJobSettings.SWEEP_BACKOFF_MILLIS,
            LegacyOpenDistroScheduledJobSettings.SWEEP_BACKOFF_RETRY_COUNT,
            LegacyOpenDistroScheduledJobSettings.SWEEP_PERIOD,
            LegacyOpenDistroScheduledJobSettings.SWEEP_PAGE_SIZE,
            LegacyOpenDistroScheduledJobSettings.SWEEPER_ENABLED,
            AlertingSettings.INPUT_TIMEOUT,
            AlertingSettings.INDEX_TIMEOUT,
            AlertingSettings.BULK_TIMEOUT,
            AlertingSettings.ALERT_BACKOFF_MILLIS,
            AlertingSettings.ALERT_BACKOFF_COUNT,
            AlertingSettings.MOVE_ALERTS_BACKOFF_MILLIS,
            AlertingSettings.MOVE_ALERTS_BACKOFF_COUNT,
            AlertingSettings.ALERT_HISTORY_ENABLED,
            AlertingSettings.ALERT_HISTORY_ROLLOVER_PERIOD,
            AlertingSettings.ALERT_HISTORY_INDEX_MAX_AGE,
            AlertingSettings.ALERT_HISTORY_MAX_DOCS,
            AlertingSettings.ALERT_HISTORY_RETENTION_PERIOD,
            AlertingSettings.ALERTING_MAX_MONITORS,
            AlertingSettings.PERCOLATE_QUERY_DOCS_SIZE_MEMORY_PERCENTAGE_LIMIT,
            DOC_LEVEL_MONITOR_SHARD_FETCH_SIZE,
            AlertingSettings.PERCOLATE_QUERY_MAX_NUM_DOCS_IN_MEMORY,
            AlertingSettings.REQUEST_TIMEOUT,
            AlertingSettings.MAX_ACTION_THROTTLE_VALUE,
            AlertingSettings.FILTER_BY_BACKEND_ROLES,
            AlertingSettings.MAX_ACTIONABLE_ALERT_COUNT,
            LegacyOpenDistroAlertingSettings.INPUT_TIMEOUT,
            LegacyOpenDistroAlertingSettings.INDEX_TIMEOUT,
            LegacyOpenDistroAlertingSettings.BULK_TIMEOUT,
            LegacyOpenDistroAlertingSettings.ALERT_BACKOFF_MILLIS,
            LegacyOpenDistroAlertingSettings.ALERT_BACKOFF_COUNT,
            LegacyOpenDistroAlertingSettings.MOVE_ALERTS_BACKOFF_MILLIS,
            LegacyOpenDistroAlertingSettings.MOVE_ALERTS_BACKOFF_COUNT,
            LegacyOpenDistroAlertingSettings.ALERT_HISTORY_ENABLED,
            LegacyOpenDistroAlertingSettings.ALERT_HISTORY_ROLLOVER_PERIOD,
            LegacyOpenDistroAlertingSettings.ALERT_HISTORY_INDEX_MAX_AGE,
            LegacyOpenDistroAlertingSettings.ALERT_HISTORY_MAX_DOCS,
            LegacyOpenDistroAlertingSettings.ALERT_HISTORY_RETENTION_PERIOD,
            LegacyOpenDistroAlertingSettings.ALERTING_MAX_MONITORS,
            LegacyOpenDistroAlertingSettings.REQUEST_TIMEOUT,
            LegacyOpenDistroAlertingSettings.MAX_ACTION_THROTTLE_VALUE,
            LegacyOpenDistroAlertingSettings.FILTER_BY_BACKEND_ROLES,
            AlertingSettings.DOC_LEVEL_MONITOR_FETCH_ONLY_QUERY_FIELDS_ENABLED,
            DestinationSettings.EMAIL_USERNAME,
            DestinationSettings.EMAIL_PASSWORD,
            DestinationSettings.ALLOW_LIST,
            DestinationSettings.HOST_DENY_LIST,
            LegacyOpenDistroDestinationSettings.EMAIL_USERNAME,
            LegacyOpenDistroDestinationSettings.EMAIL_PASSWORD,
            LegacyOpenDistroDestinationSettings.ALLOW_LIST,
            LegacyOpenDistroDestinationSettings.HOST_DENY_LIST,
            AlertingSettings.FINDING_HISTORY_ENABLED,
            AlertingSettings.FINDING_HISTORY_MAX_DOCS,
            AlertingSettings.FINDING_HISTORY_INDEX_MAX_AGE,
            AlertingSettings.FINDING_HISTORY_ROLLOVER_PERIOD,
            AlertingSettings.FINDING_HISTORY_RETENTION_PERIOD,
            AlertingSettings.FINDINGS_INDEXING_BATCH_SIZE,
            AlertingSettings.CROSS_CLUSTER_MONITORING_ENABLED
        )
    }

    override fun onIndexModule(indexModule: IndexModule) {
        if (indexModule.index.name == ScheduledJob.SCHEDULED_JOBS_INDEX) {
            indexModule.addIndexOperationListener(sweeper)
        }
    }

    override fun getContexts(): List<ScriptContext<*>> {
        return listOf(TriggerScript.CONTEXT)
    }

    override fun reload(settings: Settings) {
        runner.reloadDestinationSettings(settings)
    }

    override fun getPipelineAggregations(): List<SearchPlugin.PipelineAggregationSpec> {
        return listOf(
            SearchPlugin.PipelineAggregationSpec(
                BucketSelectorExtAggregationBuilder.NAME,
                { sin: StreamInput -> BucketSelectorExtAggregationBuilder(sin) },
                { parser: XContentParser, agg_name: String ->
                    BucketSelectorExtAggregationBuilder.parse(agg_name, parser)
                }
            )
        )
    }
}
