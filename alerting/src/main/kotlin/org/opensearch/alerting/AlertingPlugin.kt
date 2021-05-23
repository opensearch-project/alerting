/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package org.opensearch.alerting

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionResponse
import org.opensearch.alerting.action.AcknowledgeAlertAction
import org.opensearch.alerting.action.DeleteDestinationAction
import org.opensearch.alerting.action.DeleteEmailAccountAction
import org.opensearch.alerting.action.DeleteEmailGroupAction
import org.opensearch.alerting.action.DeleteMonitorAction
import org.opensearch.alerting.action.ExecuteMonitorAction
import org.opensearch.alerting.action.GetAlertsAction
import org.opensearch.alerting.action.GetDestinationsAction
import org.opensearch.alerting.action.GetEmailAccountAction
import org.opensearch.alerting.action.GetEmailGroupAction
import org.opensearch.alerting.action.GetMonitorAction
import org.opensearch.alerting.action.IndexDestinationAction
import org.opensearch.alerting.action.IndexEmailAccountAction
import org.opensearch.alerting.action.IndexEmailGroupAction
import org.opensearch.alerting.action.IndexMonitorAction
import org.opensearch.alerting.action.SearchEmailAccountAction
import org.opensearch.alerting.action.SearchEmailGroupAction
import org.opensearch.alerting.action.SearchMonitorAction
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.core.JobSweeper
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.core.action.node.ScheduledJobsStatsAction
import org.opensearch.alerting.core.action.node.ScheduledJobsStatsTransportAction
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.core.resthandler.RestScheduledJobStatsHandler
import org.opensearch.alerting.core.schedule.JobScheduler
import org.opensearch.alerting.core.settings.ScheduledJobSettings
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.resthandler.RestAcknowledgeAlertAction
import org.opensearch.alerting.resthandler.RestDeleteDestinationAction
import org.opensearch.alerting.resthandler.RestDeleteEmailAccountAction
import org.opensearch.alerting.resthandler.RestDeleteEmailGroupAction
import org.opensearch.alerting.resthandler.RestDeleteMonitorAction
import org.opensearch.alerting.resthandler.RestExecuteMonitorAction
import org.opensearch.alerting.resthandler.RestGetAlertsAction
import org.opensearch.alerting.resthandler.RestGetDestinationsAction
import org.opensearch.alerting.resthandler.RestGetEmailAccountAction
import org.opensearch.alerting.resthandler.RestGetEmailGroupAction
import org.opensearch.alerting.resthandler.RestGetMonitorAction
import org.opensearch.alerting.resthandler.RestIndexDestinationAction
import org.opensearch.alerting.resthandler.RestIndexEmailAccountAction
import org.opensearch.alerting.resthandler.RestIndexEmailGroupAction
import org.opensearch.alerting.resthandler.RestIndexMonitorAction
import org.opensearch.alerting.resthandler.RestSearchEmailAccountAction
import org.opensearch.alerting.resthandler.RestSearchEmailGroupAction
import org.opensearch.alerting.resthandler.RestSearchMonitorAction
import org.opensearch.alerting.script.TriggerScript
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.DestinationSettings
import org.opensearch.alerting.transport.TransportAcknowledgeAlertAction
import org.opensearch.alerting.transport.TransportDeleteDestinationAction
import org.opensearch.alerting.transport.TransportDeleteEmailAccountAction
import org.opensearch.alerting.transport.TransportDeleteEmailGroupAction
import org.opensearch.alerting.transport.TransportDeleteMonitorAction
import org.opensearch.alerting.transport.TransportExecuteMonitorAction
import org.opensearch.alerting.transport.TransportGetAlertsAction
import org.opensearch.alerting.transport.TransportGetDestinationsAction
import org.opensearch.alerting.transport.TransportGetEmailAccountAction
import org.opensearch.alerting.transport.TransportGetEmailGroupAction
import org.opensearch.alerting.transport.TransportGetMonitorAction
import org.opensearch.alerting.transport.TransportIndexDestinationAction
import org.opensearch.alerting.transport.TransportIndexEmailAccountAction
import org.opensearch.alerting.transport.TransportIndexEmailGroupAction
import org.opensearch.alerting.transport.TransportIndexMonitorAction
import org.opensearch.alerting.transport.TransportSearchEmailAccountAction
import org.opensearch.alerting.transport.TransportSearchEmailGroupAction
import org.opensearch.alerting.transport.TransportSearchMonitorAction
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.node.DiscoveryNodes
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.io.stream.NamedWriteableRegistry
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.IndexScopedSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.settings.SettingsFilter
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.env.Environment
import org.opensearch.env.NodeEnvironment
import org.opensearch.index.IndexModule
import org.opensearch.painless.spi.PainlessExtension
import org.opensearch.painless.spi.Whitelist
import org.opensearch.painless.spi.WhitelistLoader
import org.opensearch.plugins.ActionPlugin
import org.opensearch.plugins.Plugin
import org.opensearch.plugins.ReloadablePlugin
import org.opensearch.plugins.ScriptPlugin
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
 * It also adds [Monitor.XCONTENT_REGISTRY], [SearchInput.XCONTENT_REGISTRY] to the
 * [NamedXContentRegistry] so that we are able to deserialize the custom named objects.
 */
internal class AlertingPlugin : PainlessExtension, ActionPlugin, ScriptPlugin, ReloadablePlugin, Plugin() {

    override fun getContextWhitelists(): Map<ScriptContext<*>, List<Whitelist>> {
        val whitelist = WhitelistLoader.loadFromResourceFiles(javaClass, "org.opensearch.alerting.txt")
        return mapOf(TriggerScript.CONTEXT to listOf(whitelist))
    }

    companion object {
        @JvmField val OPEN_SEARCH_DASHBOARDS_USER_AGENT = "OpenSearch-Dashboards"
        @JvmField val UI_METADATA_EXCLUDE = arrayOf("monitor.${Monitor.UI_METADATA_FIELD}")
        @JvmField val MONITOR_BASE_URI = "/_plugins/_alerting/monitors"
        @JvmField val DESTINATION_BASE_URI = "/_plugins/_alerting/destinations"
        @JvmField val LEGACY_OPENDISTRO_MONITOR_BASE_URI = "/_opendistro/_alerting/monitors"
        @JvmField val LEGACY_OPENDISTRO_DESTINATION_BASE_URI = "/_opendistro/_alerting/destinations"
        @JvmField val EMAIL_ACCOUNT_BASE_URI = "$DESTINATION_BASE_URI/email_accounts"
        @JvmField val EMAIL_GROUP_BASE_URI = "$DESTINATION_BASE_URI/email_groups"
        @JvmField val LEGACY_OPENDISTRO_EMAIL_ACCOUNT_BASE_URI = "$LEGACY_OPENDISTRO_DESTINATION_BASE_URI/email_accounts"
        @JvmField val LEGACY_OPENDISTRO_EMAIL_GROUP_BASE_URI = "$LEGACY_OPENDISTRO_DESTINATION_BASE_URI/email_groups"
        @JvmField val ALERTING_JOB_TYPES = listOf("monitor")
    }

    lateinit var runner: MonitorRunner
    lateinit var scheduler: JobScheduler
    lateinit var sweeper: JobSweeper
    lateinit var scheduledJobIndices: ScheduledJobIndices
    lateinit var threadPool: ThreadPool
    lateinit var alertIndices: AlertIndices
    lateinit var clusterService: ClusterService

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
            RestSearchMonitorAction(settings, clusterService),
            RestExecuteMonitorAction(),
            RestAcknowledgeAlertAction(),
            RestScheduledJobStatsHandler("_alerting"),
            RestIndexDestinationAction(),
            RestDeleteDestinationAction(),
            RestIndexEmailAccountAction(),
            RestDeleteEmailAccountAction(),
            RestSearchEmailAccountAction(),
            RestGetEmailAccountAction(),
            RestIndexEmailGroupAction(),
            RestDeleteEmailGroupAction(),
            RestSearchEmailGroupAction(),
            RestGetEmailGroupAction(),
            RestGetDestinationsAction(),
            RestGetAlertsAction()
        )
    }

    override fun getActions(): List<ActionPlugin.ActionHandler<out ActionRequest, out ActionResponse>> {
        return listOf(
            ActionPlugin.ActionHandler(ScheduledJobsStatsAction.INSTANCE, ScheduledJobsStatsTransportAction::class.java),
            ActionPlugin.ActionHandler(IndexDestinationAction.INSTANCE, TransportIndexDestinationAction::class.java),
            ActionPlugin.ActionHandler(IndexMonitorAction.INSTANCE, TransportIndexMonitorAction::class.java),
            ActionPlugin.ActionHandler(GetMonitorAction.INSTANCE, TransportGetMonitorAction::class.java),
            ActionPlugin.ActionHandler(ExecuteMonitorAction.INSTANCE, TransportExecuteMonitorAction::class.java),
            ActionPlugin.ActionHandler(SearchMonitorAction.INSTANCE, TransportSearchMonitorAction::class.java),
            ActionPlugin.ActionHandler(DeleteMonitorAction.INSTANCE, TransportDeleteMonitorAction::class.java),
            ActionPlugin.ActionHandler(DeleteDestinationAction.INSTANCE, TransportDeleteDestinationAction::class.java),
            ActionPlugin.ActionHandler(AcknowledgeAlertAction.INSTANCE, TransportAcknowledgeAlertAction::class.java),
            ActionPlugin.ActionHandler(IndexEmailAccountAction.INSTANCE, TransportIndexEmailAccountAction::class.java),
            ActionPlugin.ActionHandler(GetEmailAccountAction.INSTANCE, TransportGetEmailAccountAction::class.java),
            ActionPlugin.ActionHandler(SearchEmailAccountAction.INSTANCE, TransportSearchEmailAccountAction::class.java),
            ActionPlugin.ActionHandler(DeleteEmailAccountAction.INSTANCE, TransportDeleteEmailAccountAction::class.java),
            ActionPlugin.ActionHandler(IndexEmailGroupAction.INSTANCE, TransportIndexEmailGroupAction::class.java),
            ActionPlugin.ActionHandler(GetEmailGroupAction.INSTANCE, TransportGetEmailGroupAction::class.java),
            ActionPlugin.ActionHandler(SearchEmailGroupAction.INSTANCE, TransportSearchEmailGroupAction::class.java),
            ActionPlugin.ActionHandler(DeleteEmailGroupAction.INSTANCE, TransportDeleteEmailGroupAction::class.java),
            ActionPlugin.ActionHandler(GetDestinationsAction.INSTANCE, TransportGetDestinationsAction::class.java),
            ActionPlugin.ActionHandler(GetAlertsAction.INSTANCE, TransportGetAlertsAction::class.java)
        )
    }

    override fun getNamedXContent(): List<NamedXContentRegistry.Entry> {
        return listOf(Monitor.XCONTENT_REGISTRY, SearchInput.XCONTENT_REGISTRY)
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
        alertIndices = AlertIndices(settings, client, threadPool, clusterService)
        runner = MonitorRunner(settings, client, threadPool, scriptService, xContentRegistry, alertIndices, clusterService)
        scheduledJobIndices = ScheduledJobIndices(client.admin(), clusterService)
        scheduler = JobScheduler(threadPool, runner)
        sweeper = JobSweeper(environment.settings(), client, clusterService, threadPool, xContentRegistry, scheduler, ALERTING_JOB_TYPES)
        this.threadPool = threadPool
        this.clusterService = clusterService
        return listOf(sweeper, scheduler, runner, scheduledJobIndices)
    }

    override fun getSettings(): List<Setting<*>> {
        return listOf(
            ScheduledJobSettings.REQUEST_TIMEOUT,
            ScheduledJobSettings.SWEEP_BACKOFF_MILLIS,
            ScheduledJobSettings.SWEEP_BACKOFF_RETRY_COUNT,
            ScheduledJobSettings.SWEEP_PERIOD,
            ScheduledJobSettings.SWEEP_PAGE_SIZE,
            ScheduledJobSettings.SWEEPER_ENABLED,
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
            AlertingSettings.REQUEST_TIMEOUT,
            AlertingSettings.MAX_ACTION_THROTTLE_VALUE,
            AlertingSettings.FILTER_BY_BACKEND_ROLES,
            DestinationSettings.EMAIL_USERNAME,
            DestinationSettings.EMAIL_PASSWORD,
            DestinationSettings.ALLOW_LIST,
            DestinationSettings.HOST_DENY_LIST
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
}
