/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.alerting.monitor.fanouts.TransportRemoteDocLevelMonitorFanOutAction;
import org.opensearch.alerting.monitor.runners.SampleRemoteDocLevelMonitorRunner;
import org.opensearch.alerting.monitor.runners.SampleRemoteMonitorRunner1;
import org.opensearch.alerting.monitor.runners.SampleRemoteMonitorRunner2;
import org.opensearch.alerting.spi.RemoteMonitorRunner;
import org.opensearch.alerting.spi.RemoteMonitorRunnerExtension;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;
import reactor.util.annotation.NonNull;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class SampleRemoteMonitorPlugin extends Plugin implements ActionPlugin, RemoteMonitorRunnerExtension {

    public static final String SAMPLE_REMOTE_MONITOR1 = "sample_remote_monitor1";

    public static final String SAMPLE_REMOTE_MONITOR2 = "sample_remote_monitor2";

    public static final String SAMPLE_REMOTE_DOC_LEVEL_MONITOR = "remote_doc_level_monitor";

    private static final Logger log = LogManager.getLogger(SampleRemoteMonitorPlugin.class);

    @Override
    public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        SampleRemoteMonitorRunner1 monitorRunner1 = SampleRemoteMonitorRunner1.getMonitorRunner();
        monitorRunner1.setClient(client);
        SampleRemoteMonitorRunner2 monitorRunner2 = SampleRemoteMonitorRunner2.getMonitorRunner();
        monitorRunner2.setClient(client);
        return Collections.emptyList();
    }

    @NonNull
    @Override
    public Map<String, RemoteMonitorRunner> getMonitorTypesToMonitorRunners() {
        return Map.of(SAMPLE_REMOTE_MONITOR1, SampleRemoteMonitorRunner1.getMonitorRunner(),
        SAMPLE_REMOTE_MONITOR2, SampleRemoteMonitorRunner2.getMonitorRunner(),
        SAMPLE_REMOTE_DOC_LEVEL_MONITOR, SampleRemoteDocLevelMonitorRunner.getMonitorRunner());
    }

    @Override
    public List<RestHandler> getRestHandlers(
            Settings settings,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return Collections.singletonList(new SampleRemoteMonitorRestHandler());
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
                new ActionPlugin.ActionHandler<>(SampleRemoteDocLevelMonitorRunner.REMOTE_DOC_LEVEL_MONITOR_ACTION_INSTANCE, TransportRemoteDocLevelMonitorFanOutAction.class)
        );
    }
}