/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class SampleRemoteMonitorPlugin extends Plugin implements ActionPlugin, RemoteMonitorRunnerExtension {

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
        SampleRemoteMonitorRunner monitorRunner = SampleRemoteMonitorRunner.getMonitorRunner();
        return Collections.emptyList();
    }

    @Override
    public String getMonitorType() {
        return "sample_remote_monitor";
    }

    @Override
    public RemoteMonitorRunner getMonitorRunner() {
        return SampleRemoteMonitorRunner.getMonitorRunner();
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
}