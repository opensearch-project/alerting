/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.monitor.fanouts;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.alerting.monitor.runners.SampleRemoteDocLevelMonitorRunner;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.alerting.action.DocLevelMonitorFanOutRequest;
import org.opensearch.commons.alerting.action.DocLevelMonitorFanOutResponse;
import org.opensearch.commons.alerting.model.InputRunResults;
import org.opensearch.commons.alerting.model.Monitor;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;

public class TransportRemoteDocLevelMonitorFanOutAction extends HandledTransportAction<DocLevelMonitorFanOutRequest, DocLevelMonitorFanOutResponse> {

    private final ClusterService clusterService;

    private final Settings settings;

    private final Client client;

    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public TransportRemoteDocLevelMonitorFanOutAction(
            TransportService transportService,
            Client client,
            NamedXContentRegistry xContentRegistry,
            ClusterService clusterService,
            Settings settings,
            ActionFilters actionFilters
    ) {
        super(SampleRemoteDocLevelMonitorRunner.REMOTE_DOC_LEVEL_MONITOR_ACTION_NAME, transportService, actionFilters, DocLevelMonitorFanOutRequest::new);
        this.clusterService = clusterService;
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.settings = settings;
    }

    @Override
    protected void doExecute(Task task, DocLevelMonitorFanOutRequest request, ActionListener<DocLevelMonitorFanOutResponse> actionListener) {
        Monitor monitor = request.getMonitor();
        Map<String, Object> lastRunContext = request.getMonitorMetadata().getLastRunContext();
        ((Map<String, Object>) lastRunContext.get("index")).put("0", 0);
        IndexRequest indexRequest = new IndexRequest(SampleRemoteDocLevelMonitorRunner.SAMPLE_REMOTE_DOC_LEVEL_MONITOR_RUNNER_INDEX)
                .source(Map.of("sample", "record")).setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        this.client.index(indexRequest, new ActionListener<>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                DocLevelMonitorFanOutResponse response = new DocLevelMonitorFanOutResponse(
                        clusterService.localNode().getId(),
                        request.getExecutionId(),
                        monitor.getId(),
                        lastRunContext,
                        new InputRunResults(),
                        new HashMap<>(),
                        null
                );
                actionListener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                actionListener.onFailure(e);
            }
        });
    }
}