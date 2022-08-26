package org.opensearch.alerting.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.alerting.action.IndexMonitorAction2;
import org.opensearch.alerting.model.Model2ModelTranslator;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.commons.model2.action.IndexMonitorRequest;
import org.opensearch.commons.model2.action.IndexMonitorResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportIndexMonitorAction2 extends TransportAction<IndexMonitorRequest, IndexMonitorResponse> {

    private final Logger LOG = LogManager.getLogger(TransportIndexMonitorAction.class);

    private final Client client;
    private final ClusterService cluster;


    @Inject
    public TransportIndexMonitorAction2(final TransportService transport,
                                        final Client client,
                                        final ActionFilters actionFilters,
                                        final NamedXContentRegistry xContentRegistry,
                                        final ClusterService clusterService,
                                        final Settings settings) {
        super(IndexMonitorAction2.NAME, actionFilters, transport.getTaskManager());

        this.client = client;
        this.cluster = clusterService;
    }

    @Override
    protected void doExecute(final Task task, IndexMonitorRequest request, final ActionListener<IndexMonitorResponse> actionListener) {
        try {
            final org.opensearch.alerting.action.IndexMonitorRequest xRequest = new org.opensearch.alerting.action.IndexMonitorRequest(
                    request.monitorId,
                    request.seqNo,
                    request.primaryTerm,
                    request.refreshPolicy,
                    request.method,
                    Model2ModelTranslator.fromModel2(request.monitor));

            this.client.execute(org.opensearch.alerting.action.IndexMonitorAction.Companion.getINSTANCE(), xRequest,
                    ActionListener.map(actionListener,
                            response -> new IndexMonitorResponse(
                                    response.getId(),
                                    response.getVersion(),
                                    response.getSeqNo(),
                                    response.getVersion(),
                                    response.getStatus(),
                                    Model2ModelTranslator.toModel2(response.getMonitor()))));
        } catch (final Exception e) {
            actionListener.onFailure(e);
        }
    }
}