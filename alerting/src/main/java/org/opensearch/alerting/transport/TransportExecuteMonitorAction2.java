package org.opensearch.alerting.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.alerting.action.ExecuteMonitorAction2;
import org.opensearch.alerting.model.Model2ModelTranslator;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.commons.model2.action.ExecuteMonitorRequest;
import org.opensearch.commons.model2.action.ExecuteMonitorResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportExecuteMonitorAction2 extends TransportAction<ExecuteMonitorRequest, ExecuteMonitorResponse> {

    private final Logger LOG = LogManager.getLogger(TransportExecuteMonitorAction2.class);


    private final Client client;
    private final ClusterService cluster;


    @Inject
    public TransportExecuteMonitorAction2(final TransportService transport,
                                          final Client client,
                                          final ActionFilters actionFilters,
                                          final NamedXContentRegistry xContentRegistry,
                                          final ClusterService clusterService,
                                          final Settings settings) {
        super(ExecuteMonitorAction2.NAME, actionFilters, transport.getTaskManager());

        this.client = client;
        this.cluster = clusterService;
    }

    // TODO: if you need to deserialize via XContent, note there is ModelSerializer.read(XContent,....)
    @Override
    protected void doExecute(final Task task, ExecuteMonitorRequest request, final ActionListener<ExecuteMonitorResponse> actionListener) {
        try {
            final org.opensearch.alerting.action.ExecuteMonitorRequest xRequest = new org.opensearch.alerting.action.ExecuteMonitorRequest(
                    false,
                    TimeValue.ZERO,
                    null,
                    Model2ModelTranslator.fromModel2(request.monitor));

            this.client.execute(org.opensearch.alerting.action.ExecuteMonitorAction.Companion.getINSTANCE(), xRequest, new ActionListener<>() {
                @Override
                public void onResponse(final org.opensearch.alerting.action.ExecuteMonitorResponse response) {
                    final ExecuteMonitorResponse xResponse = new ExecuteMonitorResponse(response.getMonitorRunResult().getMonitorName());
                    actionListener.onResponse(xResponse);
                }

                @Override
                public void onFailure(final Exception e) {
                    actionListener.onFailure(e);
                }
            });
        } catch (final Exception e) {
            actionListener.onFailure(e);
        }
    }
}