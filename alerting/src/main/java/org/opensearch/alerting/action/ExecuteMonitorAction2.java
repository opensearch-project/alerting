package org.opensearch.alerting.action;

import org.opensearch.action.ActionType;

public class ExecuteMonitorAction2 extends ActionType<org.opensearch.commons.model2.action.ExecuteMonitorResponse> {

    public static final String NAME = "cluster:admin/opendistro/alerting/monitor/execute2";
    public static final ExecuteMonitorAction2 INSTANCE = new ExecuteMonitorAction2(NAME);

    private ExecuteMonitorAction2(final String name) {
        super(name, org.opensearch.commons.model2.action.ExecuteMonitorResponse::new);
    }
}
