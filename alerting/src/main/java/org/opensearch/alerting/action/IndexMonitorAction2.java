package org.opensearch.alerting.action;

import org.opensearch.action.ActionType;

public class IndexMonitorAction2 extends ActionType<org.opensearch.commons.model2.action.IndexMonitorResponse> {

    public static final String NAME = "cluster:admin/opendistro/alerting/monitor/write2";
    public static final IndexMonitorAction2 INSTANCE = new IndexMonitorAction2(NAME);

    private IndexMonitorAction2(final String name) {
        super(name, org.opensearch.commons.model2.action.IndexMonitorResponse::new);
    }
}
