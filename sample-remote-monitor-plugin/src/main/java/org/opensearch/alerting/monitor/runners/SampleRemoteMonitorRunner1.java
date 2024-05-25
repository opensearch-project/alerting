/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.monitor.runners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.alerting.monitor.triggers.SampleRemoteMonitorTriggerRunResult;
import org.opensearch.alerting.spi.RemoteMonitorRunner;
import org.opensearch.client.Client;
import org.opensearch.commons.alerting.model.InputRunResults;
import org.opensearch.commons.alerting.model.Monitor;
import org.opensearch.commons.alerting.model.MonitorRunResult;
import org.opensearch.commons.alerting.model.TriggerRunResult;
import org.opensearch.transport.TransportService;

import java.time.Instant;
import java.util.Map;

public class SampleRemoteMonitorRunner1 extends RemoteMonitorRunner {

    public static final String SAMPLE_MONITOR_RUNNER1_INDEX = ".opensearch-alerting-sample-remote-monitor1";

    private static final Logger log = LogManager.getLogger(SampleRemoteMonitorRunner1.class);

    private static SampleRemoteMonitorRunner1 INSTANCE;

    private Client client;

    public static SampleRemoteMonitorRunner1 getMonitorRunner() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (SampleRemoteMonitorRunner1.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new SampleRemoteMonitorRunner1();
            return INSTANCE;
        }
    }

    public void setClient(Client client) {
        this.client = client;
    }

    @Override
    public MonitorRunResult<TriggerRunResult> runMonitor(
            Monitor monitor,
            Instant periodStart,
            Instant periodEnd,
            boolean dryrun,
            String executionId,
            TransportService transportService
    ) {
        IndexRequest indexRequest = new IndexRequest(SAMPLE_MONITOR_RUNNER1_INDEX)
                .source(Map.of("sample", "record")).setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        this.client.index(indexRequest);

        return new MonitorRunResult<>(
                monitor.getName(),
                periodStart,
                periodEnd,
                null,
                new InputRunResults(),
                Map.of("test-trigger", new SampleRemoteMonitorTriggerRunResult("test-trigger", null, Map.of()))
        );
    }
}