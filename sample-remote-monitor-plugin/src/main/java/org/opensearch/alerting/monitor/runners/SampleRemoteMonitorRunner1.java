/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.monitor.runners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.alerting.monitor.inputs.SampleRemoteMonitorInput1;
import org.opensearch.alerting.monitor.trigger.results.SampleRemoteMonitorTriggerRunResult;
import org.opensearch.alerting.monitor.triggers.SampleRemoteMonitorTrigger1;
import org.opensearch.alerting.spi.RemoteMonitorRunner;
import org.opensearch.client.Client;
import org.opensearch.commons.alerting.model.Input;
import org.opensearch.commons.alerting.model.InputRunResults;
import org.opensearch.commons.alerting.model.Monitor;
import org.opensearch.commons.alerting.model.MonitorRunResult;
import org.opensearch.commons.alerting.model.Trigger;
import org.opensearch.commons.alerting.model.TriggerRunResult;
import org.opensearch.commons.alerting.model.remote.monitors.RemoteMonitorInput;
import org.opensearch.commons.alerting.model.remote.monitors.RemoteMonitorTrigger;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
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
        try {
            BytesReference customInputSerialized = null;
            Input input = monitor.getInputs().get(0);
            if (input instanceof RemoteMonitorInput) {
                customInputSerialized = ((RemoteMonitorInput) input).getInput();
            }
            StreamInput sin = StreamInput.wrap(customInputSerialized.toBytesRef().bytes);
            SampleRemoteMonitorInput1 remoteMonitorInput = new SampleRemoteMonitorInput1(sin);

            BytesReference customTriggerSerialized = null;
            Trigger trigger = monitor.getTriggers().get(0);
            if (trigger instanceof RemoteMonitorTrigger) {
                customTriggerSerialized = ((RemoteMonitorTrigger) trigger).getTrigger();
            }
            StreamInput triggerSin = StreamInput.wrap(customTriggerSerialized.toBytesRef().bytes);
            SampleRemoteMonitorTrigger1 remoteMonitorTrigger = new SampleRemoteMonitorTrigger1(triggerSin);

            IndexRequest indexRequest = new IndexRequest(SAMPLE_MONITOR_RUNNER1_INDEX)
                    .source(Map.of(remoteMonitorInput.getA(), remoteMonitorTrigger.getC())).setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            this.client.index(indexRequest);

            return new MonitorRunResult<>(
                    monitor.getName(),
                    periodStart,
                    periodEnd,
                    null,
                    new InputRunResults(),
                    Map.of("test-trigger", new SampleRemoteMonitorTriggerRunResult("test-trigger", null, Map.of()))
            );
        } catch (Exception ex) {
            return new MonitorRunResult<>(
                    monitor.getName(),
                    periodStart,
                    periodEnd,
                    ex,
                    new InputRunResults(),
                    Map.of("test-trigger", new SampleRemoteMonitorTriggerRunResult("test-trigger", ex, Map.of()))
            );
        }

    }
}