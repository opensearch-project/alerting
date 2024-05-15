/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.alerting.spi.RemoteMonitorRunResult;
import org.opensearch.alerting.spi.RemoteMonitorRunner;
import org.opensearch.commons.alerting.model.Monitor;
import org.opensearch.transport.TransportService;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class SampleRemoteMonitorRunner implements RemoteMonitorRunner {

    private static final Logger log = LogManager.getLogger(SampleRemoteMonitorRunner.class);

    private static SampleRemoteMonitorRunner INSTANCE;

    public static SampleRemoteMonitorRunner getMonitorRunner() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (SampleRemoteMonitorRunner.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new SampleRemoteMonitorRunner();
            return INSTANCE;
        }
    }

    @Override
    public RemoteMonitorRunResult runMonitor(
            Monitor monitor,
            Instant periodStart,
            Instant periodEnd,
            boolean dryrun,
            TransportService transportService
    ) {
        log.info("hit here");
        return new RemoteMonitorRunResult(
                List.of(),
                null,
                Map.of()
        );
    }
}