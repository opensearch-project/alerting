/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.spi

import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.transport.TransportService
import java.time.Instant

interface RemoteMonitorRunner {

    fun runMonitor(
        monitor: Monitor,
        periodStart: Instant,
        periodEnd: Instant,
        dryrun: Boolean,
        transportService: TransportService
    ): RemoteMonitorRunResult
}