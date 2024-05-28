/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.remote.monitors

import org.opensearch.alerting.spi.RemoteMonitorRunner

/**
 * Class to store monitorType to monitorRunner tuples.
 */
class RemoteMonitorRegistry(val monitorType: String, val monitorRunner: RemoteMonitorRunner)
