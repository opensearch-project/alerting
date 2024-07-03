/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.spi

interface RemoteMonitorRunnerExtension {

    fun getMonitorTypesToMonitorRunners(): Map<String, RemoteMonitorRunner>
}