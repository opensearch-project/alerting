/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.spi

interface RemoteMonitorRunnerExtension {

    fun getMonitorType(): String

    fun getMonitorRunner(): RemoteMonitorRunner
}