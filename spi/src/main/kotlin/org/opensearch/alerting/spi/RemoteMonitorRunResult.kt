/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.spi

import java.time.Instant

class RemoteActionRunResult(
    val actionId: String,
    val actionName: String,
    val output: Map<String, String>,
    val throttled: Boolean = false,
    val executionTime: Instant? = null,
    val error: Exception? = null
)

class RemoteMonitorTriggerRunResult(
    val triggerName: String,
    val error: Exception? = null,
    val actionResultsMap: MutableMap<String, MutableMap<String, RemoteActionRunResult>>
)

class RemoteMonitorRunResult(
    val results: List<Map<String, Any>>,
    val error: Exception?,
    val triggerResults: Map<String, RemoteMonitorTriggerRunResult> = mapOf()
)