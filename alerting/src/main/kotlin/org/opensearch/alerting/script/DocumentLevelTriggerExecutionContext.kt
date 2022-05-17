/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.script

import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.DocumentLevelTrigger
import org.opensearch.alerting.model.Monitor
import java.time.Instant

data class DocumentLevelTriggerExecutionContext(
    override val monitor: Monitor,
    val trigger: DocumentLevelTrigger,
    override val results: List<Map<String, Any>>,
    override val periodStart: Instant,
    override val periodEnd: Instant,
    val alerts: List<Alert> = listOf(),
    val triggeredDocs: List<String>,
    val relatedFindings: List<String>,
    override val error: Exception? = null
) : TriggerExecutionContext(monitor, results, periodStart, periodEnd, error) {

    constructor(
        monitor: Monitor,
        trigger: DocumentLevelTrigger,
        alerts: List<Alert> = listOf()
    ) : this(
        monitor, trigger, emptyList(), Instant.now(), Instant.now(),
        alerts, emptyList(), emptyList(), null
    )

    /**
     * Mustache templates need special permissions to reflectively introspect field names. To avoid doing this we
     * translate the context to a Map of Strings to primitive types, which can be accessed without reflection.
     */
    override fun asTemplateArg(): Map<String, Any?> {
        val tempArg = super.asTemplateArg().toMutableMap()
        tempArg["trigger"] = trigger.asTemplateArg()
        tempArg["alerts"] = alerts.map { it.asTemplateArg() }
        return tempArg
    }
}
