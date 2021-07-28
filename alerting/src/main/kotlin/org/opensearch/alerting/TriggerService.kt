/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.Trigger
import org.opensearch.alerting.model.TriggerRunResult
import org.opensearch.alerting.script.TriggerExecutionContext
import org.opensearch.alerting.script.TriggerScript
import org.opensearch.client.Client
import org.opensearch.script.ScriptService

/** Service that handles executing Triggers */
class TriggerService(val client: Client, val scriptService: ScriptService) {

    private val logger = LogManager.getLogger(TriggerService::class.java)

    fun isTriggerActionable(ctx: TriggerExecutionContext, result: TriggerRunResult): Boolean {
        // Suppress actions if the current alert is acknowledged and there are no errors.
        val suppress = ctx.alert?.state == Alert.State.ACKNOWLEDGED && result.error == null && ctx.error == null
        return result.triggered && !suppress
    }

    fun runTrigger(monitor: Monitor, trigger: Trigger, ctx: TriggerExecutionContext): TriggerRunResult {
        return try {
            val triggered = scriptService.compile(trigger.condition, TriggerScript.CONTEXT)
                .newInstance(trigger.condition.params)
                .execute(ctx)
            TriggerRunResult(trigger.name, triggered, null)
        } catch (e: Exception) {
            logger.info("Error running script for monitor ${monitor.id}, trigger: ${trigger.id}", e)
            // if the script fails we need to send an alert so set triggered = true
            TriggerRunResult(trigger.name, true, e)
        }
    }
}
