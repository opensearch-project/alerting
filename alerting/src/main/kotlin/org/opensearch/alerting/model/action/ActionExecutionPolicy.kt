/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.action

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

/**
 * This class represents the container for various configurations which control Action behavior.
 */
data class ActionExecutionPolicy(
    val actionExecutionScope: ActionExecutionScope
) : Writeable, ToXContentObject {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this (
        ActionExecutionScope.readFrom(sin) // actionExecutionScope
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(ACTION_EXECUTION_SCOPE, actionExecutionScope)
        return builder.endObject()
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        if (actionExecutionScope is PerAlertActionScope) {
            out.writeEnum(ActionExecutionScope.Type.PER_ALERT)
        } else {
            out.writeEnum(ActionExecutionScope.Type.PER_EXECUTION)
        }
        actionExecutionScope.writeTo(out)
    }

    companion object {
        const val ACTION_EXECUTION_SCOPE = "action_execution_scope"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ActionExecutionPolicy {
            lateinit var actionExecutionScope: ActionExecutionScope

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ACTION_EXECUTION_SCOPE -> actionExecutionScope = ActionExecutionScope.parse(xcp)
                }
            }

            return ActionExecutionPolicy(
                requireNotNull(actionExecutionScope) { "Action execution scope is null" }
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): ActionExecutionPolicy {
            return ActionExecutionPolicy(sin)
        }

        /**
         * The default [ActionExecutionPolicy] configuration for Bucket-Level Monitors.
         *
         * If Query-Level Monitors integrate the use of [ActionExecutionPolicy] then a separate default configuration
         * will need to be made depending on the desired behavior.
         */
        fun getDefaultConfigurationForBucketLevelMonitor(): ActionExecutionPolicy {
            val defaultActionExecutionScope = PerAlertActionScope(
                actionableAlerts = setOf(AlertCategory.DEDUPED, AlertCategory.NEW)
            )
            return ActionExecutionPolicy(actionExecutionScope = defaultActionExecutionScope)
        }

        /**
         * The default [ActionExecutionPolicy] configuration for Document-Level Monitors.
         *
         * If Query-Level Monitors integrate the use of [ActionExecutionPolicy] then a separate default configuration
         * will need to be made depending on the desired behavior.
         */
        fun getDefaultConfigurationForDocumentLevelMonitor(): ActionExecutionPolicy {
            val defaultActionExecutionScope = PerAlertActionScope(
                actionableAlerts = setOf(AlertCategory.DEDUPED, AlertCategory.NEW)
            )
            return ActionExecutionPolicy(actionExecutionScope = defaultActionExecutionScope)
        }
    }
}
