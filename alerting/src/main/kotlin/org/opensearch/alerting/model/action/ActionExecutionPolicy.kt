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
// TODO: Should throttleEnabled be included in here as well?
data class ActionExecutionPolicy(
    val throttle: Throttle? = null,
    val actionExecutionScope: ActionExecutionScope
) : Writeable, ToXContentObject {

    init {
        if (actionExecutionScope is PerExecutionActionScope) {
            require(throttle == null) { "Throttle is currently not supported for per execution action scope" }
        }
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this (
        sin.readOptionalWriteable(::Throttle), // throttle
        ActionExecutionScope.readFrom(sin) // actionExecutionScope
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        val xContentBuilder = builder.startObject()
        if (throttle != null) {
            xContentBuilder.field(THROTTLE_FIELD, throttle)
        }
        xContentBuilder.field(ACTION_EXECUTION_SCOPE, actionExecutionScope)
        return xContentBuilder.endObject()
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        if (throttle != null) {
            out.writeBoolean(true)
            throttle.writeTo(out)
        } else {
            out.writeBoolean(false)
        }
        if (actionExecutionScope is PerAlertActionScope) {
            out.writeEnum(ActionExecutionScope.Type.PER_ALERT)
        } else {
            out.writeEnum(ActionExecutionScope.Type.PER_EXECUTION)
        }
        actionExecutionScope.writeTo(out)
    }

    companion object {
        const val THROTTLE_FIELD = "throttle"
        const val ACTION_EXECUTION_SCOPE = "action_execution_scope"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ActionExecutionPolicy {
            var throttle: Throttle? = null
            lateinit var actionExecutionScope: ActionExecutionScope

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    THROTTLE_FIELD -> {
                        throttle = if (xcp.currentToken() == Token.VALUE_NULL) null else Throttle.parse(xcp)
                    }
                    ACTION_EXECUTION_SCOPE -> actionExecutionScope = ActionExecutionScope.parse(xcp)
                }
            }

            return ActionExecutionPolicy(
                throttle,
                requireNotNull(actionExecutionScope) { "Action execution scope is null" }
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): ActionExecutionPolicy {
            return ActionExecutionPolicy(sin)
        }

        /**
         * The default [ActionExecutionPolicy] configuration.
         *
         * This is currently only used by Bucket-Level Monitors and was configured with that in mind.
         * If Query-Level Monitors integrate the use of [ActionExecutionPolicy] then a separate default configuration
         * might need to be made depending on the desired behavior.
         */
        fun getDefaultConfiguration(): ActionExecutionPolicy {
            val defaultActionExecutionScope = PerAlertActionScope(
                actionableAlerts = setOf(AlertCategory.DEDUPED, AlertCategory.NEW)
            )
            return ActionExecutionPolicy(throttle = null, actionExecutionScope = defaultActionExecutionScope)
        }
    }
}
