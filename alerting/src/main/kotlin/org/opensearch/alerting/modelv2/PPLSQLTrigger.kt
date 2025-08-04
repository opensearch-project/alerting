/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.modelv2

import org.opensearch.alerting.modelv2.MonitorV2.Companion.ALERTING_V2_MAX_NAME_LENGTH
import org.opensearch.alerting.modelv2.MonitorV2.Companion.UUID_LENGTH
import org.opensearch.alerting.modelv2.TriggerV2.Companion.ACTIONS_FIELD
import org.opensearch.alerting.modelv2.TriggerV2.Companion.DEFAULT_EXPIRE_DURATION
import org.opensearch.alerting.modelv2.TriggerV2.Companion.EXPIRE_FIELD
import org.opensearch.alerting.modelv2.TriggerV2.Companion.ID_FIELD
import org.opensearch.alerting.modelv2.TriggerV2.Companion.LAST_TRIGGERED_FIELD
import org.opensearch.alerting.modelv2.TriggerV2.Companion.MONITOR_V2_MIN_EXPIRE_DURATION_MINUTES
import org.opensearch.alerting.modelv2.TriggerV2.Companion.MONITOR_V2_MIN_THROTTLE_DURATION_MINUTES
import org.opensearch.alerting.modelv2.TriggerV2.Companion.NAME_FIELD
import org.opensearch.alerting.modelv2.TriggerV2.Companion.NOTIFICATIONS_ID_MAX_LENGTH
import org.opensearch.alerting.modelv2.TriggerV2.Companion.SEVERITY_FIELD
import org.opensearch.alerting.modelv2.TriggerV2.Companion.THROTTLE_FIELD
import org.opensearch.alerting.modelv2.TriggerV2.Severity
import org.opensearch.common.CheckedFunction
import org.opensearch.common.UUIDs
import org.opensearch.commons.alerting.model.action.Action
import org.opensearch.commons.alerting.util.instant
import org.opensearch.commons.alerting.util.optionalTimeField
import org.opensearch.core.ParseField
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import java.io.IOException
import java.time.Instant

/**
 * The PPL/SQL Trigger for PPL/SQL Monitors
 *
 * There are two types of PPLTrigger conditions: NUMBER_OF_RESULTS and CUSTOM
 * NUMBER_OF_RESULTS: triggers based on whether the number of query results returned by the PPLSQLMonitor
 *                    query meets some threshold
 * CUSTOM: triggers based on a custom condition that user specifies (a single ppl eval statement)
 *
 * PPLTriggers can run on two modes: RESULT_SET and PER_RESULT
 * RESULT_SET: exactly one Alert is generated when the Trigger condition is met
 * PER_RESULT: one Alert is generated per trigger condition-meeting query result row
 *
 * @property id Trigger ID, defaults to a base64 UUID.
 * @property name Display name of the Trigger.
 * @property severity The severity level of the Trigger.
 * @property throttleDuration Optional duration (in minutes) for which alerts from this Trigger should be throttled/suppressed.
 *                            Null indicates no throttling.
 * @property expireDuration Duration (in minutes) after which alerts from this Trigger should be deleted permanently.
 * @property lastTriggeredTime The last time this Trigger generated an Alert. Null if Trigger hasn't generated an Alert yet.
 * @property actions List of notification-sending actions to run when the Trigger condition is met.
 * @property mode Specifies whether the trigger evaluates the entire result set or each result individually.
 *               Can be either [TriggerMode.RESULT_SET] or [TriggerMode.PER_RESULT].
 * @property conditionType The type of condition to evaluate.
 *               Can be either [ConditionType.NUMBER_OF_RESULTS] or [ConditionType.CUSTOM].
 * @property numResultsCondition The comparison operator for NUMBER_OF_RESULTS conditions. Required if using NUMBER_OF_RESULTS conditions,
 *                               required to be null otherwise.
 * @property numResultsValue The threshold value for NUMBER_OF_RESULTS conditions. Required if using NUMBER_OF_RESULTS conditions,
 *                           required to be null otherwise.
 * @property customCondition A custom condition expression. Required if using CUSTOM conditions,
 *                           required to be null otherwise.
 */
data class PPLSQLTrigger(
    override val id: String = UUIDs.base64UUID(),
    override val name: String,
    override val severity: Severity,
    override val throttleDuration: Long?,
    override val expireDuration: Long = DEFAULT_EXPIRE_DURATION,
    override var lastTriggeredTime: Instant?,
    override val actions: List<Action>,
    val mode: TriggerMode, // RESULT_SET or PER_RESULT
    val conditionType: ConditionType, // NUMBER_OF_RESULTS or CUSTOM
    val numResultsCondition: NumResultsCondition?,
    val numResultsValue: Long?,
    val customCondition: String?
) : TriggerV2 {

    init {
        requireNotNull(this.name) { "Trigger name must be included." }
        requireNotNull(this.severity) { "Trigger severity must be included." }
        requireNotNull(this.mode) { "Trigger mode must be included." }
        requireNotNull(this.conditionType) { "Trigger condition type must be included." }

        require(this.id.length <= UUID_LENGTH) {
            "Trigger ID too long, length must be less than $UUID_LENGTH."
        }

        require(this.name.length <= ALERTING_V2_MAX_NAME_LENGTH) {
            "Trigger name too long, length must be less than $ALERTING_V2_MAX_NAME_LENGTH."
        }

        require(this.expireDuration >= MONITOR_V2_MIN_EXPIRE_DURATION_MINUTES) {
            "expire duration cannot be less than $MONITOR_V2_MIN_EXPIRE_DURATION_MINUTES, was $expireDuration."
        }

        this.throttleDuration?.let {
            require(it >= MONITOR_V2_MIN_THROTTLE_DURATION_MINUTES) {
                "Throttle duration cannot be less than $MONITOR_V2_MIN_THROTTLE_DURATION_MINUTES, was $throttleDuration."
            }
        }

        this.actions.forEach {
            require(it.name.length <= ALERTING_V2_MAX_NAME_LENGTH) {
                "Name of action with ID ${it.id} too long, length must be less than $ALERTING_V2_MAX_NAME_LENGTH."
            }
            require(it.destinationId.length <= NOTIFICATIONS_ID_MAX_LENGTH) {
                "Channel ID of action with ID ${it.id} too long, length must be less than $NOTIFICATIONS_ID_MAX_LENGTH."
            }
            require(it.destinationId.isNotEmpty()) {
                "Channel ID should not be empty."
            }
            require(it.destinationId.matches(Regex("^[a-zA-Z0-9_-]+$"))) {
                "Channel ID should only have alphanumeric characters, dashes, and underscores."
            }
        }

        when (this.conditionType) {
            ConditionType.NUMBER_OF_RESULTS -> {
                requireNotNull(this.numResultsCondition) {
                    "if trigger condition is of type ${ConditionType.NUMBER_OF_RESULTS.value}, " +
                        "$NUM_RESULTS_CONDITION_FIELD must be included."
                }
                requireNotNull(this.numResultsValue) {
                    "if trigger condition is of type ${ConditionType.NUMBER_OF_RESULTS.value}, " +
                        "$NUM_RESULTS_VALUE_FIELD must be included."
                }
                require(this.customCondition == null) {
                    "if trigger condition is of type ${ConditionType.NUMBER_OF_RESULTS.value}, " +
                        "$CUSTOM_CONDITION_FIELD must not be included."
                }
            }
            ConditionType.CUSTOM -> {
                requireNotNull(this.customCondition) {
                    "if trigger condition is of type ${ConditionType.CUSTOM.value}, " +
                        "$CUSTOM_CONDITION_FIELD must be included."
                }
                require(this.numResultsCondition == null) {
                    "if trigger condition is of type ${ConditionType.CUSTOM.value}, " +
                        "$NUM_RESULTS_CONDITION_FIELD must not be included."
                }
                require(this.numResultsValue == null) {
                    "if trigger condition is of type ${ConditionType.CUSTOM.value}, " +
                        "$NUM_RESULTS_VALUE_FIELD must not be included."
                }
            }
        }

        if (conditionType == ConditionType.NUMBER_OF_RESULTS) {
            require(this.numResultsValue!! >= 0L) { "Number of results to check for cannot be negative." }
        }
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(), // id
        sin.readString(), // name
        sin.readEnum(Severity::class.java), // severity
        sin.readOptionalLong(), // throttleDuration
        sin.readLong(), // expireDuration
        sin.readOptionalInstant(), // lastTriggeredTime
        sin.readList(::Action), // actions
        sin.readEnum(TriggerMode::class.java), // trigger mode
        sin.readEnum(ConditionType::class.java), // condition type
        if (sin.readBoolean()) sin.readEnum(NumResultsCondition::class.java) else null, // num results condition
        sin.readOptionalLong(), // num results value
        sin.readOptionalString() // custom condition
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeString(name)
        out.writeEnum(severity)
        out.writeOptionalLong(throttleDuration)
        out.writeLong(expireDuration)
        out.writeOptionalInstant(lastTriggeredTime)
        out.writeCollection(actions)
        out.writeEnum(mode)
        out.writeEnum(conditionType)

        out.writeBoolean(numResultsCondition != null)
        numResultsCondition?.let { out.writeEnum(numResultsCondition) }

        out.writeOptionalLong(numResultsValue)
        out.writeOptionalString(customCondition)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        builder.startObject()
        builder.field(ID_FIELD, id)
        builder.field(NAME_FIELD, name)
        builder.field(SEVERITY_FIELD, severity.value)
        throttleDuration?.let { builder.field(THROTTLE_FIELD, throttleDuration) }
        builder.field(EXPIRE_FIELD, expireDuration)
        builder.optionalTimeField(LAST_TRIGGERED_FIELD, lastTriggeredTime)
        builder.field(ACTIONS_FIELD, actions.toTypedArray())
        builder.field(MODE_FIELD, mode.value)
        builder.field(CONDITION_TYPE_FIELD, conditionType.value)
        numResultsCondition?.let { builder.field(NUM_RESULTS_CONDITION_FIELD, numResultsCondition.value) }
        numResultsValue?.let { builder.field(NUM_RESULTS_VALUE_FIELD, numResultsValue) }
        customCondition?.let { builder.field(CUSTOM_CONDITION_FIELD, customCondition) }
        builder.endObject()
        return builder
    }

    fun asTemplateArg(): Map<String, Any?> {
        return mapOf(
            ID_FIELD to id,
            NAME_FIELD to name,
            SEVERITY_FIELD to severity.value,
            THROTTLE_FIELD to throttleDuration,
            EXPIRE_FIELD to expireDuration,
            ACTIONS_FIELD to actions.map { it.asTemplateArg() },
            MODE_FIELD to mode.value,
            CONDITION_TYPE_FIELD to conditionType.value,
            NUM_RESULTS_CONDITION_FIELD to numResultsCondition?.value,
            NUM_RESULTS_VALUE_FIELD to numResultsValue,
            CUSTOM_CONDITION_FIELD to customCondition
        )
    }

    enum class TriggerMode(val value: String) {
        RESULT_SET("result_set"),
        PER_RESULT("per_result");

        companion object {
            fun enumFromString(value: String): TriggerMode? = entries.firstOrNull { it.value == value }
        }
    }

    enum class ConditionType(val value: String) {
        NUMBER_OF_RESULTS("number_of_results"),
        CUSTOM("custom");

        companion object {
            fun enumFromString(value: String): ConditionType? = entries.firstOrNull { it.value == value }
        }
    }

    enum class NumResultsCondition(val value: String) {
        GREATER_THAN(">"),
        GREATER_THAN_EQUAL(">="),
        LESS_THAN("<"),
        LESS_THAN_EQUAL("<="),
        EQUAL("=="),
        NOT_EQUAL("!=");

        companion object {
            fun enumFromString(value: String): NumResultsCondition? = entries.firstOrNull { it.value == value }
        }
    }

    companion object {
        // trigger wrapper object field name
        const val PPL_SQL_TRIGGER_FIELD = "ppl_sql_trigger"

        // field names
        const val MODE_FIELD = "mode"
        const val CONDITION_TYPE_FIELD = "type"
        const val NUM_RESULTS_CONDITION_FIELD = "num_results_condition"
        const val NUM_RESULTS_VALUE_FIELD = "num_results_value"
        const val CUSTOM_CONDITION_FIELD = "custom_condition"

        val XCONTENT_REGISTRY = NamedXContentRegistry.Entry(
            TriggerV2::class.java,
            ParseField(PPL_SQL_TRIGGER_FIELD),
            CheckedFunction { parseInner(it) }
        )

        @JvmStatic
        @Throws(IOException::class)
        fun parseInner(xcp: XContentParser): PPLSQLTrigger {
            var id = UUIDs.base64UUID() // assign a default triggerId if one is not specified
            var name: String? = null
            var severity: Severity? = null
            var throttleDuration: Long? = null
            var expireDuration: Long = DEFAULT_EXPIRE_DURATION
            var lastTriggeredTime: Instant? = null
            val actions: MutableList<Action> = mutableListOf()
            var mode: TriggerMode? = null
            var conditionType: ConditionType? = null
            var numResultsCondition: NumResultsCondition? = null
            var numResultsValue: Long? = null
            var customCondition: String? = null

            /* parse */
            XContentParserUtils.ensureExpectedToken( // outer trigger object start
                XContentParser.Token.START_OBJECT,
                xcp.currentToken(), xcp
            )

            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ID_FIELD -> id = xcp.text()
                    NAME_FIELD -> name = xcp.text()
                    SEVERITY_FIELD -> {
                        val input = xcp.text()
                        val enumMatchResult = Severity.enumFromString(input)
                            ?: throw IllegalArgumentException(
                                "Invalid value for $SEVERITY_FIELD: $input. " +
                                    "Supported values are ${Severity.entries.map { it.value }}"
                            )
                        severity = enumMatchResult
                    }
                    MODE_FIELD -> {
                        val input = xcp.text()
                        val enumMatchResult = TriggerMode.enumFromString(input)
                            ?: throw IllegalArgumentException(
                                "Invalid value for $MODE_FIELD: $input. " +
                                    "Supported values are ${TriggerMode.entries.map { it.value }}"
                            )
                        mode = enumMatchResult
                    }
                    CONDITION_TYPE_FIELD -> {
                        val input = xcp.text()
                        val enumMatchResult = ConditionType.enumFromString(input)
                            ?: throw IllegalArgumentException(
                                "Invalid value for $CONDITION_TYPE_FIELD: $input. " +
                                    "Supported values are ${ConditionType.entries.map { it.value }}"
                            )
                        conditionType = enumMatchResult
                    }
                    NUM_RESULTS_CONDITION_FIELD -> {
                        if (xcp.currentToken() != XContentParser.Token.VALUE_NULL) {
                            val input = xcp.text()
                            val enumMatchResult = NumResultsCondition.enumFromString(input)
                                ?: throw IllegalArgumentException(
                                    "Invalid value for $NUM_RESULTS_CONDITION_FIELD: $input. " +
                                        "Supported values are ${NumResultsCondition.entries.map { it.value }}"
                                )
                            numResultsCondition = enumMatchResult
                        }
                    }
                    NUM_RESULTS_VALUE_FIELD -> {
                        if (xcp.currentToken() != XContentParser.Token.VALUE_NULL) {
                            numResultsValue = xcp.longValue()
                        }
                    }
                    CUSTOM_CONDITION_FIELD -> {
                        if (xcp.currentToken() != XContentParser.Token.VALUE_NULL) {
                            customCondition = xcp.text()
                        }
                    }
                    THROTTLE_FIELD -> {
                        if (xcp.currentToken() != XContentParser.Token.VALUE_NULL) {
                            throttleDuration = xcp.longValue()
                        }
                    }
                    EXPIRE_FIELD -> {
                        if (xcp.currentToken() != XContentParser.Token.VALUE_NULL) {
                            expireDuration = xcp.longValue()
                        }
                    }
                    LAST_TRIGGERED_FIELD -> lastTriggeredTime = xcp.instant()
                    ACTIONS_FIELD -> {
                        XContentParserUtils.ensureExpectedToken(
                            XContentParser.Token.START_ARRAY,
                            xcp.currentToken(),
                            xcp
                        )
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            actions.add(Action.parse(xcp))
                        }
                    }
                    else -> throw IllegalArgumentException("Unexpected field when parsing PPL Trigger: $fieldName")
                }
            }

            /* validations */
            requireNotNull(name) { "Trigger name must be included" }
            requireNotNull(severity) { "Trigger severity must be included" }
            requireNotNull(mode) { "Trigger mode must be included" }
            requireNotNull(conditionType) { "Trigger condition type must be included" }

            // 3. prepare and return PPLTrigger object
            return PPLSQLTrigger(
                id,
                name,
                severity,
                throttleDuration,
                expireDuration,
                lastTriggeredTime,
                actions,
                mode,
                conditionType,
                numResultsCondition,
                numResultsValue,
                customCondition
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): PPLSQLTrigger {
            return PPLSQLTrigger(sin)
        }
    }
}
