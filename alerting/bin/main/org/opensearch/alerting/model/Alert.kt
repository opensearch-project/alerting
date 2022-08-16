/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.alerts.AlertError
import org.opensearch.alerting.opensearchapi.instant
import org.opensearch.alerting.opensearchapi.optionalTimeField
import org.opensearch.alerting.opensearchapi.optionalUserField
import org.opensearch.alerting.util.IndexUtils.Companion.NO_SCHEMA_VERSION
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.lucene.uid.Versions
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.commons.authuser.User
import java.io.IOException
import java.time.Instant

data class Alert(
    val id: String = NO_ID,
    val version: Long = NO_VERSION,
    val schemaVersion: Int = NO_SCHEMA_VERSION,
    val monitorId: String,
    val monitorName: String,
    val monitorVersion: Long,
    val monitorUser: User?,
    val triggerId: String,
    val triggerName: String,
    val findingIds: List<String>,
    val relatedDocIds: List<String>,
    val state: State,
    val startTime: Instant,
    val endTime: Instant? = null,
    val lastNotificationTime: Instant? = null,
    val acknowledgedTime: Instant? = null,
    val errorMessage: String? = null,
    val errorHistory: List<AlertError>,
    val severity: String,
    val actionExecutionResults: List<ActionExecutionResult>,
    val aggregationResultBucket: AggregationResultBucket? = null
) : Writeable, ToXContent {

    init {
        if (errorMessage != null) require(state == State.DELETED || state == State.ERROR) {
            "Attempt to create an alert with an error in state: $state"
        }
    }

    constructor(
        monitor: Monitor,
        trigger: QueryLevelTrigger,
        startTime: Instant,
        lastNotificationTime: Instant?,
        state: State = State.ACTIVE,
        errorMessage: String? = null,
        errorHistory: List<AlertError> = mutableListOf(),
        actionExecutionResults: List<ActionExecutionResult> = mutableListOf(),
        schemaVersion: Int = NO_SCHEMA_VERSION
    ) : this(
        monitorId = monitor.id, monitorName = monitor.name, monitorVersion = monitor.version, monitorUser = monitor.user,
        triggerId = trigger.id, triggerName = trigger.name, state = state, startTime = startTime,
        lastNotificationTime = lastNotificationTime, errorMessage = errorMessage, errorHistory = errorHistory,
        severity = trigger.severity, actionExecutionResults = actionExecutionResults, schemaVersion = schemaVersion,
        aggregationResultBucket = null, findingIds = emptyList(), relatedDocIds = emptyList()
    )

    constructor(
        monitor: Monitor,
        trigger: BucketLevelTrigger,
        startTime: Instant,
        lastNotificationTime: Instant?,
        state: State = State.ACTIVE,
        errorMessage: String? = null,
        errorHistory: List<AlertError> = mutableListOf(),
        actionExecutionResults: List<ActionExecutionResult> = mutableListOf(),
        schemaVersion: Int = NO_SCHEMA_VERSION
    ) : this(
        monitorId = monitor.id, monitorName = monitor.name, monitorVersion = monitor.version, monitorUser = monitor.user,
        triggerId = trigger.id, triggerName = trigger.name, state = state, startTime = startTime,
        lastNotificationTime = lastNotificationTime, errorMessage = errorMessage, errorHistory = errorHistory,
        severity = trigger.severity, actionExecutionResults = actionExecutionResults, schemaVersion = schemaVersion,
        aggregationResultBucket = null, findingIds = emptyList(), relatedDocIds = emptyList()
    )

    constructor(
        monitor: Monitor,
        trigger: BucketLevelTrigger,
        startTime: Instant,
        lastNotificationTime: Instant?,
        state: State = State.ACTIVE,
        errorMessage: String? = null,
        errorHistory: List<AlertError> = mutableListOf(),
        actionExecutionResults: List<ActionExecutionResult> = mutableListOf(),
        schemaVersion: Int = NO_SCHEMA_VERSION,
        aggregationResultBucket: AggregationResultBucket
    ) : this(
        monitorId = monitor.id, monitorName = monitor.name, monitorVersion = monitor.version, monitorUser = monitor.user,
        triggerId = trigger.id, triggerName = trigger.name, state = state, startTime = startTime,
        lastNotificationTime = lastNotificationTime, errorMessage = errorMessage, errorHistory = errorHistory,
        severity = trigger.severity, actionExecutionResults = actionExecutionResults, schemaVersion = schemaVersion,
        aggregationResultBucket = aggregationResultBucket, findingIds = emptyList(), relatedDocIds = emptyList()
    )

    constructor(
        id: String = NO_ID,
        monitor: Monitor,
        trigger: DocumentLevelTrigger,
        findingIds: List<String>,
        relatedDocIds: List<String>,
        startTime: Instant,
        lastNotificationTime: Instant?,
        state: State = State.ACTIVE,
        errorMessage: String? = null,
        errorHistory: List<AlertError> = mutableListOf(),
        actionExecutionResults: List<ActionExecutionResult> = mutableListOf(),
        schemaVersion: Int = NO_SCHEMA_VERSION
    ) : this(
        id = id, monitorId = monitor.id, monitorName = monitor.name, monitorVersion = monitor.version, monitorUser = monitor.user,
        triggerId = trigger.id, triggerName = trigger.name, state = state, startTime = startTime,
        lastNotificationTime = lastNotificationTime, errorMessage = errorMessage, errorHistory = errorHistory,
        severity = trigger.severity, actionExecutionResults = actionExecutionResults, schemaVersion = schemaVersion,
        aggregationResultBucket = null, findingIds = findingIds, relatedDocIds = relatedDocIds
    )

    enum class State {
        ACTIVE, ACKNOWLEDGED, COMPLETED, ERROR, DELETED
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        version = sin.readLong(),
        schemaVersion = sin.readInt(),
        monitorId = sin.readString(),
        monitorName = sin.readString(),
        monitorVersion = sin.readLong(),
        monitorUser = if (sin.readBoolean()) {
            User(sin)
        } else null,
        triggerId = sin.readString(),
        triggerName = sin.readString(),
        findingIds = sin.readStringList(),
        relatedDocIds = sin.readStringList(),
        state = sin.readEnum(State::class.java),
        startTime = sin.readInstant(),
        endTime = sin.readOptionalInstant(),
        lastNotificationTime = sin.readOptionalInstant(),
        acknowledgedTime = sin.readOptionalInstant(),
        errorMessage = sin.readOptionalString(),
        errorHistory = sin.readList(::AlertError),
        severity = sin.readString(),
        actionExecutionResults = sin.readList(::ActionExecutionResult),
        aggregationResultBucket = if (sin.readBoolean()) AggregationResultBucket(sin) else null
    )

    fun isAcknowledged(): Boolean = (state == State.ACKNOWLEDGED)

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
        out.writeInt(schemaVersion)
        out.writeString(monitorId)
        out.writeString(monitorName)
        out.writeLong(monitorVersion)
        out.writeBoolean(monitorUser != null)
        monitorUser?.writeTo(out)
        out.writeString(triggerId)
        out.writeString(triggerName)
        out.writeStringCollection(findingIds)
        out.writeStringCollection(relatedDocIds)
        out.writeEnum(state)
        out.writeInstant(startTime)
        out.writeOptionalInstant(endTime)
        out.writeOptionalInstant(lastNotificationTime)
        out.writeOptionalInstant(acknowledgedTime)
        out.writeOptionalString(errorMessage)
        out.writeCollection(errorHistory)
        out.writeString(severity)
        out.writeCollection(actionExecutionResults)
        if (aggregationResultBucket != null) {
            out.writeBoolean(true)
            aggregationResultBucket.writeTo(out)
        } else {
            out.writeBoolean(false)
        }
    }

    companion object {

        const val ALERT_ID_FIELD = "id"
        const val SCHEMA_VERSION_FIELD = "schema_version"
        const val ALERT_VERSION_FIELD = "version"
        const val MONITOR_ID_FIELD = "monitor_id"
        const val MONITOR_VERSION_FIELD = "monitor_version"
        const val MONITOR_NAME_FIELD = "monitor_name"
        const val MONITOR_USER_FIELD = "monitor_user"
        const val TRIGGER_ID_FIELD = "trigger_id"
        const val TRIGGER_NAME_FIELD = "trigger_name"
        const val FINDING_IDS = "finding_ids"
        const val RELATED_DOC_IDS = "related_doc_ids"
        const val STATE_FIELD = "state"
        const val START_TIME_FIELD = "start_time"
        const val LAST_NOTIFICATION_TIME_FIELD = "last_notification_time"
        const val END_TIME_FIELD = "end_time"
        const val ACKNOWLEDGED_TIME_FIELD = "acknowledged_time"
        const val ERROR_MESSAGE_FIELD = "error_message"
        const val ALERT_HISTORY_FIELD = "alert_history"
        const val SEVERITY_FIELD = "severity"
        const val ACTION_EXECUTION_RESULTS_FIELD = "action_execution_results"
        const val BUCKET_KEYS = AggregationResultBucket.BUCKET_KEYS
        const val PARENTS_BUCKET_PATH = AggregationResultBucket.PARENTS_BUCKET_PATH
        const val NO_ID = ""
        const val NO_VERSION = Versions.NOT_FOUND

        @JvmStatic @JvmOverloads
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, id: String = NO_ID, version: Long = NO_VERSION): Alert {

            lateinit var monitorId: String
            var schemaVersion = NO_SCHEMA_VERSION
            lateinit var monitorName: String
            var monitorVersion: Long = Versions.NOT_FOUND
            var monitorUser: User? = null
            lateinit var triggerId: String
            lateinit var triggerName: String
            val findingIds = mutableListOf<String>()
            val relatedDocIds = mutableListOf<String>()
            lateinit var state: State
            lateinit var startTime: Instant
            lateinit var severity: String
            var endTime: Instant? = null
            var lastNotificationTime: Instant? = null
            var acknowledgedTime: Instant? = null
            var errorMessage: String? = null
            val errorHistory: MutableList<AlertError> = mutableListOf()
            val actionExecutionResults: MutableList<ActionExecutionResult> = mutableListOf()
            var aggAlertBucket: AggregationResultBucket? = null
            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    MONITOR_ID_FIELD -> monitorId = xcp.text()
                    SCHEMA_VERSION_FIELD -> schemaVersion = xcp.intValue()
                    MONITOR_NAME_FIELD -> monitorName = xcp.text()
                    MONITOR_VERSION_FIELD -> monitorVersion = xcp.longValue()
                    MONITOR_USER_FIELD -> monitorUser = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else User.parse(xcp)
                    TRIGGER_ID_FIELD -> triggerId = xcp.text()
                    FINDING_IDS -> {
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            findingIds.add(xcp.text())
                        }
                    }
                    RELATED_DOC_IDS -> {
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            relatedDocIds.add(xcp.text())
                        }
                    }
                    STATE_FIELD -> state = State.valueOf(xcp.text())
                    TRIGGER_NAME_FIELD -> triggerName = xcp.text()
                    START_TIME_FIELD -> startTime = requireNotNull(xcp.instant())
                    END_TIME_FIELD -> endTime = xcp.instant()
                    LAST_NOTIFICATION_TIME_FIELD -> lastNotificationTime = xcp.instant()
                    ACKNOWLEDGED_TIME_FIELD -> acknowledgedTime = xcp.instant()
                    ERROR_MESSAGE_FIELD -> errorMessage = xcp.textOrNull()
                    ALERT_HISTORY_FIELD -> {
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            errorHistory.add(AlertError.parse(xcp))
                        }
                    }
                    SEVERITY_FIELD -> severity = xcp.text()
                    ACTION_EXECUTION_RESULTS_FIELD -> {
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            actionExecutionResults.add(ActionExecutionResult.parse(xcp))
                        }
                    }
                    AggregationResultBucket.CONFIG_NAME -> {
                        // If an Alert with aggAlertBucket contents is indexed into the alerts index first, then
                        // that field will be added to the mappings.
                        // In this case, that field will default to null when it isn't present for Alerts created by Query-Level Monitors
                        // (even though the toXContent doesn't output the field) so null is being accounted for here.
                        aggAlertBucket = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) {
                            null
                        } else {
                            AggregationResultBucket.parse(xcp)
                        }
                    }
                }
            }

            return Alert(
                id = id, version = version, schemaVersion = schemaVersion, monitorId = requireNotNull(monitorId),
                monitorName = requireNotNull(monitorName), monitorVersion = monitorVersion, monitorUser = monitorUser,
                triggerId = requireNotNull(triggerId), triggerName = requireNotNull(triggerName),
                state = requireNotNull(state), startTime = requireNotNull(startTime), endTime = endTime,
                lastNotificationTime = lastNotificationTime, acknowledgedTime = acknowledgedTime,
                errorMessage = errorMessage, errorHistory = errorHistory, severity = severity,
                actionExecutionResults = actionExecutionResults, aggregationResultBucket = aggAlertBucket, findingIds = findingIds,
                relatedDocIds = relatedDocIds
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): Alert {
            return Alert(sin)
        }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return createXContentBuilder(builder, true)
    }

    fun toXContentWithUser(builder: XContentBuilder): XContentBuilder {
        return createXContentBuilder(builder, false)
    }
    private fun createXContentBuilder(builder: XContentBuilder, secure: Boolean): XContentBuilder {
        builder.startObject()
            .field(ALERT_ID_FIELD, id)
            .field(ALERT_VERSION_FIELD, version)
            .field(MONITOR_ID_FIELD, monitorId)
            .field(SCHEMA_VERSION_FIELD, schemaVersion)
            .field(MONITOR_VERSION_FIELD, monitorVersion)
            .field(MONITOR_NAME_FIELD, monitorName)

        if (!secure) {
            builder.optionalUserField(MONITOR_USER_FIELD, monitorUser)
        }

        builder.field(TRIGGER_ID_FIELD, triggerId)
            .field(TRIGGER_NAME_FIELD, triggerName)
            .field(FINDING_IDS, findingIds.toTypedArray())
            .field(RELATED_DOC_IDS, relatedDocIds.toTypedArray())
            .field(STATE_FIELD, state)
            .field(ERROR_MESSAGE_FIELD, errorMessage)
            .field(ALERT_HISTORY_FIELD, errorHistory.toTypedArray())
            .field(SEVERITY_FIELD, severity)
            .field(ACTION_EXECUTION_RESULTS_FIELD, actionExecutionResults.toTypedArray())
            .optionalTimeField(START_TIME_FIELD, startTime)
            .optionalTimeField(LAST_NOTIFICATION_TIME_FIELD, lastNotificationTime)
            .optionalTimeField(END_TIME_FIELD, endTime)
            .optionalTimeField(ACKNOWLEDGED_TIME_FIELD, acknowledgedTime)
        aggregationResultBucket?.innerXContent(builder)
        builder.endObject()
        return builder
    }

    fun asTemplateArg(): Map<String, Any?> {
        return mapOf(
            ACKNOWLEDGED_TIME_FIELD to acknowledgedTime?.toEpochMilli(),
            ALERT_ID_FIELD to id,
            ALERT_VERSION_FIELD to version,
            END_TIME_FIELD to endTime?.toEpochMilli(),
            ERROR_MESSAGE_FIELD to errorMessage,
            LAST_NOTIFICATION_TIME_FIELD to lastNotificationTime?.toEpochMilli(),
            SEVERITY_FIELD to severity,
            START_TIME_FIELD to startTime.toEpochMilli(),
            STATE_FIELD to state.toString(),
            // Converting bucket keys to comma separated String to avoid manipulation in Action mustache templates
            BUCKET_KEYS to aggregationResultBucket?.bucketKeys?.joinToString(","),
            PARENTS_BUCKET_PATH to aggregationResultBucket?.parentBucketPath,
            FINDING_IDS to findingIds.joinToString(","),
            RELATED_DOC_IDS to relatedDocIds.joinToString(",")
        )
    }
}
