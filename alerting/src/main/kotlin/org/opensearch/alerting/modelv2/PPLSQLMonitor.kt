/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.modelv2

import org.opensearch.alerting.core.util.nonOptionalTimeField
import org.opensearch.alerting.modelv2.MonitorV2.Companion.ALERTING_V2_MAX_NAME_LENGTH
import org.opensearch.alerting.modelv2.MonitorV2.Companion.DESCRIPTION_FIELD
import org.opensearch.alerting.modelv2.MonitorV2.Companion.DESCRIPTION_MAX_LENGTH
import org.opensearch.alerting.modelv2.MonitorV2.Companion.ENABLED_FIELD
import org.opensearch.alerting.modelv2.MonitorV2.Companion.ENABLED_TIME_FIELD
import org.opensearch.alerting.modelv2.MonitorV2.Companion.LAST_UPDATE_TIME_FIELD
import org.opensearch.alerting.modelv2.MonitorV2.Companion.LOOK_BACK_WINDOW_FIELD
import org.opensearch.alerting.modelv2.MonitorV2.Companion.MONITOR_V2_MAX_TRIGGERS
import org.opensearch.alerting.modelv2.MonitorV2.Companion.MONITOR_V2_MIN_LOOK_BACK_WINDOW
import org.opensearch.alerting.modelv2.MonitorV2.Companion.NAME_FIELD
import org.opensearch.alerting.modelv2.MonitorV2.Companion.NO_ID
import org.opensearch.alerting.modelv2.MonitorV2.Companion.NO_VERSION
import org.opensearch.alerting.modelv2.MonitorV2.Companion.SCHEDULE_FIELD
import org.opensearch.alerting.modelv2.MonitorV2.Companion.SCHEMA_VERSION_FIELD
import org.opensearch.alerting.modelv2.MonitorV2.Companion.TIMESTAMP_FIELD
import org.opensearch.alerting.modelv2.MonitorV2.Companion.TRIGGERS_FIELD
import org.opensearch.alerting.modelv2.MonitorV2.Companion.USER_FIELD
import org.opensearch.commons.alerting.model.CronSchedule
import org.opensearch.commons.alerting.model.Schedule
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.alerting.util.IndexUtils
import org.opensearch.commons.alerting.util.instant
import org.opensearch.commons.alerting.util.optionalTimeField
import org.opensearch.commons.alerting.util.optionalUserField
import org.opensearch.commons.authuser.User
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import java.io.IOException
import java.time.Instant

/**
 * PPL/SQL Monitor for OpenSearch Alerting V2
 *
 * @property id Monitor ID. Defaults to [NO_ID].
 * @property version Version number of the monitor. Defaults to [NO_VERSION].
 * @property name Display name of the monitor.
 * @property enabled Boolean flag indicating whether the monitor is currently on or off.
 * @property schedule Defines when and how often the monitor should run. Can be a CRON or interval schedule.
 * @property lookBackWindow How far back each Monitor execution's query should look back when searching data.
 * @property lastUpdateTime Timestamp of the last update to this monitor.
 * @property enabledTime Timestamp when the monitor was last enabled. Null if never enabled.
 * @property description Optional Monitor description.
 * @property triggers List of [PPLTrigger]s associated with this monitor.
 * @property schemaVersion Version of the alerting-config index schema used when this Monitor was indexed. Defaults to [NO_SCHEMA_VERSION].
 * @property queryLanguage The query language used. Defaults to [QueryLanguage.PPL].
 * @property query The query string to be executed by this monitor.
 *
 * @opensearch.experimental
 */
data class PPLSQLMonitor(
    override val id: String = NO_ID,
    override val version: Long = NO_VERSION,
    override val name: String,
    override val enabled: Boolean,
    override val schedule: Schedule,
    override val lookBackWindow: Long?,
    override val timestampField: String?,
    override val lastUpdateTime: Instant,
    override val enabledTime: Instant?,
    override val description: String?,
    override val user: User?,
    override val triggers: List<PPLSQLTrigger>,
    override val schemaVersion: Int = IndexUtils.NO_SCHEMA_VERSION,
    val queryLanguage: QueryLanguage = QueryLanguage.PPL, // default to PPL, SQL not currently supported
    val query: String
) : MonitorV2 {

    // specify scheduled job type
    override val type = MonitorV2.MONITOR_V2_TYPE

    override fun fromDocument(id: String, version: Long): PPLSQLMonitor = copy(id = id, version = version)

    init {
        // SQL monitors are not yet supported
        if (this.queryLanguage == QueryLanguage.SQL) {
            throw IllegalArgumentException("SQL queries are not supported. Please use a PPL query.")
        }

        require(this.name.length <= ALERTING_V2_MAX_NAME_LENGTH) {
            "Monitor name too long, length must be less than $ALERTING_V2_MAX_NAME_LENGTH."
        }

        if (this.lookBackWindow != null) {
            requireNotNull(this.timestampField) { "If look back window is specified, timestamp field must not be null." }
        } else {
            require(this.timestampField == null) { "If look back window is not specified, timestamp field must not be specified." }
        }

        require(this.triggers.isNotEmpty()) { "Monitor must include at least 1 trigger." }
        require(this.triggers.size <= MONITOR_V2_MAX_TRIGGERS) { "Monitors can only have $MONITOR_V2_MAX_TRIGGERS triggers." }

        lookBackWindow?.let {
            require(this.lookBackWindow >= MONITOR_V2_MIN_LOOK_BACK_WINDOW) {
                "Monitors look back windows must be at least $MONITOR_V2_MIN_LOOK_BACK_WINDOW minute."
            }
        }

        this.description?.let {
            require(this.description.length <= DESCRIPTION_MAX_LENGTH) { "Description must be under $DESCRIPTION_MAX_LENGTH characters." }
        }

        // for checking trigger ID uniqueness
        val triggerIds = mutableSetOf<String>()
        this.triggers.forEach { trigger ->
            require(triggerIds.add(trigger.id)) { "Duplicate trigger id: ${trigger.id}. Trigger ids must be unique." }
        }

        if (this.enabled) {
            requireNotNull(this.enabledTime)
        } else {
            require(this.enabledTime == null)
        }
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        version = sin.readLong(),
        name = sin.readString(),
        enabled = sin.readBoolean(),
        schedule = Schedule.readFrom(sin),
        lookBackWindow = sin.readOptionalLong(),
        timestampField = sin.readOptionalString(),
        lastUpdateTime = sin.readInstant(),
        enabledTime = sin.readOptionalInstant(),
        description = sin.readOptionalString(),
        user = if (sin.readBoolean()) {
            User(sin)
        } else {
            null
        },
        triggers = sin.readList(PPLSQLTrigger.Companion::readFrom),
        schemaVersion = sin.readInt(),
        queryLanguage = sin.readEnum(QueryLanguage::class.java),
        query = sin.readString()
    )

    override fun toXContentWithUser(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return createXContentBuilder(builder, params, true)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return createXContentBuilder(builder, params, false)
    }

    private fun createXContentBuilder(builder: XContentBuilder, params: ToXContent.Params, withUser: Boolean): XContentBuilder {
        builder.startObject() // overall start object

        // if this is being written as ScheduledJob, add extra object layer and add ScheduledJob
        // related metadata, default to false
        if (params.paramAsBoolean("with_type", false)) {
            builder.startObject(MonitorV2.MONITOR_V2_TYPE)
        }

        // wrap PPLSQLMonitor in outer object named after its monitor type
        // required for MonitorV2 XContentParser to first encounter this,
        // read in monitor type, then delegate to correct parse() function
        builder.startObject(PPL_SQL_MONITOR_TYPE) // monitor type start object

        builder.field(NAME_FIELD, name)
        builder.field(SCHEDULE_FIELD, schedule)
        builder.field(LOOK_BACK_WINDOW_FIELD, lookBackWindow)
        builder.field(TIMESTAMP_FIELD, timestampField)
        builder.field(ENABLED_FIELD, enabled)
        builder.nonOptionalTimeField(LAST_UPDATE_TIME_FIELD, lastUpdateTime)
        builder.optionalTimeField(ENABLED_TIME_FIELD, enabledTime)
        builder.field(DESCRIPTION_FIELD, description)
        builder.field(TRIGGERS_FIELD, triggers.toTypedArray())
        builder.field(SCHEMA_VERSION_FIELD, schemaVersion)
        builder.field(QUERY_LANGUAGE_FIELD, queryLanguage.value)
        builder.field(QUERY_FIELD, query)

        if (withUser) {
            builder.optionalUserField(USER_FIELD, user)
        }

        builder.endObject() // monitor type end object

        // if ScheduledJob metadata was added, end the extra object layer that was created
        if (params.paramAsBoolean("with_type", false)) {
            builder.endObject()
        }

        builder.endObject() // overall end object

        return builder
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
        out.writeString(name)
        out.writeBoolean(enabled)

        if (schedule is CronSchedule) {
            out.writeEnum(Schedule.TYPE.CRON)
        } else {
            out.writeEnum(Schedule.TYPE.INTERVAL)
        }
        schedule.writeTo(out)

        out.writeOptionalLong(lookBackWindow)
        out.writeOptionalString(timestampField)
        out.writeInstant(lastUpdateTime)
        out.writeOptionalInstant(enabledTime)
        out.writeOptionalString(description)

        out.writeBoolean(user != null)
        user?.writeTo(out)

        out.writeVInt(triggers.size)
        triggers.forEach { it.writeTo(out) }
        out.writeInt(schemaVersion)
        out.writeEnum(queryLanguage)
        out.writeString(query)
    }

    override fun asTemplateArg(): Map<String, Any?> {
        return mapOf(
            IndexUtils._ID to id,
            IndexUtils._VERSION to version,
            NAME_FIELD to name,
            ENABLED_FIELD to enabled,
            SCHEDULE_FIELD to schedule,
            LOOK_BACK_WINDOW_FIELD to lookBackWindow,
            LAST_UPDATE_TIME_FIELD to lastUpdateTime.toEpochMilli(),
            ENABLED_TIME_FIELD to enabledTime?.toEpochMilli(),
            QUERY_FIELD to query
        )
    }

    override fun makeCopy(
        id: String,
        version: Long,
        name: String,
        enabled: Boolean,
        schedule: Schedule,
        lastUpdateTime: Instant,
        enabledTime: Instant?,
        description: String?,
        user: User?,
        schemaVersion: Int,
        lookBackWindow: Long?,
        timestampField: String?
    ): PPLSQLMonitor {
        return copy(
            id = id,
            version = version,
            name = name,
            enabled = enabled,
            schedule = schedule,
            lastUpdateTime = lastUpdateTime,
            enabledTime = enabledTime,
            description = description,
            user = user,
            schemaVersion = schemaVersion,
            lookBackWindow = lookBackWindow,
            timestampField = timestampField
        )
    }

    enum class QueryLanguage(val value: String) {
        PPL(PPL_QUERY_LANGUAGE),
        SQL(SQL_QUERY_LANGUAGE);

        companion object {
            fun enumFromString(value: String): QueryLanguage? = QueryLanguage.values().firstOrNull { it.value == value }
        }
    }

    companion object {
        // monitor type name
        const val PPL_SQL_MONITOR_TYPE = "ppl_monitor"

        // query languages
        const val PPL_QUERY_LANGUAGE = "ppl"
        const val SQL_QUERY_LANGUAGE = "sql"

        // field names
        const val QUERY_LANGUAGE_FIELD = "query_language"
        const val QUERY_FIELD = "query"

        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, id: String = NO_ID, version: Long = NO_VERSION): PPLSQLMonitor {
            var name: String? = null
            var enabled = true
            var schedule: Schedule? = null
            var lookBackWindow: Long? = null
            var timestampField: String? = null
            var lastUpdateTime: Instant? = null
            var enabledTime: Instant? = null
            var description: String? = null
            var user: User? = null
            val triggers: MutableList<PPLSQLTrigger> = mutableListOf()
            var schemaVersion = IndexUtils.NO_SCHEMA_VERSION
            var queryLanguage: QueryLanguage = QueryLanguage.PPL // default to PPL
            var query: String? = null

            /* parse */
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME_FIELD -> name = xcp.text()
                    ENABLED_FIELD -> enabled = xcp.booleanValue()
                    SCHEDULE_FIELD -> schedule = Schedule.parse(xcp)
                    LOOK_BACK_WINDOW_FIELD -> {
                        if (xcp.currentToken() != XContentParser.Token.VALUE_NULL) {
                            lookBackWindow = xcp.longValue()
                        }
                    }
                    TIMESTAMP_FIELD -> {
                        if (xcp.currentToken() != XContentParser.Token.VALUE_NULL) {
                            timestampField = xcp.text()
                        }
                    }
                    LAST_UPDATE_TIME_FIELD -> lastUpdateTime = xcp.instant()
                    ENABLED_TIME_FIELD -> enabledTime = xcp.instant()
                    DESCRIPTION_FIELD -> {
                        if (xcp.currentToken() != XContentParser.Token.VALUE_NULL) {
                            description = xcp.text()
                        }
                    }
                    USER_FIELD -> {
                        if (xcp.currentToken() != XContentParser.Token.VALUE_NULL) {
                            user = User.parse(xcp)
                        }
                    }
                    TRIGGERS_FIELD -> {
                        XContentParserUtils.ensureExpectedToken(
                            XContentParser.Token.START_ARRAY,
                            xcp.currentToken(),
                            xcp
                        )
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            triggers.add(PPLSQLTrigger.parseInner(xcp))
                        }
                    }
                    SCHEMA_VERSION_FIELD -> schemaVersion = xcp.intValue()
                    QUERY_LANGUAGE_FIELD -> {
                        val input = xcp.text()
                        val enumMatchResult = QueryLanguage.enumFromString(input)
                            ?: throw AlertingException.wrap(
                                IllegalArgumentException(
                                    "Invalid value for $QUERY_LANGUAGE_FIELD: $input. " +
                                        "Supported values are ${QueryLanguage.values().map { it.value }}"
                                )
                            )
                        queryLanguage = enumMatchResult
                    }
                    QUERY_FIELD -> query = xcp.text()
                    else -> throw IllegalArgumentException("Unexpected field when parsing PPL/SQL Monitor: $fieldName")
                }
            }

            /* validations */

            // if enabled, set time of MonitorV2 creation/update is set as enable time
            if (enabled && enabledTime == null) {
                enabledTime = Instant.now()
            } else if (!enabled) {
                enabledTime = null
            }

            lastUpdateTime = lastUpdateTime ?: Instant.now()

            // check for required fields
            requireNotNull(name) { "Monitor name is null" }
            requireNotNull(schedule) { "Schedule is null" }
            requireNotNull(query) { "Query is null" }
            requireNotNull(lastUpdateTime) { "Last update time is null" }

            /* return PPLSQLMonitor */
            return PPLSQLMonitor(
                id,
                version,
                name,
                enabled,
                schedule,
                lookBackWindow,
                timestampField,
                lastUpdateTime,
                enabledTime,
                description,
                user,
                triggers,
                schemaVersion,
                queryLanguage,
                query
            )
        }
    }
}
