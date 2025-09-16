package org.opensearch.alerting.core.modelv2

import org.opensearch.alerting.core.modelv2.MonitorV2.Companion.ENABLED_FIELD
import org.opensearch.alerting.core.modelv2.MonitorV2.Companion.ENABLED_TIME_FIELD
import org.opensearch.alerting.core.modelv2.MonitorV2.Companion.LAST_UPDATE_TIME_FIELD
import org.opensearch.alerting.core.modelv2.MonitorV2.Companion.LOOK_BACK_WINDOW_FIELD
import org.opensearch.alerting.core.modelv2.MonitorV2.Companion.NAME_FIELD
import org.opensearch.alerting.core.modelv2.MonitorV2.Companion.NO_ID
import org.opensearch.alerting.core.modelv2.MonitorV2.Companion.NO_VERSION
import org.opensearch.alerting.core.modelv2.MonitorV2.Companion.SCHEDULE_FIELD
import org.opensearch.alerting.core.modelv2.MonitorV2.Companion.SCHEMA_VERSION_FIELD
import org.opensearch.alerting.core.modelv2.MonitorV2.Companion.TRIGGERS_FIELD
import org.opensearch.alerting.core.modelv2.MonitorV2.Companion.USER_FIELD
import org.opensearch.alerting.core.util.nonOptionalTimeField
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.CronSchedule
import org.opensearch.commons.alerting.model.Schedule
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

// TODO: probably change this to be called PPLSQLMonitor. A PPL Monitor and SQL Monitor
// would have the exact same functionality, except the choice of language
// when calling PPL/SQL plugin's execute API would be different.
// we dont need 2 different monitor types for that, just a simple if check
// for query language at monitor execution time
/**
 * PPL (Piped Processing Language) Monitor for OpenSearch Alerting V2
 *
 * @property id Monitor ID. Defaults to [NO_ID].
 * @property version Version number of the monitor. Defaults to [NO_VERSION].
 * @property name Display name of the monitor.
 * @property enabled Boolean flag indicating whether the monitor is currently on or off.
 * @property schedule Defines when and how often the monitor should run. Can be a CRON or interval schedule.
 * @property lookBackWindow How far back each Monitor execution's query should look back when searching data.
 *                    Only applicable if Monitor uses CRON schedule. Optional even if CRON schedule is used.
 * @property lastUpdateTime Timestamp of the last update to this monitor.
 * @property enabledTime Timestamp when the monitor was last enabled. Null if never enabled.
 * @property triggers List of [PPLTrigger]s associated with this monitor.
 * @property schemaVersion Version of the alerting-config index schema used when this Monitor was indexed. Defaults to [NO_SCHEMA_VERSION].
 * @property queryLanguage The query language used. Defaults to [QueryLanguage.PPL].
 * @property query The PPL query string to be executed by this monitor.
 */
data class PPLMonitor(
    override val id: String = NO_ID,
    override val version: Long = NO_VERSION,
    override val name: String,
    override val enabled: Boolean,
    override val schedule: Schedule,
    override val lookBackWindow: TimeValue? = null,
    override val lastUpdateTime: Instant,
    override val enabledTime: Instant?,
    override val user: User?,
    override val triggers: List<PPLTrigger>,
    override val schemaVersion: Int = IndexUtils.NO_SCHEMA_VERSION,
    val queryLanguage: QueryLanguage = QueryLanguage.PPL, // default to PPL, SQL not currently supported
    val query: String
) : MonitorV2 {

    // specify scheduled job type
    override val type = MonitorV2.MONITOR_V2_TYPE

    override fun fromDocument(id: String, version: Long): PPLMonitor = copy(id = id, version = version)

    init {
        // SQL monitors are not yet supported
        if (this.queryLanguage == QueryLanguage.SQL) {
            throw IllegalStateException("Monitors with SQL queries are not supported")
        }

        // for checking trigger ID uniqueness
        val triggerIds = mutableSetOf<String>()
        triggers.forEach { trigger ->
            require(triggerIds.add(trigger.id)) { "Duplicate trigger id: ${trigger.id}. Trigger ids must be unique." }
        }

        if (enabled) {
            requireNotNull(enabledTime)
        } else {
            require(enabledTime == null)
        }

        // TODO: create setting for max triggers and check for max triggers here
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        version = sin.readLong(),
        name = sin.readString(),
        enabled = sin.readBoolean(),
        schedule = Schedule.readFrom(sin),
        lookBackWindow = TimeValue.parseTimeValue(sin.readString(), PLACEHOLDER_LOOK_BACK_WINDOW_SETTING_NAME),
        lastUpdateTime = sin.readInstant(),
        enabledTime = sin.readOptionalInstant(),
        user = if (sin.readBoolean()) {
            User(sin)
        } else {
            null
        },
        triggers = sin.readList(PPLTrigger::readFrom),
        schemaVersion = sin.readInt(),
        queryLanguage = sin.readEnum(QueryLanguage::class.java),
        query = sin.readString()
    )

    fun toXContentWithUser(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
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

        // wrap PPLMonitor in outer object named after its monitor type
        // required for MonitorV2 XContentParser to first encounter this,
        // read in monitor type, then delegate to correct parse() function
        builder.startObject(PPL_MONITOR_TYPE) // monitor type start object

        if (withUser) {
            builder.optionalUserField(USER_FIELD, user)
        }

        builder.field(NAME_FIELD, name)
        builder.field(SCHEDULE_FIELD, schedule)
        builder.field(LOOK_BACK_WINDOW_FIELD, lookBackWindow?.toHumanReadableString(0))
        builder.field(ENABLED_FIELD, enabled)
        builder.nonOptionalTimeField(LAST_UPDATE_TIME_FIELD, lastUpdateTime)
        builder.optionalTimeField(ENABLED_TIME_FIELD, enabledTime)
        builder.field(TRIGGERS_FIELD, triggers.toTypedArray())
        builder.field(SCHEMA_VERSION_FIELD, schemaVersion)
        builder.field(QUERY_LANGUAGE_FIELD, queryLanguage.value)
        builder.field(QUERY_FIELD, query)

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

        out.writeBoolean(lookBackWindow != null)
        lookBackWindow?.let { out.writeString(lookBackWindow.toHumanReadableString(0)) }

        out.writeInstant(lastUpdateTime)
        out.writeOptionalInstant(enabledTime)

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
            LOOK_BACK_WINDOW_FIELD to lookBackWindow?.toHumanReadableString(0),
            LAST_UPDATE_TIME_FIELD to lastUpdateTime.toEpochMilli(),
            ENABLED_TIME_FIELD to enabledTime?.toEpochMilli(),
            TRIGGERS_FIELD to triggers,
            QUERY_LANGUAGE_FIELD to queryLanguage.value,
            QUERY_FIELD to query
        )
    }

    enum class QueryLanguage(val value: String) {
        PPL(PPL_QUERY_LANGUAGE),
        SQL(SQL_QUERY_LANGUAGE);

        companion object {
            fun enumFromString(value: String): QueryLanguage? = QueryLanguage.entries.firstOrNull { it.value == value }
        }
    }

    companion object {
        // monitor type name
        const val PPL_MONITOR_TYPE = "ppl_monitor" // TODO: eventually change to SQL_PPL_MONITOR_TYPE

        // query languages
        const val PPL_QUERY_LANGUAGE = "ppl"
        const val SQL_QUERY_LANGUAGE = "sql"

        // field names
        const val QUERY_LANGUAGE_FIELD = "query_language"
        const val QUERY_FIELD = "query"

        // mock setting name used when parsing TimeValue
        // TimeValue class is usually reserved for declaring settings, but we're using it
        // outside that use case here, which is why we need these placeholders
        private const val PLACEHOLDER_LOOK_BACK_WINDOW_SETTING_NAME = "ppl_monitor_look_back_window"

        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, id: String = NO_ID, version: Long = NO_VERSION): PPLMonitor {
            var name: String? = null
            var enabled = true
            var schedule: Schedule? = null
            var lookBackWindow: TimeValue? = null
            var lastUpdateTime: Instant? = null
            var enabledTime: Instant? = null
            var user: User? = null
            val triggers: MutableList<PPLTrigger> = mutableListOf()
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
                        lookBackWindow = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) {
                            null
                        } else {
                            val input = xcp.text()
                            // throws IllegalArgumentException if there's parsing error
                            TimeValue.parseTimeValue(input, PLACEHOLDER_LOOK_BACK_WINDOW_SETTING_NAME)
                        }
                    }
                    LAST_UPDATE_TIME_FIELD -> lastUpdateTime = xcp.instant()
                    ENABLED_TIME_FIELD -> enabledTime = xcp.instant()
                    USER_FIELD -> user = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else User.parse(xcp)
                    TRIGGERS_FIELD -> {
                        XContentParserUtils.ensureExpectedToken(
                            XContentParser.Token.START_ARRAY,
                            xcp.currentToken(),
                            xcp
                        )
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            triggers.add(PPLTrigger.parseInner(xcp))
                        }
                    }
                    SCHEMA_VERSION_FIELD -> schemaVersion = xcp.intValue()
                    QUERY_LANGUAGE_FIELD -> {
                        val input = xcp.text()
                        val enumMatchResult = QueryLanguage.enumFromString(input)
                            ?: throw IllegalArgumentException(
                                "Invalid value for $QUERY_LANGUAGE_FIELD: $input. " +
                                    "Supported values are ${QueryLanguage.entries.map { it.value }}"
                            )
                        queryLanguage = enumMatchResult
                    }
                    QUERY_FIELD -> query = xcp.text()
                    else -> throw IllegalArgumentException("Unexpected field when parsing PPL Monitor: $fieldName")
                }
            }

            /* validations */

            // ensure there's at least 1 trigger
            if (triggers.isEmpty()) {
                throw IllegalArgumentException("Monitor must include at least 1 trigger")
            }

            // ensure the trigger suppress durations are valid
            triggers.forEach { trigger ->
                trigger.suppressDuration?.let { suppressDuration ->
                    // TODO: these max and min values are completely arbitrary, make them settings
                    val minValue = TimeValue.timeValueMinutes(1)
                    val maxValue = TimeValue.timeValueDays(5)

                    require(suppressDuration <= maxValue) { "Suppress duration must be at most $maxValue but was $suppressDuration" }

                    require(suppressDuration >= minValue) { "Suppress duration must be at least $minValue but was $suppressDuration" }
                }
            }

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

            if (queryLanguage == QueryLanguage.SQL) {
                throw IllegalArgumentException("SQL queries are not supported. Please use a PPL query.")
            }

            /* return PPLMonitor */
            return PPLMonitor(
                id,
                version,
                name,
                enabled,
                schedule,
                lookBackWindow,
                lastUpdateTime,
                enabledTime,
                user,
                triggers,
                schemaVersion,
                queryLanguage,
                query
            )
        }
    }
}
