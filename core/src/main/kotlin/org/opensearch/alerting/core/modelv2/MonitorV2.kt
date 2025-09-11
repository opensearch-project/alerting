package org.opensearch.alerting.core.modelv2

import org.opensearch.alerting.core.modelv2.PPLMonitor.Companion.PPL_MONITOR_TYPE
import org.opensearch.common.CheckedFunction
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.Schedule
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.core.ParseField
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import java.io.IOException
import java.time.Instant

interface MonitorV2 : ScheduledJob {
    override val id: String
    override val version: Long
    override val name: String
    override val enabled: Boolean
    override val schedule: Schedule
    override val lastUpdateTime: Instant // required for scheduled job maintenance
    override val enabledTime: Instant? // required for scheduled job maintenance
    val triggers: List<TriggerV2>
    val schemaVersion: Int // for updating monitors
    val lookBackWindow: TimeValue? // how far back to look when querying data during monitor execution

    fun asTemplateArg(): Map<String, Any?>

    enum class MonitorV2Type(val value: String) {
        PPL_MONITOR(PPL_MONITOR_TYPE);

        override fun toString(): String {
            return value
        }

        companion object {
            fun enumFromString(value: String): MonitorV2Type? {
                return MonitorV2Type.entries.find { it.value == value }
            }
        }
    }

    companion object {
        // scheduled job field names
        const val MONITOR_V2_TYPE = "monitor_v2" // scheduled job type is MonitorV2

        // field names
        const val NAME_FIELD = "name"
        const val ENABLED_FIELD = "enabled"
        const val SCHEDULE_FIELD = "schedule"
        const val LAST_UPDATE_TIME_FIELD = "last_update_time"
        const val ENABLED_TIME_FIELD = "enabled_time"
        const val TRIGGERS_FIELD = "triggers"
        const val LOOK_BACK_WINDOW_FIELD = "look_back_window"

        // default values
        const val NO_ID = ""
        const val NO_VERSION = 1L

        val XCONTENT_REGISTRY = NamedXContentRegistry.Entry(
            ScheduledJob::class.java,
            ParseField(MONITOR_V2_TYPE),
            CheckedFunction { parse(it) }
        )

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): MonitorV2 {
            /* parse outer object for monitorV2 type, then delegate to correct monitorV2 parser */

            XContentParserUtils.ensureExpectedToken( // outer monitor object start
                XContentParser.Token.START_OBJECT,
                xcp.currentToken(),
                xcp
            )

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, xcp.nextToken(), xcp) // monitor type field name
            val monitorTypeText = xcp.currentName()
            val monitorType = MonitorV2Type.enumFromString(monitorTypeText)
                ?: throw IllegalStateException("when parsing MonitorV2, received invalid monitor type: $monitorTypeText")

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp) // inner monitor object start

            return when (monitorType) {
                MonitorV2Type.PPL_MONITOR -> PPLMonitor.parse(xcp)
            }
        }

        fun readFrom(sin: StreamInput): MonitorV2 {
            return when (val monitorType = sin.readEnum(MonitorV2Type::class.java)) {
                MonitorV2Type.PPL_MONITOR -> PPLMonitor(sin)
                else -> throw IllegalStateException("Unexpected input \"$monitorType\" when reading MonitorV2")
            }
        }

        fun writeTo(out: StreamOutput, monitorV2: MonitorV2) {
            when (monitorV2) {
                is PPLMonitor -> {
                    out.writeEnum(MonitorV2Type.PPL_MONITOR)
                    monitorV2.writeTo(out)
                }
            }
        }
    }
}
