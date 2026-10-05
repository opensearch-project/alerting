package org.opensearch.alerting.core.modelv2

import org.opensearch.alerting.core.modelv2.MonitorV2RunResult.Companion.ERROR_FIELD
import org.opensearch.alerting.core.modelv2.MonitorV2RunResult.Companion.MONITOR_V2_NAME_FIELD
import org.opensearch.alerting.core.modelv2.MonitorV2RunResult.Companion.PERIOD_END_FIELD
import org.opensearch.alerting.core.modelv2.MonitorV2RunResult.Companion.PERIOD_START_FIELD
import org.opensearch.alerting.core.modelv2.MonitorV2RunResult.Companion.TRIGGER_RESULTS_FIELD
import org.opensearch.alerting.core.util.nonOptionalTimeField
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException
import java.time.Instant

data class PPLMonitorRunResult(
    override val monitorName: String,
    override val error: Exception?,
    override val periodStart: Instant,
    override val periodEnd: Instant,
    override val triggerResults: Map<String, PPLTriggerRunResult>,
    val pplQueryResults: Map<String, Map<String, Any?>> // key: trigger id, value: query results
) : MonitorV2RunResult<PPLTriggerRunResult> {

    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    constructor(sin: StreamInput) : this(
        sin.readString(), // monitorName
        sin.readException(), // error
        sin.readInstant(), // periodStart
        sin.readInstant(), // periodEnd
        sin.readMap() as Map<String, PPLTriggerRunResult>, // triggerResults
        sin.readMap() as Map<String, Map<String, Any?>> // pplQueryResults
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field(MONITOR_V2_NAME_FIELD, monitorName)
        builder.nonOptionalTimeField(PERIOD_START_FIELD, periodStart)
        builder.nonOptionalTimeField(PERIOD_END_FIELD, periodEnd)
        builder.field(ERROR_FIELD, error?.message)
        builder.field(TRIGGER_RESULTS_FIELD, triggerResults)
        builder.field(PPL_QUERY_RESULTS_FIELD, pplQueryResults)
        builder.endObject()
        return builder
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(monitorName)
        out.writeException(error)
        out.writeInstant(periodStart)
        out.writeInstant(periodEnd)
        out.writeMap(triggerResults)
        out.writeMap(pplQueryResults)
    }

    companion object {
        const val PPL_QUERY_RESULTS_FIELD = "ppl_query_results"
    }
}
